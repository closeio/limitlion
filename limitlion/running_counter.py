import itertools
import math
import time
from collections import namedtuple
from distutils.version import LooseVersion

import pkg_resources

REDIS_PY_VERSION = pkg_resources.get_distribution("redis").version
IS_REDIS_PY_2 = LooseVersion(REDIS_PY_VERSION) < LooseVersion("3")


BucketValue = namedtuple('BucketValue', ['bucket', 'value'])


class RunningCounter:
    """
    A running counter keeps counts per interval for a specified period. The
    interval is specified in seconds and period specifies how many buckets
    should be kept.

    Buckets are addressed using the first epoch second for that interval
    calculated as follows:

        floor(epoch seconds / interval).

    For example, if using 1 hour intervals the bucket id for 2/19/19 01:23:09Z
    would be 1550539389 / (60 * 60) = 430705. This bucket id is used to generate
    a Redis key with the following format: [key prefix]:[key]:[bucket id].

    A group name can be provided to keep track of the list of counters in named
    group.

    Summing up all bucket values for the RunningCounter's window gives the total
    count.

    """

    def __init__(
        self,
        redis_instance,
        interval,
        periods,
        name=None,
        name_prefix='rc',
        group=None,
    ):
        """
        Inits RunningCounter class.

        Args:
            redis_instance: Redis client instance.
            interval (int): How many seconds are collected in each bucket.
            periods (int): How many buckets to key.
            name (string): Optional; Name of this counter counter.
            name_prefix (string): Optional; Prepended to name to generate Redis
                                 key.
            group (string): Optional; Keep track of keys if group name is
                            specified.
        """
        if group is not None and name is not None:
            raise ValueError('Cannot set key and group in __init__')
        self.redis = redis_instance
        self.name_prefix = name_prefix
        self.name = name
        self.group_name = group
        self.interval = interval
        self.periods = periods

    @property
    def window(self):
        """
        Running counter window.

        Returns:
            Integer seconds for window of Running Counter.
        """
        return self.interval * self.periods

    def _key(self, name, bucket):
        if self.group_name:
            return '{}:{}:{}:{}'.format(
                self.name_prefix, self.group_name, name, bucket
            )
        else:
            return '{}:{}:{}'.format(self.name_prefix, name, bucket)

    def _group_key(self):
        """
        Redis key with names of all counters from a group.
        """
        assert self.group_name is not None
        return '{}:{}:{}'.format(
            self.name_prefix, self.group_name, 'group_keys'
        )

    def _get_name(self, name):
        if name is None:
            if self.name is None:
                raise ValueError('Name not specified')
            else:
                return self.name
        return name

    def counts(self, name=None, now=None):
        """
        Get RunningCounter bucket counts.

        Args:
            name: Optional; Must be provided if not provided to __init__().
            now: Optional; Specify time to ensure consistency across multiple
            calls.

        Returns:
            List of BucketValues.
        """
        if not now:
            now = time.time()
        name = self._get_name(name)

        buckets = self._all_buckets(now)

        results = self.redis.mget(
            map(lambda bucket: self._key(name, bucket), buckets)
        )

        counts = [None if v is None else float(v) for v in results]

        bucket_values = [
            BucketValue(bv[0], bv[1])
            for bv in zip(buckets, counts)
            if bv[1] is not None
        ]
        return bucket_values

    def _all_buckets(self, now):
        """
        Get all time buckets in running counter's range.
        """
        current_bucket = int(math.floor(now / self.interval))
        buckets = range(current_bucket, current_bucket - self.periods, -1)
        return buckets

    def count(self, name=None, now=None):
        """
        Get total count for counter.

        Args:
            name: Optional; Must be provided if not provided to __init__().
            now: Optional; Specify time to ensure consistency across multiple
            calls.

        Returns:
            Sum of all buckets.
        """
        name = self._get_name(name)
        return sum([bv.value for bv in self.counts(name=name, now=now)])

    def inc(self, increment=1, name=None):
        """
        Update rate counter.

        Args:
            increment: Float of value to add to bucket.
            name: Optional; Must be provided if not provided to __init__().

        """

        # If more consistent time is needed across calling
        # processes, this method could be converted into a
        # Lua script to use Redis server time.
        now = time.time()

        name = self._get_name(name)

        bucket = int(math.floor(now / self.interval))
        bucket_key = self._key(name, bucket)
        expire = self.periods * self.interval + 15

        pipeline = self.redis.pipeline()
        pipeline.incrbyfloat(bucket_key, increment)
        pipeline.expire(bucket_key, expire)
        if self.group_name is not None:
            group_key = self._group_key()
            if IS_REDIS_PY_2:
                pipeline.zadd(group_key, name, now)
            else:
                pipeline.zadd(group_key, {name: now})
            pipeline.expire(group_key, expire)
            # Trim zset to keys used within window so
            # it doesn't grow uncapped.
            pipeline.zremrangebyscore(group_key, '-inf', now - self.window - 1)
        pipeline.execute()

    def group(self):
        """
        Get all counter names in a group.

        Returns:
            List of counter names
        """
        group_key = self._group_key()
        pipeline = self.redis.pipeline()
        # Trim zset keys so we don't look for values
        # that won't exist anyway
        pipeline.zremrangebyscore(
            group_key, '-inf', time.time() - self.window - 1
        )
        pipeline.zrange(group_key, 0, -1)
        results = pipeline.execute()
        return [v.decode() if isinstance(v, bytes) else v for v in results[1]]

    def group_counts(self):
        """
        Get count for each key in group.

        Returns:
            Dictionary of {[key], [count]}
        """
        values = {}
        # Ensure consistent time across all keys in group
        now = time.time()
        # Could do this in a pipeline but if a group is huge
        # it might be better to do them one at a time
        for name in self.group():
            values[name] = self.count(name, now=now)

        return values

    def delete(self, name=None):
        """
        Remove a counter.

        Args:
            name: Optional; Must be provided if not provided to __init__().
        """
        name = self._get_name(name)
        buckets = self._all_buckets(time.time())
        counter_keys = [self._key(name, bucket) for bucket in buckets]

        pipeline = self.redis.pipeline()
        pipeline.delete(*counter_keys)
        if self.group_name:
            pipeline.zrem(self._group_key(), name)
        pipeline.execute()

    def delete_group(self):
        """
        Remove all counters in a group. A group_name must be provided to
        __init__()
        """
        now = time.time()
        all_counters = self.group()
        buckets = self._all_buckets(now)
        counter_keys = [
            self._key(key, bucket)
            for key, bucket in itertools.product(all_counters, buckets)
        ]
        self.redis.delete(self._group_key(), *counter_keys)
