import itertools
import time
from collections import namedtuple
from distutils.version import LooseVersion

import pkg_resources

REDIS_PY_VERSION = pkg_resources.get_distribution("redis").version
IS_REDIS_PY_2 = LooseVersion(REDIS_PY_VERSION) < LooseVersion("3")


BucketCount = namedtuple('BucketCount', ['bucket', 'count'])


class RunningCounter:
    """
    A running counter keeps counts per interval for a specified number of
    buckets

    Buckets are addressed using the first epoch second for that interval
    calculated as follows:

        floor(epoch seconds / interval).

    For example, if using 1 hour intervals the bucket id for 2/19/19 01:23:09Z
    would be floor(1550539389 / (60 * 60)) = 430705. This bucket id is used to
    generate a Redis key with the following format:
    [key prefix]:[key]:[bucket id].

    A group name can be provided to keep track of the list of counters in named
    group.

    Summing up all bucket values for the RunningCounter's window gives the total
    count.

    """

    def __init__(
        self,
        redis,
        interval,
        num_buckets,
        name=None,
        name_prefix='rc',
        group_name=None,
    ):
        """
        Inits RunningCounter class.

        Args:
            redis: Redis client instance.
            interval (int): How many seconds are collected in each bucket.
            num_buckets (int): How many buckets to keep.
            name (string): Optional; Name of this running counter.
            name_prefix (string): Optional; Prepended to name to generate Redis
                key. Name xor group_name must be set.
            group_name (string): Optional; Keep track of keys if group name is
                specified. Name xor group_name must be set.
        """
        if (name is None) == (group_name is None):
            raise ValueError('Either name xor group must be set in __init__')
        self.redis = redis
        self.interval = interval
        self.num_buckets = num_buckets
        self.name = name
        self.name_prefix = name_prefix
        self.group_name = group_name

    @property
    def window(self):
        """
        Running counter window.

        Returns:
            Integer seconds for window of Running Counter.
        """
        return self.interval * self.num_buckets

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
        if self.name:
            if name and self.name != name:
                raise ValueError(
                    'Cannot specify different name when already set in __init__'
                )
            return self.name
        else:
            if name is None:
                raise ValueError('Name not specified')
            return name

    def _get_buckets(self, recent_buckets=None, now=None):
        """
        Get all buckets in the running counter's window, or only the most
        recent_buckets.
        """
        now = now or time.time()
        current_bucket = int(now) // self.interval
        if recent_buckets is None:
            oldest_bucket = current_bucket - self.num_buckets
        else:
            if recent_buckets > self.num_buckets:
                raise ValueError(
                    'recent_buckets must be less or equal to num_buckets '
                    'in __init__'
                )
            oldest_bucket = current_bucket - recent_buckets
        buckets = range(current_bucket, oldest_bucket, -1)
        return buckets

    def buckets_counts(self, name=None, recent_buckets=None, now=None):
        """
        Get RunningCounter buckets with counts. Missing buckets are filled
        with 0. Most recent buckets are first.

        Args:
            name: Optional; Must be provided if not provided to __init__().
            recent_buckets: Optional; Number of most recent buckets to consider.
            now: Optional; Specify time to ensure consistency across multiple
                calls.

        Returns:
            List of BucketCount.
        """
        if not now:
            now = time.time()
        name = self._get_name(name)

        buckets = self._get_buckets(recent_buckets=recent_buckets, now=now)

        results = self.redis.mget(
            map(lambda bucket: self._key(name, bucket), buckets)
        )

        counts = [0 if v is None else float(v) for v in results]

        buckets_counts = [
            BucketCount(bv[0], bv[1]) for bv in zip(buckets, counts)
        ]
        return buckets_counts

    def count(self, name=None, recent_buckets=None, now=None):
        """
        Get total count for counter.

        Args:
            name: Optional; Must be provided if not provided to __init__().
            recent_buckets: Optional; Number of most recent buckets to consider.
            now: Optional; Specify time to ensure consistency across multiple
                calls.

        Returns:
            Sum of all buckets.
        """
        name = self._get_name(name)
        return sum(
            [
                bv.count
                for bv in self.buckets_counts(
                    name=name, recent_buckets=recent_buckets, now=now
                )
            ]
        )

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

        bucket = int(now) // self.interval
        bucket_key = self._key(name, bucket)
        expire = self.num_buckets * self.interval + 15

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

    def group_counts(self, recent_buckets=None):
        """
        Get count for each counter in group.

        Args:
            recent_buckets: Optional; Number of most recent buckets to consider.

        Returns:
            Dictionary of {[couter name], [count]}
        """
        values = {}
        # Ensure consistent time across all keys in group
        now = time.time()
        # Could do this in a pipeline but if a group is huge
        # it might be better to do them one at a time
        for name in self.group():
            values[name] = self.count(
                name, recent_buckets=recent_buckets, now=now
            )

        return values

    def group_buckets_counts(self, recent_buckets=None):
        """
        Get count for each counter and bucket in group.

        Args:
            recent_buckets: Optional; Number of most recent buckets to consider.

        Returns:
            Dictionary of {[counter name], [BucketCount]}
        """
        values = {}
        now = time.time()
        for name in self.group():
            values[name] = self.buckets_counts(
                name, recent_buckets=recent_buckets, now=now
            )

        return values

    def delete(self, name=None):
        """
        Remove a counter.

        Args:
            name: Optional; Must be provided if not provided to __init__().
        """
        name = self._get_name(name)
        buckets = self._get_buckets(now=time.time())
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
        buckets = self._get_buckets(now=now)
        counter_keys = [
            self._key(key, bucket)
            for key, bucket in itertools.product(all_counters, buckets)
        ]
        self.redis.delete(self._group_key(), *counter_keys)
