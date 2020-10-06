"""LimitLion tests."""
import math
import time

import pytest

from limitlion.running_counter import BucketValue, RunningCounter


class TestRunningCounter:
    """
    Running Counter tests.
    """

    def test_main(self, redis):
        key = 'test'
        period = 10
        interval = 5

        # Start counter now
        now = start = int(time.time())

        counter = RunningCounter(redis, interval, period, key,)
        # Add two values to current bucket
        counter.inc(1, _now=now)
        counter.inc(1.2, _now=now)

        buckets = counter.counts(_now=now)
        bucket = int(math.floor(now / interval))
        assert buckets == [
            BucketValue(bucket, 2.2),
        ]
        assert counter.count(_now=now) == 2.2

        # Move half way into window and add value to bucket
        now = start + int(period * interval / 2)
        counter.inc(2.3, _now=now)
        buckets = counter.counts(_now=now)
        new_bucket = int(math.floor(now / interval))
        assert buckets == [
            BucketValue(new_bucket, 2.3),
            BucketValue(bucket, 2.2),
        ]
        assert counter.count(_now=now) == 4.5

        # Move forward enough to drop first bucket
        now = start + period * interval + 1
        buckets = counter.counts(_now=now)
        assert buckets == [BucketValue(new_bucket, 2.3)]
        assert counter.count(_now=now) == 2.3

        # Move forward enough to drop all buckets
        now = start + period * interval + int(period * interval / 2)
        buckets = counter.counts(_now=now)
        assert buckets == []
        assert counter.count(_now=now) == 0

    def test_multi_keys(self, redis):
        period = 10
        interval = 5

        # Start counter now
        now = int(time.time())

        counter = RunningCounter(redis, interval, period)

        # Fail incrementing since no key provided to __init__
        with pytest.raises(ValueError):
            counter.inc(1)

        # Increment two different keys
        counter.inc(1.2, 'test')
        counter.inc(2.2, 'test2')
        buckets = counter.counts(key='test', _now=now)
        bucket = int(math.floor(now / interval))
        assert buckets == [BucketValue(bucket, 1.2)]
        assert counter.count(key='test', _now=now) == 1.2

        buckets = counter.counts(key='test2', _now=now)
        assert buckets == [BucketValue(bucket, 2.2)]
        assert counter.count(key='test2', _now=now) == 2.2

    def test_window(self, redis):
        counter = RunningCounter(redis, 9, 8, 'test')
        assert counter.window == 72  # Seconds

    def test_redis_expirations(self, redis):
        # Test TTL when specifying key in constructor
        key = 'test'
        counter = RunningCounter(redis, 9, 8, key)
        counter.inc(2.3)
        buckets = counter.counts()
        ttl = redis.ttl(counter._key(key, buckets[0].bucket))
        assert ttl > counter.window

        # Test TTL when specifying key in inc
        key = 'test2'
        counter = RunningCounter(redis, 9, 8)
        counter.inc(2.3, key=key)
        buckets = counter.counts(key=key)
        ttl = redis.ttl(counter._key(key, buckets[0].bucket))
        assert ttl > counter.window

    def test_groups(self, redis):
        counter = RunningCounter(redis, 10, 10, group='group')
        counter.inc(1.2, 'test')
        counter.inc(2.2, 'test2')

        assert counter.group() == ['test', 'test2']
        assert counter.group_counts() == {'test': 1.2, 'test2': 2.2}

        # Make sure there aren't collisions between two groups
        # using the same keys
        counter = RunningCounter(redis, 10, 10, group='group2')
        counter.inc(1.2, 'test')
        counter.inc(2.2, 'test2')

        assert counter.group() == ['test', 'test2']
        assert counter.group_counts() == {'test': 1.2, 'test2': 2.2}

    def test_group_key_purging(self, redis):
        now = int(time.time())
        counter = RunningCounter(redis, 10, 10, group='group')
        counter.inc(1.2, 'test', _now=now)
        print (type(counter.group()[0]))
        assert counter.group() == ['test']
        counter.inc(2.2, 'test2', _now=now + counter.window)
        assert counter.group() == ['test', 'test2']
        # One second past window should result in first key being
        # removed from the zset
        counter.inc(2.2, 'test2', _now=now + counter.window + 1)
        assert counter.group() == ['test2']

    def test_group_bad_init(self, redis):
        with pytest.raises(ValueError):
            RunningCounter(redis, 1, 1, key='test', group='group')

    def test_empty_counter(self, redis):
        counter = RunningCounter(redis, 1, 1, key='test_empty')
        count = counter.count()
        assert count == 0
