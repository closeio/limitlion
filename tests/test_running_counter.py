"""LimitLion tests."""
import math
import time
import datetime
from freezefrog import FreezeTime

import pytest

from limitlion.running_counter import BucketValue, RunningCounter


class TestRunningCounter:
    def test_main(self, redis):
        key = 'test'
        period = 10
        interval = 5

        # Start counter now
        now = start = datetime.datetime.utcnow()
        with FreezeTime(now):
            counter = RunningCounter(
                redis,
                interval,
                period,
                key,
            )
            # Add two values to current bucket
            counter.inc(1)
            counter.inc(1.2)

            buckets = counter.counts()
            bucket = int(math.floor(time.time() / interval))
            assert buckets == [
                BucketValue(bucket, 2.2),
            ]
            assert counter.count() == 2.2

        # Move half way into window and add value to bucket
        now = start + datetime.timedelta(seconds=int(period * interval / 2))
        with FreezeTime(now):
            counter.inc(2.3)
            buckets = counter.counts()
            new_bucket = int(math.floor(time.time() / interval))
            assert buckets == [
                BucketValue(new_bucket, 2.3),
                BucketValue(bucket, 2.2),
            ]
            assert counter.count() == 4.5

        # Move forward enough to drop first bucket
        now = start + datetime.timedelta(seconds=period * interval + 1)
        with FreezeTime(now):
            buckets = counter.counts()
            assert buckets == [BucketValue(new_bucket, 2.3)]
            assert counter.count() == 2.3

        # Move forward enough to drop all buckets
        now = start + datetime.timedelta(
            seconds=period * interval + int(period * interval / 2)
        )
        with FreezeTime(now):
            buckets = counter.counts()
            assert buckets == []
            assert counter.count() == 0

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
        buckets = counter.counts(key='test', now=now)
        bucket = int(math.floor(now / interval))
        assert buckets == [BucketValue(bucket, 1.2)]
        assert counter.count(key='test', now=now) == 1.2

        buckets = counter.counts(key='test2', now=now)
        assert buckets == [BucketValue(bucket, 2.2)]
        assert counter.count(key='test2', now=now) == 2.2

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
        start = datetime.datetime.now()
        counter = RunningCounter(redis, 10, 10, group='group')
        with FreezeTime(start):
            counter.inc(1.2, 'test')

        assert counter.group() == ['test']
        with FreezeTime(start + datetime.timedelta(seconds=counter.window)):
            counter.inc(2.2, 'test2')
            assert counter.group() == ['test', 'test2']

        # One second past window should result in first key being
        # removed from the zset
        with FreezeTime(
            start + datetime.timedelta(seconds=counter.window + 1)
        ):
            counter.inc(2.2, 'test2')
            assert counter.group() == ['test2']

    def test_group_bad_init(self, redis):
        with pytest.raises(ValueError):
            RunningCounter(redis, 1, 1, key='test', group='group')

    def test_empty_counter(self, redis):
        counter = RunningCounter(redis, 1, 1, key='test_empty')
        count = counter.count()
        assert count == 0

    def test_delete_counter(self, redis):
        counter = RunningCounter(redis, 1, 1, key='key1')
        counter.inc()
        counter.delete()
        assert counter.count() == 0
        counter.inc(key="other_key")
        counter.delete(key="other_key")
        assert counter.count(key="other_key") == 0

    def test_delete_group_counter(self, redis):
        counter = RunningCounter(redis, 1, 1, group='group')
        counter.inc(key="key1")
        counter.delete(key="key1")
        assert counter.group_counts() == {}
        counter.inc(key="key1")
        counter.inc(key="key2")
        counter.delete_group()
        assert counter.group_counts() == {}
