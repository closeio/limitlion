"""LimitLion tests."""
import datetime
import time

import pytest
from freezefrog import FreezeTime

from limitlion.running_counter import BucketCount, RunningCounter


class TestRunningCounter:
    def test_main(self, redis):
        name = 'test'
        num_buckets = 10
        interval = 5

        # Start counter now
        now = start = datetime.datetime.utcnow()
        with FreezeTime(now):
            counter = RunningCounter(
                redis,
                interval,
                num_buckets,
                name,
            )
            # Add two values to current bucket
            counter.inc(1)
            counter.inc(1.2)

            buckets_counts = counter.buckets_counts()
            bucket = int(time.time()) // interval
            assert buckets_counts == [
                BucketCount(bucket, 2.2),
            ]
            assert counter.count() == 2.2

        # Move half way into window and add value to bucket
        now = start + datetime.timedelta(
            seconds=int(num_buckets * interval / 2)
        )
        with FreezeTime(now):
            counter.inc(2.3)
            buckets_counts = counter.buckets_counts()
            new_bucket = int(time.time()) // interval
            assert buckets_counts == [
                BucketCount(new_bucket, 2.3),
                BucketCount(bucket, 2.2),
            ]
            assert counter.count() == 4.5

        # Move forward enough to drop first bucket
        now = start + datetime.timedelta(seconds=num_buckets * interval + 1)
        with FreezeTime(now):
            buckets_counts = counter.buckets_counts()
            assert buckets_counts == [BucketCount(new_bucket, 2.3)]
            assert counter.count() == 2.3

        # Move forward enough to drop all buckets
        now = start + datetime.timedelta(
            seconds=num_buckets * interval + int(num_buckets * interval / 2)
        )
        with FreezeTime(now):
            buckets_counts = counter.buckets_counts()
            assert buckets_counts == []
            assert counter.count() == 0

    def test_multi_counters_not_allowed(self, redis):
        counter = RunningCounter(redis, 10, 10, name='test1')

        with pytest.raises(ValueError):
            counter.inc(1, name='test2')

        with pytest.raises(ValueError):
            counter.buckets_counts(name='test2')

    def test_window(self, redis):
        counter = RunningCounter(redis, 9, 8, 'test')
        assert counter.window == 72  # Seconds

    def test_redis_expirations(self, redis):
        # Test TTL when specifying name in constructor
        name = 'test'
        counter = RunningCounter(redis, 9, 8, name)
        counter.inc(2.3)
        buckets_counts = counter.buckets_counts()
        ttl = redis.ttl(counter._key(name, buckets_counts[0].bucket))
        assert ttl > counter.window

        # Test TTL when specifying name in inc
        name = 'test2'
        counter = RunningCounter(redis, 9, 8, name)
        counter.inc(2.3)
        buckets_counts = counter.buckets_counts(name=name)
        ttl = redis.ttl(counter._key(name, buckets_counts[0].bucket))
        assert ttl > counter.window

    def test_groups(self, redis):
        counter = RunningCounter(redis, 10, 10, group_name='group')
        counter.inc(1.2, 'test')
        counter.inc(2.2, 'test2')

        assert counter.group() == ['test', 'test2']
        assert counter.group_counts() == {'test': 1.2, 'test2': 2.2}

        # Make sure there aren't collisions between two groups
        # using the same names
        counter = RunningCounter(redis, 10, 10, group_name='group2')
        counter.inc(1.2, 'test')
        counter.inc(2.2, 'test2')

        assert counter.group() == ['test', 'test2']
        assert counter.group_counts() == {'test': 1.2, 'test2': 2.2}

    def test_group_counter_purging(self, redis):
        start = datetime.datetime.now()
        counter = RunningCounter(redis, 10, 10, group_name='group')
        with FreezeTime(start):
            counter.inc(1.2, 'test')

        assert counter.group() == ['test']
        with FreezeTime(start + datetime.timedelta(seconds=counter.window)):
            counter.inc(2.2, 'test2')
            assert counter.group() == ['test', 'test2']

        # One second past window should result in first counter being
        # removed from the zset
        with FreezeTime(
            start + datetime.timedelta(seconds=counter.window + 1)
        ):
            counter.inc(2.2, 'test2')
            assert counter.group() == ['test2']

    def test_group_bad_init(self, redis):
        with pytest.raises(ValueError):
            RunningCounter(redis, 1, 1, name='test', group_name='group')

    def test_empty_counter(self, redis):
        counter = RunningCounter(redis, 1, 1, name='test_empty')
        count = counter.count()
        assert count == 0

    def test_delete_counter(self, redis):
        counter = RunningCounter(redis, 1, 1, name='name1')
        counter.inc()
        counter.delete()
        assert counter.count() == 0

    def test_delete_group_counter(self, redis):
        counter = RunningCounter(redis, 1, 1, group_name='group')
        counter.inc(name="name1")
        counter.delete(name="name1")
        assert counter.group_counts() == {}
        counter.inc(name="name1")
        counter.inc(name="name2")
        counter.delete_group()
        assert counter.group_counts() == {}

    def test_group_counts_specify_recent_buckets(self, redis):
        start = datetime.datetime.now()
        counter = RunningCounter(redis, 10, 10, group_name='group')
        with FreezeTime(start):
            counter.inc(1, 'counter1')
            counter.inc(3, 'counter2')

        with FreezeTime(start + datetime.timedelta(seconds=counter.interval)):
            counter.inc(1, 'counter1')
            counter.inc(3, 'counter2')
        with FreezeTime(
            start + datetime.timedelta(seconds=counter.interval * 2)
        ):
            counter.inc(1, 'counter1')
            counter.inc(3, 'counter2')
            assert counter.group_counts(recent_buckets=1) == {
                'counter1': 1.0,
                'counter2': 3.0,
            }
            assert counter.group_counts(recent_buckets=2) == {
                'counter1': 2.0,
                'counter2': 6.0,
            }
            assert counter.group_counts() == {
                'counter1': 3.0,
                'counter2': 9.0,
            }
            assert counter.group_counts(recent_buckets=10) == {
                'counter1': 3.0,
                'counter2': 9.0,
            }
            with pytest.raises(ValueError):
                counter.group_counts(recent_buckets=11)
