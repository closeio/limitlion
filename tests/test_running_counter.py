"""LimitLion tests."""
import math

import time

import pytest
import limitlion


@pytest.fixture
def counter(redis):
    limitlion.running_counter_configure(redis, True)


class TestRunningCounter:
    """
    Tests throttle.
    """

    def test_running_counter(self, redis, counter):
        key = 'test'
        period = 10
        interval = 5

        # Start counter now
        now = start = int(time.time())
        self._freeze_redis_time(redis, now, 0)

        # Add two values to current bucket
        limitlion.running_counter_update(key, interval, period, 1)
        limitlion.running_counter_update(key, interval, period, 1.2)

        running_counter = limitlion.running_counter_counts(
            key, interval, period, _now=now
        )
        bucket = int(math.floor(now / interval))
        assert running_counter[0] == bucket
        zipped = zip(running_counter[1], running_counter[2])
        assert zipped == [(bucket, 2.2)]

        # Move half way into window and add value to bucket
        now = start + int(period * interval / 2)
        self._freeze_redis_time(redis, now, 0)
        limitlion.running_counter_update(key, interval, period, 2.3)

        running_counter = limitlion.running_counter_counts(
            key, interval, period, _now=now
        )
        new_bucket = int(math.floor(now / interval))
        assert running_counter[0] == math.floor(now / interval)
        zipped = zip(running_counter[1], running_counter[2])
        assert zipped == [(bucket, 2.2), (new_bucket, 2.3)]

        # Drop first bucket
        now = start + period * interval
        self._freeze_redis_time(redis, now, 0)
        running_counter = limitlion.running_counter_counts(
            key, interval, period, _now=now
        )
        assert running_counter[0] == math.floor(now / interval)
        zipped = zip(running_counter[1], running_counter[2])
        assert zipped == [(new_bucket, 2.3)]

        # Drop all buckets
        now = start + period * interval + int(period * interval / 2)
        self._freeze_redis_time(redis, now, 0)
        running_counter = limitlion.running_counter_counts(
            key, interval, period, _now=now
        )
        assert running_counter[0] == math.floor(now / interval)
        zipped = zip(running_counter[1], running_counter[2])
        assert zipped == []
