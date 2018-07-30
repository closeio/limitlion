"""LimitLion tests."""

import math
import time

import pytest
import redis

import limitlion

REDIS_HOST = 'localhost'
REDIS_PORT = 36379
REDIS_DB = 1

TEST_PARAMETERS = []
for window in (1, 2, 5, 10):
    for burst in (1, 2, 3.3, 10):
        for rps in (.0001, .2, .5, .6, 1, 2, 2.2, 5, 10):
            TEST_PARAMETERS.append((rps, burst, window))


class TestThrottleNotConfigured():
    """
    Tests throttle configuration check.

    This test is run first before it is configured for the remaining tests.
    """

    def test_not_configured(self):
        with pytest.raises(RuntimeError) as excinfo:
            limitlion.throttle('test', 1, 1, 1, 1)
        assert 'Throttle is not configured' in str(excinfo.value)


class TestThrottle():
    """
    Tests throttle.
    """

    def setup_method(self, test_method):
        """Test setup."""

        self.redis = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)
        limitlion.throttle_configure(self.redis, True)

        self.redis.flushdb()

    def _get_redis_key(self, name):
        return limitlion.KEY_FORMAT.format(name)

    def _fake_work(self, key, rps=5, burst=1, window=5, requested_tokens=1):
        return limitlion.throttle(key, rps, burst,
                                           window, requested_tokens)

    @staticmethod
    def _get_microseconds(time):
        return int((time - int(time)) * 1000000)

    def _fake_bucket_tokens(self, key, tokens, refreshed):
        """Create a faked token bucket in Redis."""

        assert refreshed == int(refreshed)
        self.redis.hmset(key, {'tokens': tokens, 'refreshed': refreshed})

    def _freeze_redis_time(self, seconds=int(time.time()), microseconds=0):
        """
        Freeze time in Redis.

        Utilizes the modified Lua script loaded when the throttle is configured
        with testing=True.  The modified script will use time values from the
        keys frozen_second and frozen_microsecond instead of the Redis TIME
        command.
        """

        # Confirm this is being called properly with whole numbers
        assert seconds == int(seconds)
        assert microseconds == int(microseconds)
        assert seconds >= 0
        assert microseconds >= 0

        # Pull whole seconds out of microseconds
        seconds += int(microseconds / 1000000)
        microseconds = microseconds % 1000000

        self.redis.mset(frozen_second=seconds, frozen_microsecond=microseconds)

    @pytest.mark.parametrize('rps, burst, window', TEST_PARAMETERS)
    def test_bursting(self, rps, burst, window):
        """Test bursting logic."""

        capacity = math.ceil(rps * burst * window)
        start_time = int(time.time())
        self._freeze_redis_time(start_time, 0)

        allowed, tokens, sleep = self._fake_work('test', rps, burst, window)
        assert allowed is True
        assert tokens == capacity - 1

        self._freeze_redis_time(start_time + window, 500000)
        allowed, tokens, sleep = self._fake_work('test', rps, burst,
                                                 window, capacity)
        assert allowed is True
        assert tokens == 0

        self._freeze_redis_time(start_time + 2 * window, 500000)
        allowed, tokens, sleep = self._fake_work('test', rps, burst, window)
        assert allowed is True
        assert tokens == math.ceil(rps * window) - 1

    @pytest.mark.parametrize('rps, burst, window', TEST_PARAMETERS)
    def test_zero_rps(self, rps, burst, window):
        """Test RPS set to 0."""

        allowed, tokens, sleep = self._fake_work('test', 0, burst, window)
        assert allowed is False
        assert sleep == window
        assert tokens == 0

    @pytest.mark.parametrize('rps, burst, window', TEST_PARAMETERS)
    def test_request_all_tokens(self, rps, burst, window):
        """Test request all tokens in one request."""

        allowed, tokens, sleep = self._fake_work('test', rps, burst, window,
                                                 (rps * burst * window))
        assert allowed is True
        assert sleep <= window
        assert tokens == 0

    @pytest.mark.parametrize('rps, burst, window', TEST_PARAMETERS)
    def test_request_too_many_tokens(self, rps, burst, window):
        """Test requesting capacity plus 1."""

        allowed, tokens, sleep = self._fake_work('test', rps, burst, window,
                                                 (rps * burst * window) + 1)
        assert allowed is False
        assert sleep <= window
        assert tokens == math.ceil(rps * burst * window)

    @pytest.mark.parametrize('rps, burst, window', TEST_PARAMETERS)
    def test_multiple_throttles(self, rps, burst, window):
        """Test multiple throttles."""

        throttle_name_1 = 'test1'
        throttle_redis_key_1 = self._get_redis_key(throttle_name_1)
        throttle_name_2 = 'test2'
        throttle_redis_key_2 = self._get_redis_key(throttle_name_2)

        start_time = int(time.time())
        # Fake bucket with two tokens left
        self._fake_bucket_tokens(throttle_redis_key_1, 1, start_time)
        self._fake_bucket_tokens(throttle_redis_key_2, 3, start_time)

        # Set time 4 microseconds into the first second of this window
        self._freeze_redis_time(start_time, 4)

        allowed, tokens, sleep = self._fake_work(throttle_name_1,
                                                 rps, burst, window)
        assert allowed is True
        assert tokens == 0

        allowed, tokens, sleep = self._fake_work(throttle_name_1,
                                                 rps, burst, window)
        assert allowed is False
        assert tokens == 0

        # Second throttle, add +1 to RPS just to test with a different value
        # for the second throttle
        allowed, tokens, sleep = self._fake_work(throttle_name_2,
                                                 rps + 1, burst, window)
        assert allowed is True
        assert tokens == min(math.ceil((rps + 1) * window * burst), 3) - 1

        # Confirm first throttle is still out of tokens
        allowed, tokens, sleep = self._fake_work(throttle_name_1,
                                                 rps, burst, window)
        assert allowed is False
        assert tokens == 0

    @pytest.mark.parametrize('rps, burst, window', TEST_PARAMETERS)
    def test_rate_limits(self, rps, burst, window):
        """Test requests over allowable limit."""

        # Don't include burst in this capacity because we don't start with
        # an empty bucket which is when burst tokens are available
        capacity = math.ceil(rps * window)

        # Max capacity is needed because the Lua script will fix buckets
        # that we add too many tokens with a call to _fake_bucket_tokens.
        # For example, setting tokens to 2 with rps, burst, window = 1 is
        # technically too many tokens in that bucket.
        max_capacity = math.ceil(rps * window * burst)

        throttle_name = 'test'
        throttle_redis_key = self._get_redis_key(throttle_name)

        start_time = int(time.time())
        # Fake bucket with two tokens left
        self._fake_bucket_tokens(throttle_redis_key, 2, start_time)
        # Set time 4 microseconds into the first second of this window
        self._freeze_redis_time(start_time, 4)

        # Fist call should be under limit
        allowed, tokens, sleep = self._fake_work(throttle_name,
                                                 rps, burst, window)
        assert allowed is True
        assert tokens == min(max_capacity, 2) - 1

        # Second call may be allowed depending on RPS
        allowed, tokens, sleep = self._fake_work(throttle_name,
                                                 rps, burst, window)
        assert allowed is (min(max_capacity, 2) - 1 > 0)
        assert tokens == 0

        # Third call should be over limit
        allowed, tokens, sleep = self._fake_work(throttle_name,
                                                 rps, burst, window)
        assert allowed is False
        assert tokens == 0
        # This might need to be switched to checking if it is within +/- 1
        # microsecond to deal with floating point rounding madness.
        assert sleep == window - .000004

        # Call should succeed if we come back exactly at the sleep time.
        # Next window starts at:
        # start_time + whole seconds of sleep + microseconds of sleep + initial
        # 4 microsecond of start time
        # Floating point comparison madness with + 4 versus + 5
        self._freeze_redis_time(int(start_time) + int(sleep),
                                TestThrottle._get_microseconds(sleep) + 5)
        allowed, tokens, sleep = self._fake_work(throttle_name,
                                                 rps, burst, window)
        assert allowed is True
        assert tokens == capacity - 1

    def test_changing_settings(self):
        """Test changing throttle settings."""

        throttle_name = 'test'

        start_time = int(time.time())
        self._freeze_redis_time(start_time, 0)

        # Fist call should be under limit
        allowed, tokens, sleep = self._fake_work(throttle_name, 5, 1, 5)
        assert allowed is True
        assert tokens == 24

        limitlion.throttle_set(throttle_name, 10, 1, 5)

        # Second call should still be under limit and changing default should
        # not change tokens remaining
        allowed, tokens, sleep = self._fake_work(throttle_name, 100, 100, 100)
        assert allowed is True
        assert tokens == 23

        self._freeze_redis_time(start_time + 6, 0)
        # This would actually temporarily starve throttles because they would
        # not add tokens for an extra 5 seconds longer than planned.
        limitlion.throttle_set(throttle_name, 10, 1, 10)

        allowed, tokens, sleep = self._fake_work(throttle_name, 5, 1, 5)
        assert allowed is True
        assert tokens == 22

        # Move into next window
        self._freeze_redis_time(start_time + 11, 0)

        allowed, tokens, sleep = self._fake_work(throttle_name, 5, 1, 5)
        assert allowed is True
        assert tokens == 99

    def test_get_throttle(self):
        """Test getting throttle settings."""

        throttle_name = 'test'

        start_time = int(time.time())

        self._freeze_redis_time(start_time, 0)

        self._fake_work(throttle_name, 5, 2, 6)
        tokens, refreshed, rps, burst, window = \
            limitlion.throttle_get(throttle_name)
        assert int(tokens) == 59
        assert int(refreshed) == start_time
        assert int(rps) == 5
        assert int(burst) == 2
        assert int(window) == 6

    def test_delete_throttle(self):
        """Test throttle delete."""

        throttle_name = 'test'
        self._fake_work(throttle_name, 5, 1, 5)

        limitlion.throttle_delete(throttle_name)
        key = self._get_redis_key(throttle_name)
        assert self.redis.exists(key) is False
        assert self.redis.exists(key + ':knobs') is False

    def test_throttle_wait(self):
        """
        Test wait helper method.

        Would probably be nice to add a timeout to that method so we could
        have a better test that actually waits for a little while.
        """

        throttle_func = limitlion.throttle_wait('test', rps=123)
        throttle_func()
