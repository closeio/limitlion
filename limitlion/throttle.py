"""Token bucket throttle backed by Redis."""

import time

import pkg_resources

KEY_FORMAT = 'throttle:{}'

# throttle knob defaults
THROTTLE_BURST_DEFAULT = 1
THROTTLE_WINDOW_DEFAULT = 5
THROTTLE_REQUESTED_TOKENS_DEFAULT = 1

# The default is to extend a throttle's knob settings TTL out
# 7 days each time the throttle is used.
DEFAULT_KNOBS_TTL = 60 * 60 * 24 * 7

throttle_script = None
redis = None


def _validate_throttle(key, params):
    check_values_pipe = redis.pipeline()
    for param, param_name in params:
        if param is not None:
            # Throttle values can only be positive floats
            try:
                assert float(param) >= 0
            except (ValueError, AssertionError):
                raise ValueError(
                    '"{}" is not a valid throttle value. Throttle values must '
                    'be positive floats.'.format(param)
                )
        else:
            check_values_pipe.hexists(key, param_name)
    if not all(check_values_pipe.execute()):
        raise IndexError(
            "Throttle knob {} doesn't exist or is invalid".format(key)
        )


def _verify_configured():
    if not redis or not throttle_script:
        raise RuntimeError('Throttle is not configured')


def throttle(
    name,
    rps,
    burst=THROTTLE_BURST_DEFAULT,
    window=THROTTLE_WINDOW_DEFAULT,
    requested_tokens=THROTTLE_REQUESTED_TOKENS_DEFAULT,
    knobs_ttl=DEFAULT_KNOBS_TTL,
):
    """
    Throttle that allows orchestration of distributed workers.

    Args:
        name: Name of throttle.  Used as part of the Redis key.
        rps: Default requests per second allowed by this throttle
        burst: Default burst multiplier
        window: Default limit window in seconds
        requested_tokens: Number of tokens required for this work request
        knobs_ttl: Throttle's knob TTL value (0 disables setting TTL)

    Returns:
        allowed: True if work is allowed
        tokens: Number of tokens left in throttle bucket
        sleep: Seconds before next limit window starts.  If work is
               not allowed you should sleep this many seconds. (float)

    The first use of a throttle will set the default values in redis for
    rps, burst, and window. Subsequent calls will use the values stored in
    Redis. This allows changes to the throttle knobs to be made on the fly by
    simply changing the values stored in redis.

    See throttle_set function to set the throttle.

    Setting RPS to 0 causes all work requests to be denied and a full sleep.
    Setting RPS to -1 causes all work requests to be allowed.

    """

    _verify_configured()
    allowed, tokens, sleep = throttle_script(
        keys=[],
        args=[
            KEY_FORMAT.format(name),
            rps,
            burst,
            window,
            requested_tokens,
            knobs_ttl,
        ],
    )
    # Converting the string sleep to a float causes floating point rounding
    # issues that limits having true microsecond resolution for the sleep
    # value.
    return allowed == 1, int(tokens), float(sleep)


def throttle_configure(redis_instance, testing=False):
    """Register Lua throttle script in Redis."""

    global redis, throttle_script
    redis = redis_instance

    lua_script = pkg_resources.resource_string(
        __name__, 'throttle.lua'
    ).decode()

    # Modify scripts when testing so time can be frozen
    if testing:
        lua_script = lua_script.replace(
            'local time = redis.call("time")',
            'local time\n'
            'if redis.call("exists", "frozen_second") == 1 then\n'
            '  time = redis.call("mget", "frozen_second", "frozen_microsecond")\n'  # noqa: E501
            'else\n'
            '  time = redis.call("time")\n'
            'end',
        )
    throttle_script = redis.register_script(lua_script)


def throttle_delete(name):
    """Delete Redis throttle data."""

    _verify_configured()
    key = KEY_FORMAT.format(name)
    pipeline = redis.pipeline()
    pipeline.delete(key)
    pipeline.delete(key + ':knobs')
    pipeline.execute()


def throttle_get(name):
    """
    Get throttle values from redis.

    Returns: (tokens, refreshed, rps, burst, window)

    """

    key = KEY_FORMAT.format(name) + ':knobs'

    # Get each value in hashes individually in case they don't exist
    get_values_pipe = redis.pipeline()
    key = KEY_FORMAT.format(name)
    get_values_pipe.hget(key, 'tokens')
    get_values_pipe.hget(key, 'refreshed')

    key = KEY_FORMAT.format(name) + ':knobs'
    get_values_pipe.hget(key, 'rps')
    get_values_pipe.hget(key, 'burst')
    get_values_pipe.hget(key, 'window')

    values = get_values_pipe.execute()
    return values


def throttle_reset(name):
    """Reset throttle settings."""

    _verify_configured()
    key = KEY_FORMAT.format(name) + ':knobs'
    redis.delete(key)


def throttle_set(name, rps=None, burst=None, window=None, knobs_ttl=None):
    """
    Adjust throttle values in redis.

    If knobs_ttl is used here the throttle() call needs to be called
    with knobs_ttl=0 so the ttl isn't also set in the Lua script
    """

    _verify_configured()
    key = KEY_FORMAT.format(name) + ':knobs'

    params = [(rps, 'rps'), (burst, 'burst'), (window, 'window')]
    _validate_throttle(key, params)

    set_values_pipe = redis.pipeline()
    for param, param_name in params:
        if param is not None:
            set_values_pipe.hset(key, param_name, param)

    if knobs_ttl:
        set_values_pipe.expire(key, knobs_ttl)

    set_values_pipe.execute()


def throttle_wait(name, *args, **kwargs):
    """Sleeps time specified by throttle if needed.

    This will wait potentially forever to get permission to do work

    Usage:
    throttle = throttle_wait('name', rps=123)
    for ...:
        throttle()
        do_work()
    """

    max_wait = kwargs.pop('max_wait', None)

    def throttle_func(requested_tokens=1):
        start_time = time.time()
        allowed, tokens, sleep = throttle(
            name, *args, requested_tokens=requested_tokens, **kwargs
        )
        while not allowed:
            if max_wait is not None and time.time() - start_time > max_wait:
                break
            time.sleep(sleep)
            allowed, tokens, sleep = throttle(
                name, *args, requested_tokens=requested_tokens, **kwargs
            )
        return allowed, tokens, sleep

    return throttle_func
