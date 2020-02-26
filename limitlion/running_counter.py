"""
A running counter keeps counts per interval measured in seconds. Keeps a bucket
for each interval up to periods. It automatically drops buckets that are outside
of the current window.

Buckets are addressed using the first epoch second for that interval calculate
as follows:

    floor(epoch seconds / interval).

So if you are using 1 hour intervals the bucket for 2/19/19 01:23:09Z would be
1550539389 / (60 * 60) = 430705

The bucket values can then be retrieved to calculate rates across the whole window
or for each bucket.

"""
import math
import time
import pkg_resources


running_counter_script = None
running_counter_get_script = None
redis = None


def _setup_script(redis, filename, testing):
    lua_script = pkg_resources.resource_string(__name__, filename).decode()

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
    return redis.register_script(lua_script)


def running_counter_configure(redis_instance, testing=False):
    """Register Lua throttle script in Redis."""
    global redis, running_counter_script, running_counter_get_script
    redis = redis_instance

    running_counter_script = _setup_script(redis, 'running_counter.lua', testing)
    running_counter_get_script = _setup_script(
        redis, 'running_counter_get.lua', testing
    )


def running_counter_counts(key, interval, periods, key_prefix='counter', _now=None):
    if not _now:
        _now = time.time()
    current_bucket = int(math.floor(_now / interval))
    buckets = [bucket for bucket in range(current_bucket, current_bucket - periods, -1)]
    pipeline = redis.pipeline()
    for bucket in buckets:
        print '{}:{}:{}'.format(key_prefix, key, bucket)
        pipeline.get('{}:{}:{}'.format(key_prefix, key, bucket))

    bucket_counts = []
    for count in pipeline.execute():
        if not count:
            bucket_counts.append(0)
        else:
            bucket_counts.append(float(count))

    print buckets
    print bucket_counts
    return buckets, bucket_counts


def running_counter_update(key, interval, periods, amount=1, key_prefix='counter', _now=None):
    """
    Update rate counter.

    Args:
        key: Redis key name
        interval: Interval in seconds
        periods: Number of periods in window
        amount: Amount to increment counter (float)
        _now: For testing

    """

    if not _now:
        _now = time.time()

    bucket = int(math.floor(_now / interval))
    bucket_key = '{}:{}:{}'.format(key_prefix, key, bucket)
    print bucket_key
    expire = periods * interval + 60

    pipeline = redis.pipeline()
    pipeline.incrbyfloat(bucket_key, amount)
    pipeline.expire(bucket_key, expire)
    pipeline.execute()
