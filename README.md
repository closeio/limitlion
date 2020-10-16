
[![CircleCI](https://circleci.com/gh/closeio/limitlion.svg?style=svg)](https://circleci.com/gh/closeio/limitlion)
[![Maintainability](https://api.codeclimate.com/v1/badges/defea1a895913b923112/maintainability)](https://codeclimate.com/github/closeio/limitlion/maintainability)
[![Test Coverage](https://api.codeclimate.com/v1/badges/defea1a895913b923112/test_coverage)](https://codeclimate.com/github/closeio/limitlion/test_coverage)
# LimitLion

A token bucket rate limiting throttle using Redis as the backend. Inspired by
Stripe's [Scaling your API with rate limiters](https://stripe.com/blog/rate-limiters)
blog post.  Can be used to control processing rates from one to many processes.
Potential implementations include protecting databases from high processing rates,
orchestrating queue consumer processes, or enforcing HTTP request rate limits.

Install with: `pip install limitlion`

Following is a simple example of a throttle named `test` that allows `5` requests per second (RPS) with
a burst factor of `2` using a `8` second window and requesting `1` token (default)
for each unit of work.  Look in the `examples` directory for more.

```py
redis = redis.Redis('localhost', 6379)
throttle_configure(redis)
while True:
    allowed, tokens, sleep = throttle('test', 5, 2, 8)
    if allowed:
        print ('Do work here')
    else:
        print ('Sleeping {}'.format(sleep))
        time.sleep(sleep)
```

## Design
The rate limiting logic uses a classic token bucket algorithm but is implemented
entirely as a Lua Redis script.  It leverages the Redis [TIME](https://redis.io/commands/time)
command which ensures fair microsecond resolution across all callers independent
of the caller's clock.  Note that buckets start and end on whole seconds.

Redis 3.2+ is required because `replicate_commands()` is used to support using
the `TIME` command in a Lua script.

## Configuring
Default values for RPS, burst factor and window size are supplied to the throttle
Lua script.  The Lua script creates a `throttle:[throttle name]:knobs` hash with
these values if it does not yet exist in Redis.  The script then uses the values
in that `knobs` hash for the token bucket calculations.  Each call also sets the
TTL for the `knobs` key to 7 days so it will remain in Redis as long as the
throttle has been active in the last week.

Since these settings are stored in Redis a separate process can be used to adjust
them on the fly.  This could simply be manually issuing the Redis command to
change the RPS or a more sophisicated process that polls Prometheus metrics to
determine the current load on your database and adjust the RPS accordingly.

# Running Counter
Another small but useful tool to keep track of counts in Redis for specified
time windows. These counts can then be used to make decisions on limiting or
 failing processes as well as for diagnostics. Checkout [`running_counter.py
`](limitlion/running_county.py) for details.