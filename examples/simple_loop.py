#!/usr/bin/env python
"""
Throttle example.

Simple loop using a throttle with 5 RPS, burst of 4 and 2 second window.  Run
multiple of these to get an idea how it limits all processes to 5 RPS after
the burst tokens are consumed.
"""

import datetime
import time

import redis

from limitlion import throttle, throttle_configure, throttle_reset

redis = redis.Redis('localhost', 6379)

throttle_configure(redis)
# The default settings have a TTL of 7 days so we reset for this example
# script so changes to the defaults are applied immediately.  Normally
# you would not reset the throttle during startup.
throttle_reset('test')

i = 0
while True:
    allowed, tokens, sleep = throttle('test_simple', 5, 4, 2)
    if allowed:
        i += 1
        print(
            '{}-{} Work number {}'.format(datetime.datetime.now(), tokens, i)
        )
    else:
        print('Sleeping {}'.format(sleep))
        time.sleep(sleep)
        i = 0
