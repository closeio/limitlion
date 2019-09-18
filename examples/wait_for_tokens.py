#!/usr/bin/env python
"""
Waiting throttle example.

Uses the throttle_wait helper that will sleep until work is allowed.
"""

import datetime

import redis

from limitlion import throttle_configure, throttle_wait

redis = redis.Redis('localhost', 6379)

throttle_configure(redis)
throttle = throttle_wait('test_wait', rps=5)

while True:
    # May sleep forever if tokens never become available
    throttle()
    print('{} Doing work'.format(datetime.datetime.now()))
