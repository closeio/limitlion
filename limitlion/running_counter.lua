-- Using the time command requires this script to be replicated via commands
redis.replicate_commands()

local key = KEYS[1]
local interval = tonumber(ARGV[1])
local periods = tonumber(ARGV[2])
local amount = tonumber(ARGV[3])

local time = redis.call('time')
local now = tonumber(time[1])

--Current bucket
local bucket = math.floor(now / interval)

--Increment the current bucket value
local bucket_key = key..':'..bucket
redis.call('incrbyfloat', bucket_key, amount)

--Make sure the current bucket name is in the
--list of buckets.
redis.call('zadd', key, bucket, bucket)

--Remove old buckets
local max_old_bucket = bucket - periods
redis.call('zremrangebyscore', key, 0, max_old_bucket)

--Add an extra minute to give time for running_counter_get.lua
--callers to get the list of bucket names and then retrieve
--the values from Redis.
local expire = periods * interval + 60
redis.call('expire', bucket_key, expire)
redis.call('expire', key, expire)
