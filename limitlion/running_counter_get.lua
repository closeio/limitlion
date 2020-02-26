-- Using the time command requires this script to be replicated via commands
redis.replicate_commands()

local key = KEYS[1]
local interval = tonumber(ARGV[1])
local periods = tonumber(ARGV[2])

local time = redis.call('time')
local now = tonumber(time[1])

local bucket = math.floor(now / interval)
local max_old_bucket = bucket - periods

--Remove old buckets
redis.call('zremrangebyscore', key, 0, max_old_bucket)

--Return current set of bucket names
return {bucket, redis.call('zrange', key, 0, -1)}
