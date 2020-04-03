-- This script implements a per second token bucket rate limiting algorithm.  It is
-- based on Stripe's published script.
-- KEYS = {}  ARGV = { throttle knob name, default rate (rps), default burst multiplier,
--                     default rate limit window in seconds, requested tokens, knobs_ttl }
-- Returns: allowed (1=allowed, 0=not allowed),
--          tokens left,
--          decimal seconds left in this window

-- Using the time command requires this script to be replicated via commands
redis.replicate_commands()

local function check_bucket(bucket_key, rate, burst, window,
                            now, requested_tokens)
  -- Checks bucket to see if a request would be allowed.
  --
  -- Args:
  --   bucket_key: Redis key name of bucket
  --   rate: Request rate per second
  --   burst: Burst multiplier
  --   window: Number of seconds in window
  --   now: Current second since epoch
  --   requested_tokens: Number of tokens requested
  --
  -- Returns:
  --   allowed: 1 if this request should allowed, otherwise 0
  --   refreshed: Window start time, whole seconds since epoch
  --   filled_tokens: How many tokens in bucket

  -- Maximum size of bucket
  local capacity = math.ceil(rate * burst * window)

  local last_tokens = tonumber(redis.call("hget", bucket_key, "tokens"))
  if last_tokens == nil then
    last_tokens = capacity
  end

  local last_refreshed = tonumber(redis.call("hget", bucket_key, "refreshed"))
  if last_refreshed == nil then
    last_refreshed = 0
  end

  -- Calculate how many new tokens should be added, can be zero
  local age = math.max(0, now-last_refreshed)
  -- Whole windows that have elapsed
  local elapsed_windows = math.floor(age / window)
  local add_tokens = math.ceil(elapsed_windows * rate * window)

  -- Fill bucket with new tokens
  local filled_tokens = math.min(capacity, last_tokens + add_tokens)

  -- Determine if this request is going to be allowed
  local allowed
  if filled_tokens >= requested_tokens then
    allowed = 1
  else
    allowed = 0
  end

  local refreshed
  if add_tokens > 0 and last_refreshed == 0 then
    -- Adding tokens to a new bucket.
    refreshed = now
  elseif add_tokens > 0 and last_refreshed ~= 0 then
    -- Add tokens to an existing bucket.
    refreshed = last_refreshed + elapsed_windows * window
  else
    -- Don't change refreshed time if we haven't added new tokens
    refreshed = last_refreshed
  end

  return {allowed, refreshed, filled_tokens}
end

local function update_bucket(bucket_key, allowed, refreshed,
                             filled_tokens, ttl, requested_tokens)
  -- Updates bucket token count, last refreshed time, and TTL
  --
  -- Args:
  --   bucket_key: Redis key name of bucket
  --   allowed: 1 if this request will be allowed, otherwise 0
  --   refreshed: Window start time, whole seconds since epoch
  --   filled_tokens: How many tokens in bucket
  --   ttl: Redis key expiration
  --
  -- Returns:
  --   new_tokens: Current number of tokens in bucket

  local new_tokens = filled_tokens
  if allowed == 1 then
    new_tokens = math.max(0, filled_tokens - requested_tokens)
  end

  redis.call("hmset", bucket_key, "tokens", new_tokens, "refreshed", refreshed)
  redis.call("expire", bucket_key, ttl)
  return new_tokens
end

local name = ARGV[1]
local default_rps = ARGV[2]
local default_burst = ARGV[3]
local default_window = ARGV[4]
local requested_tokens = tonumber(ARGV[5])
local knobs_ttl = tonumber(ARGV[6])
local rps
local burst
local window

-- Lookup throttle knob settings
local knobs_key = name .. ":knobs"
-- Use
--   HMSET <knobs_key> rps <rps> burst <burst> window <window>
-- to manually override the setting for any throttle.
local knobs = redis.call("HMGET", knobs_key, "rps", "burst", "window")
if knobs[1] == false then
  -- Use defaults if knobs hash is not found
  rps = tonumber(default_rps)
  burst = tonumber(default_burst)
  window = tonumber(default_window)
else
  rps = tonumber(knobs[1])
  burst = tonumber(knobs[2])
  window = tonumber(knobs[3])
  -- Set knobs hash expiration if knobs_ttl is specified
  if knobs_ttl > 0 then
    redis.call("EXPIRE", knobs_key, knobs_ttl)
  end
end

-- Use redis server time so it is consistent across callers
-- The following line gets replaced before loading this script during tests
-- so that we can freeze the time values.  See throttle.py.
local time = redis.call("time")
local now = tonumber(time[1])

-- Keep the hash around for twice the useful burst time to reduce unnecessary expires
local ttl = math.floor(burst * window * 2)

local tokens
local seconds_left
local allowed
if rps == 0 then
  -- rps = 0 always results in a denied request with a full window sleep
  seconds_left = window
  tokens = 0
  allowed = 0
elseif rps == -1 then
  -- rps = -1 always results in an allowed
  seconds_left = 0
  tokens = 1
  allowed = 1
else
  -- Check bucket to determine if work is allowed
  local rate = check_bucket(name, rps, burst, window, now, requested_tokens)
  tokens = update_bucket(name, rate[1], rate[2], rate[3], ttl, requested_tokens)
  allowed = rate[1]
  -- Calculate decimal seconds left in the window
  local diff = math.max(0, now - tonumber(rate[2]))
  seconds_left = (window - diff - 1) + (1000000 - tonumber(time[2])) / 1000000
end

-- string.format is necessary for seconds_left because Lua to Redis number
-- conversion automatically casts numbers to integers which would drop the microseconds
return {allowed, tokens, string.format("%.6f", seconds_left)}
