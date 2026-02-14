-- Lua script for requeuing a single rejected message to its queue.
-- Sets redelivered=1 and adds to queue with appropriate score.
-- Uses routing_key from the hash as the queue name.
-- KEYS: [1] = message_key
-- ARGV: [1] = leftmost (1 or 0), [2] = priority_multiplier, [3] = message_ttl,
--       [4] = global_keyprefix, [5] = queue_key_prefix
-- Returns: 1 if requeued, 0 if message not found

local message_key = KEYS[1]
local leftmost = tonumber(ARGV[1]) == 1
local priority_multiplier = tonumber(ARGV[2])
local message_ttl = tonumber(ARGV[3])
local global_keyprefix = ARGV[4]
local queue_key_prefix = ARGV[5]

-- Get priority and routing_key (queue) from hash
local priority = redis.call('HGET', message_key, 'priority')
local routing_key = redis.call('HGET', message_key, 'routing_key')
if not priority or not routing_key then
    return 0
end

-- Mark as redelivered
redis.call('HSET', message_key, 'redelivered', '1')
if message_ttl >= 0 then
    redis.call('EXPIRE', message_key, message_ttl)
end

-- Calculate score
local score
if leftmost then
    score = 0
else
    priority = tonumber(priority)
    local now_ms = redis.call('TIME')
    now_ms = tonumber(now_ms[1]) * 1000 + math.floor(tonumber(now_ms[2]) / 1000)
    score = (255 - priority) * priority_multiplier + now_ms
end

-- Add to queue (routing_key with global prefix and queue: prefix)
local queue_key = global_keyprefix .. queue_key_prefix .. routing_key
local tag = string.match(message_key, ':(.+)$')
redis.call('ZADD', queue_key, score, tag)

return 1
