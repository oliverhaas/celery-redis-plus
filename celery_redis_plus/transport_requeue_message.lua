-- Lua script for requeuing a single rejected message to its queue.
-- Sets redelivered=1 and adds to queue with appropriate score.
-- Uses routing_key from the hash as the queue name.
-- KEYS: [1] = message_key
-- ARGV: [1] = leftmost (1 or 0), [2] = priority_multiplier, [3] = message_ttl,
--       [4] = global_keyprefix, [5] = queue_key_prefix, [6] = message_key_prefix,
--       [7] = visibility_timeout, [8] = messages_index_prefix
-- Returns: 1 if requeued, 0 if message not found

local message_key = KEYS[1]
local leftmost = tonumber(ARGV[1]) == 1
local priority_multiplier = tonumber(ARGV[2])
local message_ttl = tonumber(ARGV[3])
local global_keyprefix = ARGV[4]
local queue_key_prefix = ARGV[5]
local message_key_prefix = ARGV[6]
local visibility_timeout = tonumber(ARGV[7])
local messages_index_prefix = ARGV[8]

-- Get priority and routing_key (queue) from hash
local fields = redis.call('HMGET', message_key, 'priority', 'routing_key')
local priority = fields[1]
local routing_key = fields[2]
if not priority or not routing_key then
    return 0
end

-- Mark as redelivered
redis.call('HSET', message_key, 'redelivered', '1')
if message_ttl >= 0 then
    redis.call('EXPIRE', message_key, message_ttl)
end

-- Get current time
local now_result = redis.call('TIME')
local now_sec = tonumber(now_result[1])
local now_ms = now_sec * 1000 + math.floor(tonumber(now_result[2]) / 1000)

-- Calculate score
local score
if leftmost then
    score = 0
else
    priority = tonumber(priority)
    score = (255 - priority) * priority_multiplier + now_ms
end

-- Add to queue (routing_key with global prefix and queue: prefix)
local queue_key = global_keyprefix .. queue_key_prefix .. routing_key
-- Extract delivery tag by stripping the known prefix (global_keyprefix + message_key_prefix)
local tag = string.sub(message_key, #global_keyprefix + #message_key_prefix + 1)
redis.call('ZADD', queue_key, score, tag)

-- Update messages_index with new queue_at (now + visibility_timeout)
local index_key = global_keyprefix .. messages_index_prefix .. routing_key
redis.call('ZADD', index_key, now_sec + visibility_timeout, tag)

return 1
