-- Lua script for requeuing a single rejected message to its queue.
-- Increments delivery_count and adds to queue with appropriate score.
-- Uses routing_key from the hash as the queue name.
-- KEYS: [1] = message_key
-- ARGV: [1] = leftmost (1 or 0), [2] = priority_multiplier, [3] = message_ttl,
--       [4] = global_keyprefix, [5] = queue_key_prefix, [6] = message_key_prefix,
--       [7] = visibility_timeout, [8] = messages_index_prefix,
--       [9] = delivery_limit (-1 = no limit)
-- Returns: 1 if requeued, 0 if message not found,
--          {-1, payload} if dropped at the delivery limit (the DEL below is
--          the message's last trace, so the payload goes back for logging)

local message_key = KEYS[1]
local leftmost = tonumber(ARGV[1]) == 1
local priority_multiplier = tonumber(ARGV[2])
local message_ttl = tonumber(ARGV[3])
local global_keyprefix = ARGV[4]
local queue_key_prefix = ARGV[5]
local message_key_prefix = ARGV[6]
local visibility_timeout = tonumber(ARGV[7])
local messages_index_prefix = ARGV[8]
local delivery_limit = tonumber(ARGV[9])

-- Get priority and routing_key (queue) from hash
local fields = redis.call('HMGET', message_key, 'priority', 'routing_key')
local priority = fields[1]
local routing_key = fields[2]
if not priority or not routing_key then
    return 0
end

-- Extract delivery tag by stripping the known prefix (global_keyprefix + message_key_prefix)
local tag = string.sub(message_key, #global_keyprefix + #message_key_prefix + 1)
local queue_key = global_keyprefix .. queue_key_prefix .. routing_key
local index_key = global_keyprefix .. messages_index_prefix .. routing_key

-- Get current time
local now_result = redis.call('TIME')
local now_sec = tonumber(now_result[1])
local now_ms = now_sec * 1000 + math.floor(tonumber(now_result[2]) / 1000)

-- Calculate score
local score
if leftmost then
    score = 0
else
    score = (255 - tonumber(priority)) * priority_multiplier + now_ms
end

-- Add to queue (routing_key with global prefix and queue: prefix).
-- NX reports whether the tag was actually absent, exactly as in the sweep:
-- when enqueue_due_messages already re-enqueued after VT expiry, the existing
-- entry stays undisturbed and the sweep already counted that redelivery, so
-- counting it again here would inflate delivery_count and drop early.
local restored = redis.call('ZADD', queue_key, 'NX', score, tag) == 1

if restored then
    -- Count the redelivery. Reject-with-requeue is a redelivery just like a
    -- visibility timeout restore, and this is the counter the AMQP
    -- redelivered flag is derived from at consume time.
    local delivery_count = redis.call('HINCRBY', message_key, 'delivery_count', 1)

    -- Enforce the limit with the sweep's counting: delivery_count counts
    -- redeliveries and is 0 on the first delivery, so reaching the limit here
    -- means the requeue would start attempt limit+1. The sweep cannot catch a
    -- live reject loop -- every consume re-stamps the index deadline, so the
    -- entry never comes due there -- which is why the check runs here too.
    if delivery_limit >= 0 and delivery_count >= delivery_limit then
        local payload = redis.call('HGET', message_key, 'payload')
        redis.call('ZREM', index_key, tag)
        redis.call('ZREM', queue_key, tag)
        redis.call('DEL', message_key)
        return {-1, payload}
    end
end

if message_ttl >= 0 then
    redis.call('EXPIRE', message_key, message_ttl)
end

-- Update messages_index with new queue_at (now + visibility_timeout)
redis.call('ZADD', index_key, now_sec + visibility_timeout, tag)

return 1
