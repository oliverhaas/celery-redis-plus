-- Lua script for enqueuing messages whose queue_at time has passed.
-- This handles both delayed messages (first delivery) and timed-out messages (redelivery).
-- Uses per-message hashes: message:{tag} with fields: payload, routing_key, priority, native_delayed, eta
-- For native_delayed messages: set native_delayed=0 (first delivery, not a redelivery)
-- For timed-out messages: set redelivered=1 (message was consumed but not acked)
-- Reads routing_key from hash to add message to the correct queue.
-- KEYS: [1] = messages_index
-- ARGV: [1] = threshold, [2] = batch_limit, [3] = visibility_timeout,
--       [4] = priority_multiplier, [5] = message_key_prefix, [6] = global_keyprefix,
--       [7] = queue_key_prefix
-- Returns: total number of messages enqueued

local messages_index = KEYS[1]
local threshold = tonumber(ARGV[1])
local batch_limit = tonumber(ARGV[2])
local visibility_timeout = tonumber(ARGV[3])
local priority_multiplier = tonumber(ARGV[4])
local message_key_prefix = ARGV[5]
local global_keyprefix = ARGV[6]
local queue_key_prefix = ARGV[7]
local total_enqueued = 0

-- Get current time in seconds and milliseconds
local time_result = redis.call('TIME')
local now_sec = tonumber(time_result[1])
local now_ms = now_sec * 1000 + math.floor(tonumber(time_result[2]) / 1000)

-- Get messages ready for enqueue (score <= threshold)
local ready = redis.call('ZRANGEBYSCORE', messages_index, '-inf', threshold, 'LIMIT', 0, batch_limit)

for _, tag in ipairs(ready) do
    -- Build prefixed message key
    local message_key = global_keyprefix .. message_key_prefix .. tag

    -- Get fields from per-message hash
    local priority = redis.call('HGET', message_key, 'priority')
    if priority then
        priority = tonumber(priority)
        local routing_key = redis.call('HGET', message_key, 'routing_key')
        local eta = redis.call('HGET', message_key, 'eta')
        eta = eta and tonumber(eta) or 0

        -- Calculate queue score using eta if it's in the future, else use now
        local score_time_ms
        if eta > 0 and eta * 1000 > now_ms then
            score_time_ms = eta * 1000
        else
            score_time_ms = now_ms
        end
        local queue_score = (255 - priority) * priority_multiplier + score_time_ms

        -- Check if this is a native delayed message (first delivery) or a timed-out message (redelivery)
        local native_delayed = redis.call('HGET', message_key, 'native_delayed')
        if native_delayed and tonumber(native_delayed) == 1 then
            -- Native delayed message: clear the flag (this is the first delivery)
            redis.call('HSET', message_key, 'native_delayed', '0')
        else
            -- Timed-out message: mark as redelivered
            redis.call('HSET', message_key, 'redelivered', '1')
        end

        -- Add to the message's queue (with global prefix and queue: prefix)
        local queue_key = global_keyprefix .. queue_key_prefix .. routing_key
        redis.call('ZADD', queue_key, 'NX', queue_score, tag)

        -- Update queue_at for next cycle (now + visibility_timeout)
        local new_queue_at = now_sec + visibility_timeout
        redis.call('ZADD', messages_index, new_queue_at, tag)
        total_enqueued = total_enqueued + 1
    else
        -- No message hash = message was already acked/deleted, clean up index
        redis.call('ZREM', messages_index, tag)
    end
end

return total_enqueued
