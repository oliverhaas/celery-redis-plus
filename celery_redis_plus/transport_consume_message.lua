-- Lua script for atomic message consumption from sorted set queues.
-- Pops the highest-priority message across multiple queues, refreshes the
-- messages_index score for visibility timeout tracking, and returns the
-- message data — all in a single atomic operation.
--
-- Handles expired message hashes (x-message-ttl) by cleaning up the index
-- entry and trying the next message.
--
-- KEYS: [queue:q1, queue:q2, ...] — queue sorted set keys (with global_keyprefix applied)
-- ARGV: [1] = global_keyprefix, [2] = message_key_prefix,
--       [3] = new_queue_at (now + visibility_timeout, as string number),
--       [4] = messages_index_prefix,
--       [5..5+N-1] = queue_name_1, queue_name_2, ... (raw names, same order as KEYS)
-- Returns: {queue_name, delivery_tag, payload, restore_count} or nil

local global_keyprefix = ARGV[1]
local message_key_prefix = ARGV[2]
local new_queue_at = tonumber(ARGV[3])
local messages_index_prefix = ARGV[4]
local num_queues = #KEYS
local max_attempts = 100

for _attempt = 1, max_attempts do
    -- Find the queue with the lowest minimum score (highest priority message)
    local best_score = nil
    local best_idx = nil
    for i = 1, num_queues do
        local peek = redis.call('ZRANGE', KEYS[i], 0, 0, 'WITHSCORES')
        if #peek > 0 then
            local score = tonumber(peek[2])
            if not best_score or score < best_score then
                best_score = score
                best_idx = i
            end
        end
    end

    if not best_idx then
        return nil  -- All queues empty
    end

    -- Atomically pop from the best queue
    local result = redis.call('ZPOPMIN', KEYS[best_idx], 1)
    if #result == 0 then
        return nil  -- Shouldn't happen inside Lua, but be safe
    end

    local tag = result[1]
    local queue_name = ARGV[4 + best_idx]

    -- Fetch message data
    local message_key = global_keyprefix .. message_key_prefix .. tag
    local fields = redis.call('HMGET', message_key, 'payload', 'restore_count')

    if fields[1] then
        -- Valid message: refresh messages_index score for visibility timeout
        local index_key = global_keyprefix .. messages_index_prefix .. queue_name
        redis.call('ZADD', index_key, 'XX', new_queue_at, tag)
        return {queue_name, tag, fields[1], fields[2] or '0'}
    else
        -- Message hash expired (x-message-ttl): clean up index and try next
        local index_key = global_keyprefix .. messages_index_prefix .. queue_name
        redis.call('ZREM', index_key, tag)
    end
end

return nil  -- Max attempts reached (too many expired messages)
