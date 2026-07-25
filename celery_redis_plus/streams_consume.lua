-- Lua script for atomic single-message consumption from priority streams.
-- Iterates one queue's level streams from highest to lowest priority and
-- reads one entry via XREADGROUP, which delivers the entry and registers
-- it in the consumer group's PEL in a single atomic step. XREADGROUP
-- without a blocking timeout is legal inside Lua scripts.
--
-- Handles expired entries (x-message-ttl) lazily: an entry whose ID
-- timestamp is older than the TTL is XACK+XDEL'd and the same stream is
-- retried, so expired messages are dropped instead of delivered.
--
-- KEYS: [stream:q:9, stream:q:6, ...] - one queue's level stream keys,
--       highest priority level first (with global_keyprefix applied)
-- ARGV: [1] = consumer group name,
--       [2] = consumer name,
--       [3] = now in milliseconds,
--       [4] = message TTL in milliseconds (0 = no TTL)
-- Returns: {stream_key, entry_id, payload} or false (nil to redis-py)

local group = ARGV[1]
local consumer = ARGV[2]
local now_ms = tonumber(ARGV[3])
local ttl_ms = tonumber(ARGV[4])

for i = 1, #KEYS do
    local key = KEYS[i]
    local more = true
    while more do
        local res = redis.call('XREADGROUP', 'GROUP', group, consumer, 'COUNT', 1, 'STREAMS', key, '>')
        if res == false or #res == 0 or #res[1][2] == 0 then
            more = false  -- Stream empty, try next level
        else
            local entry = res[1][2][1]
            local entry_id = entry[1]
            -- Entries carry a single 'payload' field holding the serialized message
            local payload = entry[2][2]
            local id_ms = tonumber(string.match(entry_id, '^(%d+)'))
            if ttl_ms > 0 and id_ms < now_ms - ttl_ms then
                -- Expired (x-message-ttl): drop and retry the same stream
                redis.call('XACK', key, group, entry_id)
                redis.call('XDEL', key, entry_id)
            else
                return {key, entry_id, payload}
            end
        end
    end
end

return false
