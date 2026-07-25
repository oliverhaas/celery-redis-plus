-- Lua script for moving due delayed messages into their priority streams.
-- Members of the delayed zset are full serialized message JSON strings with
-- score = absolute delivery time in ms. Due members are XADDed to the stream
-- for their bucketed priority level, then removed from the zset.
-- Message TTL (x-message-ttl) is applied lazily: members whose delivery time
-- passed more than message_ttl_ms ago are removed without being moved.
-- Priority is read from the serialized message via cjson; malformed JSON or
-- a missing priority defaults to 0 (lowest step).
--
-- Current time is read via redis.call('TIME') rather than a caller-supplied
-- timestamp: a client clock ahead of the server would judge messages due
-- (or expired) before the server's own clock agrees, and once XADD/ZREM run
-- there is no undoing it (mirrors transport_enqueue_due_messages.lua and
-- streams_consume.lua).
--
-- The number of members processed per call is bounded by batch_limit via
-- the ZRANGEBYSCORE LIMIT clause below, so the single for loop over `due`
-- cannot iterate an unbounded number of times.
--
-- KEYS: [1] = delayed:{queue} (passed with global_keyprefix applied)
--       [2..N] = stream:{queue}:{level} keys, ASCENDING by priority step
--                (KEYS[i + 1] is the stream for the i-th step in ARGV[3])
-- ARGV: [1] = batch_limit, [2] = message_ttl_ms (0 = no TTL),
--       [3] = priority steps CSV, ascending (e.g. "0,3,6,9")
-- Returns: number of messages moved

local delayed_key = KEYS[1]
local batch_limit = tonumber(ARGV[1])
local message_ttl_ms = tonumber(ARGV[2])

local time_result = redis.call('TIME')
local now_ms = tonumber(time_result[1]) * 1000 + math.floor(tonumber(time_result[2]) / 1000)

-- Parse the ascending priority steps CSV
local steps = {}
for step in string.gmatch(ARGV[3], '([^,]+)') do
    steps[#steps + 1] = tonumber(step)
end

local moved = 0

-- Get due members with scores (score = absolute delivery time in ms)
local due = redis.call('ZRANGEBYSCORE', delayed_key, '-inf', now_ms, 'WITHSCORES', 'LIMIT', 0, batch_limit)

for i = 1, #due, 2 do
    local member = due[i]
    local score = tonumber(due[i + 1])

    if message_ttl_ms > 0 and score < now_ms - message_ttl_ms then
        -- Expired (x-message-ttl): drop without moving
        redis.call('ZREM', delayed_key, member)
    else
        -- Read priority from the serialized message (default 0 on any failure)
        local priority = 0
        local ok, decoded = pcall(cjson.decode, member)
        if ok and type(decoded) == 'table' and type(decoded['properties']) == 'table' then
            priority = tonumber(decoded['properties']['priority']) or 0
        end

        -- Highest step <= priority; lowest step if priority is below all steps
        local level_index = 1
        for j = 1, #steps do
            if steps[j] <= priority then
                level_index = j
            end
        end

        redis.call('XADD', KEYS[level_index + 1], '*', 'payload', member)
        redis.call('ZREM', delayed_key, member)
        moved = moved + 1
    end
end

return moved
