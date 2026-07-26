-- Lua script for atomically identifying and removing idle, zero-pending
-- consumers from one stream's consumer group.
-- XINFO CONSUMERS and XGROUP DELCONSUMER run in the same script, so the
-- pending/idle values a consumer is deleted on are exactly the values at
-- delete time. Doing the read and the delete as two separate round trips
-- from the client leaves a window where a peer parked in a blocking
-- XREADGROUP with zero pending can be handed a fresh entry between the
-- read and the delete; XGROUP DELCONSUMER would then remove that entry
-- from the group PEL where it is unreachable by XREADGROUP, XPENDING, or
-- XCLAIM, silently degrading at-least-once to at-most-once. Verified
-- against redis:7-alpine, valkey/valkey:latest, and redis:6.2-alpine that
-- XINFO CONSUMERS is callable from Lua and a subsequent XGROUP DELCONSUMER
-- write in the same script is permitted on all three (see carry-forward.md
-- section 7).
-- Reply shape note: inside Lua, XINFO CONSUMERS returns each consumer as a
-- flat array of alternating field name and value (NOT the dict shape
-- redis-py hands back in Python once outside Lua), and Redis 6.2 omits the
-- 'inactive' field entirely (7.2+ only), so fields are scanned by name
-- rather than by position, and 'inactive' is never read.
-- A missing stream or consumer group must not raise: the XINFO call is
-- wrapped in pcall and treated as "nothing to clean" on failure. Any other
-- XINFO failure (WRONGTYPE, OOM, ...) is not raised either, but is reported
-- back distinctly so the Python side can log it instead of the failure
-- going completely silent (verified error text below against redis:7-alpine,
-- valkey/valkey:latest, and redis:6.2-alpine: a missing key replies
-- "ERR no such key", a missing group replies "NOGROUP ...", every other
-- failure is something else).
-- KEYS: [1] = stream:{queue}:{level} (passed with global_keyprefix applied)
-- ARGV: [1] = consumer group name
--       [2] = this channel's own consumer name (never deleted)
--       [3] = idle threshold in ms
-- Returns: array of consumer names deleted, on the expected no-op path, or
--   on any other XINFO failure: a two-element array {-1, error message}.
--   -1 is a Lua number, so it decodes as a RESP integer; a deleted
--   consumer name always decodes as a bulk string, so the two replies
--   cannot be confused by position.

local stream_key = KEYS[1]
local group = ARGV[1]
local own_consumer = ARGV[2]
local idle_threshold_ms = tonumber(ARGV[3])

local ok, consumers = pcall(redis.call, 'XINFO', 'CONSUMERS', stream_key, group)
if not ok then
    local message = type(consumers) == 'string' and consumers or tostring(consumers)
    if message:find('no such key', 1, true) or message:find('NOGROUP', 1, true) then
        return {}
    end
    return {-1, message}
end

local deleted = {}

for i = 1, #consumers do
    local fields = consumers[i]
    local name = nil
    local pending = nil
    local idle = nil
    for j = 1, #fields, 2 do
        local field = fields[j]
        if field == 'name' then
            name = fields[j + 1]
        elseif field == 'pending' then
            pending = tonumber(fields[j + 1])
        elseif field == 'idle' then
            idle = tonumber(fields[j + 1])
        end
    end
    if name ~= nil and name ~= own_consumer and pending == 0 and idle ~= nil and idle > idle_threshold_ms then
        redis.call('XGROUP', 'DELCONSUMER', stream_key, group, name)
        deleted[#deleted + 1] = name
    end
end

return deleted
