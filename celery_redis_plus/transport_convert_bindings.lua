-- Lua script for converting a legacy SET binding table into a sorted set.
--
-- Bindings used to be stored in a plain SET, which is also what kombu's own
-- Redis transport writes, and a set member cannot carry an expiry. They are a
-- sorted set here, scored with the unix time the binding goes stale, so the
-- read path can drop the ones nobody refreshes any more.
--
-- Inherited members are scored +inf: this transport did not write them, so it
-- cannot know when they go stale and must never prune them. A live declarer
-- rescores its own members to a real deadline on its next bind or refresh.
--
-- SMEMBERS, DEL and ZADD have to happen without anything getting in between,
-- or a binding written by a process still running the old code lands in the
-- set after it was read and is dropped by the DEL.
--
-- KEYS: [1] = _kombu.binding.{exchange} (with global_keyprefix applied)
-- Returns the number of members converted, 0 if the key was not a set.

local key = KEYS[1]
if redis.call('TYPE', key)['ok'] ~= 'set' then
    return 0
end

local members = redis.call('SMEMBERS', key)
redis.call('DEL', key)
for i = 1, #members do
    redis.call('ZADD', key, 'inf', members[i])
end
return #members
