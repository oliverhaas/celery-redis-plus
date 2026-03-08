-- Lua script for atomically acknowledging/removing a message.
-- Removes the delivery tag from the messages_index sorted set and
-- deletes the per-message hash in a single atomic operation.
-- This prevents orphaned message hashes from connection drops mid-pipeline.
-- KEYS: [1] = messages_index:{queue} (with global_keyprefix applied)
--       [2] = message:{tag} (with global_keyprefix applied)
-- ARGV: [1] = delivery_tag (the member to ZREM from index)

redis.call('ZREM', KEYS[1], ARGV[1])
redis.call('DEL', KEYS[2])
return 1
