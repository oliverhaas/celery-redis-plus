-- Lua script for atomically acknowledging/removing a message.
-- Removes the delivery tag from the messages_index sorted set and the queue
-- sorted set, and deletes the per-message hash, in a single atomic operation.
-- This prevents orphaned message hashes from connection drops mid-pipeline.
--
-- The queue ZREM is not just about keeping ZCARD accurate. Once
-- enqueue_due_messages has restored a message while its original consumer was
-- still working on it, the tag is back in the queue. Without this ZREM the ack
-- cannot undo that restore, the duplicate stays poppable, and a second worker
-- runs the task. An ack that lands before another consumer pops now cancels the
-- restored copy outright.
--
-- KEYS: [1] = messages_index:{queue} (with global_keyprefix applied)
--       [2] = message:{tag} (with global_keyprefix applied)
--       [3] = queue:{queue} (with global_keyprefix applied)
-- ARGV: [1] = delivery_tag (the member to ZREM from index and queue)

redis.call('ZREM', KEYS[1], ARGV[1])
redis.call('ZREM', KEYS[3], ARGV[1])
redis.call('DEL', KEYS[2])
return 1
