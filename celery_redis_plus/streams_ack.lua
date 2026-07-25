-- Lua script for atomically acknowledging/removing a stream message.
-- Acks the entry in the consumer group's PEL and deletes it from the
-- stream in a single atomic operation, so a connection drop cannot leave
-- an acked-but-undeleted entry (unbounded stream growth) or a
-- deleted-but-still-pending entry.
-- For reject+requeue, ARGV[3] carries the serialized message: a copy is
-- XADDed to the stream tail BEFORE the ack, in the same atomic script.
-- The copy starts with a fresh delivery count, so voluntary requeues
-- never count toward the poison cap (max_restore_count). This closes the
-- re-read race of requeueing via XRANGE + XADD + XACK as separate calls.
-- KEYS: [1] = stream:{queue}:{level} (with global_keyprefix applied)
-- ARGV: [1] = consumer group name
--       [2] = message id to ack and delete
--       [3] = requeue payload JSON ('' = plain ack, no requeue)

if ARGV[3] ~= '' then
    redis.call('XADD', KEYS[1], '*', 'payload', ARGV[3])
end
redis.call('XACK', KEYS[1], ARGV[1], ARGV[2])
redis.call('XDEL', KEYS[1], ARGV[2])
return 1
