# Coverage Gaps in transport.py

Current coverage: 74% (839 statements, 179 missing, 258 branches, 44 partial)

## Analysis of Missing Lines

### Line 167: `_after_fork_cleanup_channel`
```python
def _after_fork_cleanup_channel(channel: Channel) -> None:
    channel._after_fork()
```
**Why uncovered**: Only called after `os.fork()`. Would need multiprocessing test.
**Difficulty**: Medium - requires process forking test

---

### Lines 261-269, 272: `GlobalKeyPrefixMixin.parse_response` BZMPOP path
```python
ret = super().parse_response(connection, command_name, **options)
if command_name == "BZMPOP" and ret:
    key, members = ret
    if isinstance(key, bytes):
        key = key.decode()
    key = key[len(self.global_keyprefix):]
    return key, members
```
**Why uncovered**: Requires `global_keyprefix` to be set AND BZMPOP to return results through the prefixed client.
**Difficulty**: Medium - need integration test with global_keyprefix config

---

### Lines 416-435: `QoS.restore_visible` message restoration loop
**Why uncovered**: Only runs when messages are in the index but not acked (visibility timeout expired).
**Difficulty**: Hard - requires simulating message consumption without ack, then waiting for visibility timeout

---

### Lines 440-448: `QoS.restore_by_tag`
**Why uncovered**: Called by restore_visible when restoring unacked messages.
**Difficulty**: Hard - same as above

---

### Lines 562, 567-569: `MultiChannelPoller.maybe_restore_messages` and `maybe_update_messages_index`
**Why uncovered**: Called from event loop, but tests use synchronous operations.
**Difficulty**: Hard - async event loop testing

---

### Lines 579, 586-612: `MultiChannelPoller.handle_event` ERR path and `get` method
**Why uncovered**: The `get` method is the async polling loop. Tests use sync operations.
**Difficulty**: Hard - would need to test async event loop directly

---

### Lines 715-717: Channel `__init__` fanout prefix string path
```python
if isinstance(self.fanout_prefix, str):
    self.keyprefix_fanout = self.fanout_prefix
```
**Why uncovered**: `fanout_prefix` is typically `True` or `False`, not a string.
**Difficulty**: Easy - test with string fanout_prefix

---

### Lines 722-724: Channel `__init__` connection failure
```python
except Exception:
    self._disconnect_pools()
    raise
```
**Why uncovered**: Would need Redis to fail on ping during channel creation.
**Difficulty**: Medium - mock Redis to fail on ping

---

### Lines 731, 735: Fork registration and `_after_fork`
**Why uncovered**: Fork handling only runs in multiprocessing scenarios.
**Difficulty**: Medium - requires forking test

---

### Lines 746, 751, 753: Pool disconnect and poll state reset
**Why uncovered**: Part of connection cleanup that doesn't run in normal tests.
**Difficulty**: Medium

---

### Lines 766-780, 783-793, 796: `_do_restore_message`, `_restore`, `_restore_at_beginning`
**Why uncovered**: Message restoration on nack/reject. Only runs when messages are rejected.
**Difficulty**: Medium - test with task that raises exception and requeue=True

---

### Lines 811, 813, 818-819: `basic_cancel` protected read path
**Why uncovered**: Protected read state only active during async polling.
**Difficulty**: Hard - async testing

---

### Lines 836, 840, 846: `_bzmpop_start` edge cases
**Why uncovered**: Line 840 (no queues) and 846 (global_keyprefix) not hit.
**Difficulty**: Easy to Medium

---

### Lines 854-856, 868: `_bzmpop_read` connection error and empty payload
**Why uncovered**: Connection errors and missing payloads rare in tests.
**Difficulty**: Medium - mock connection errors

---

### Lines 899, 904, 908, 915, 924, 948, 958-959, 964-999: XREADGROUP fanout paths
**Why uncovered**: Fanout stream reading not exercised in integration tests.
**Difficulty**: Medium - need fanout consumer tests

---

### Lines 1005, 1009-1018: `_poll_error` and synchronous `_get`
**Why uncovered**: Sync `_get` not used when async polling is available.
**Difficulty**: Easy - call `_get` directly

---

### Lines 1050-1056: `_put_fanout`
**Why uncovered**: Publishing to fanout exchanges not tested.
**Difficulty**: Easy - test fanout publish

---

### Lines 1107: `get_table` empty return
**Why uncovered**: Always have bindings in tests.
**Difficulty**: Easy - query empty exchange

---

### Lines 1132-1136: `close` fanout auto-delete cleanup
**Why uncovered**: No auto-delete fanout queues in tests.
**Difficulty**: Easy - create auto-delete fanout queue

---

### Lines 1149-1158: `_prepare_virtual_host` edge cases
**Why uncovered**: Standard vhost paths not exercised.
**Difficulty**: Easy - test with "/" and "/0" vhosts

---

### Lines 1180-1188: `_connparams` health_check_interval removal
**Why uncovered**: Connection class always supports health_check_interval.
**Difficulty**: Medium - mock connection class without health_check_interval

---

### Lines 1192-1202: `_connparams` SSL config
**Why uncovered**: SSL not tested.
**Difficulty**: Easy - test with SSL transport options

---

### Lines 1206-1221: `_connparams` Unix socket
**Why uncovered**: Unix socket connections not tested.
**Difficulty**: Medium - would need unix socket Redis

---

### Lines 1253, 1258: `_get_client` version check and prefix client
**Why uncovered**:
- Line 1253: Redis version always >= 3.2.0
- Line 1258: `global_keyprefix` not set in most tests
**Difficulty**: Easy for 1258 (test with prefix), impossible for 1253 without old redis

---

### Lines 1328-1341, 1352-1353: Transport init branches
**Analysis**: You're right! These are mutually exclusive:
- Line 1328: `if redis:` - always true since redis is imported
- Line 1333: `if redis is None:` - never true since we import redis
- Line 1338: `if self.polling_interval is not None:` - polling_interval is None by default
- Line 1341: `return redis.__version__` - `driver_version()` method not called
- Lines 1352-1353: Event loop disconnect callback - async only

The 1328 vs 1333 shows coverage reporting partial branches. Line 1328's `if redis:` branch is always taken, but the implicit `else` (line 1333's path) is never taken - that's expected and correct.

**Difficulty**:
- 1338: Easy - set polling_interval
- 1341: Easy - call driver_version()
- 1352-1353: Hard - async event loop

---

## Priority Order for Testing

### Easy (unit tests, no special setup):
1. Line 1341: Call `driver_version()`
2. Line 1107: Query empty exchange binding table
3. Lines 1009-1018: Call `_get` directly
4. Lines 1050-1056: Test fanout publish
5. Lines 1149-1158: Test vhost parsing edge cases
6. Line 1258: Test with `global_keyprefix`
7. Lines 1192-1202: Test SSL config parsing

### Medium (requires mocking or special config):
1. Lines 715-717: String fanout_prefix
2. Lines 722-724: Mock Redis ping failure
3. Lines 854-856: Mock connection errors
4. Lines 766-796: Message restoration (nack/requeue)
5. Lines 261-269, 272: Global keyprefix with BZMPOP

### Hard (async/multiprocessing):
1. Lines 167, 731, 735: Fork handling
2. Lines 579, 586-612: Async event loop
3. Lines 964-999: XREADGROUP message processing
4. Lines 1352-1353: Event loop disconnect
