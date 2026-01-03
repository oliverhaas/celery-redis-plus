# Coverage Gaps in transport.py

Current coverage: 84% (839 statements, 104 missing, 258 branches, 40 partial)

## Analysis of Missing Lines

### Line 167: `_after_fork_cleanup_channel`
```python
def _after_fork_cleanup_channel(channel: Channel) -> None:
    channel._after_fork()
```
**Why uncovered**: Only called after `os.fork()`. Would need multiprocessing test.
**Difficulty**: Medium - requires process forking test

---

### Lines 264-268: `GlobalKeyPrefixMixin.parse_response` BZMPOP path
```python
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

### Lines 1005, 1015: `_poll_error` and synchronous `_get` edge cases
**Why uncovered**: Sync `_get` edge cases not used when async polling is available.
**Difficulty**: Easy - call `_get` directly

---

### Lines 1132-1136: `close` fanout auto-delete cleanup
**Why uncovered**: No auto-delete fanout queues in tests.
**Difficulty**: Easy - create auto-delete fanout queue

---

### Lines 1180-1188: `_connparams` health_check_interval removal
**Why uncovered**: Connection class always supports health_check_interval.
**Difficulty**: Medium - mock connection class without health_check_interval

---

### Lines 1201-1202: `_connparams` SSL config edge cases
**Why uncovered**: Some SSL config paths not tested.
**Difficulty**: Easy - test with additional SSL transport options

---

### Lines 1206-1221: `_connparams` Unix socket
**Why uncovered**: Unix socket connections not tested.
**Difficulty**: Medium - would need unix socket Redis

---

### Lines 1253: `_get_client` version check
**Why uncovered**: Redis version always >= 3.2.0
**Difficulty**: Impossible without old redis

---

### Lines 1328-1341, 1352-1353: Transport init branches
**Analysis**:
- Line 1328: `if redis:` - always true since redis is imported
- Line 1333: `if redis is None:` - never true since we import redis
- Line 1338: `if self.polling_interval is not None:` - polling_interval is None by default
- Lines 1352-1353: Event loop disconnect callback - async only

**Difficulty**:
- 1338: Easy - set polling_interval
- 1352-1353: Hard - async event loop

---

## Priority Order for Testing

### Easy (unit tests, no special setup):
1. Lines 715-717: String fanout_prefix
2. Lines 1005, 1015: Call `_get` directly with edge cases
3. Lines 1132-1136: Create auto-delete fanout queue
4. Lines 1201-1202: Test additional SSL config paths
5. Line 1338: Set polling_interval

### Medium (requires mocking or special config):
1. Lines 722-724: Mock Redis ping failure
2. Lines 854-856: Mock connection errors
3. Lines 264-268: Global keyprefix with BZMPOP (needs actual BZMPOP call)
4. Lines 773, 779-780, 788: Message restoration edge cases
5. Lines 836, 840, 846: _bzmpop_start edge cases

### Hard (async/multiprocessing):
1. Lines 167, 731, 735: Fork handling
2. Lines 579, 586-612: Async event loop
3. Lines 964-999: XREADGROUP message processing
4. Lines 1352-1353: Event loop disconnect
