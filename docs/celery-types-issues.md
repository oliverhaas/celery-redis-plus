# Issues Found in celery-types Stubs

This document lists issues found in `celery-types==0.24.0` when type-checking code that uses kombu's transport API.

## 1. `kombu.utils.json.loads` - Incorrect Required Parameters

**File:** `kombu-stubs/utils/json.pyi`

**Current stub (incorrect):**
```python
def loads(
    s: str,
    _loads: Callable[[str], Any],
    decode_bytes: bool,
    object_hook: Callable[[dict[Any, Any]], None],
) -> Any: ...
```

**Actual implementation (from kombu 5.5.x):**
```python
def loads(s, _loads=json.loads, decode_bytes=True, object_hook=object_hook):
    """Deserialize json from string."""
    ...
```

**Issue:** All parameters except `s` have default values in the actual implementation, but the stub declares them as required. This causes false positives when calling `loads(some_string)`.

**Fix:**
```python
def loads(
    s: str | bytes | bytearray | memoryview,
    _loads: Callable[[str], Any] = ...,
    decode_bytes: bool = ...,
    object_hook: Callable[[dict[Any, Any]], Any] | None = ...,
) -> Any: ...
```

---

## 2. `kombu.utils.json.dumps` - Incorrect Required Parameters and First Param Name

**File:** `kombu-stubs/utils/json.pyi`

**Current stub (incorrect):**
```python
def dumps(
    s: str,
    _dumps: Callable[..., str],
    cls: JSONEncoder,
    default_kwargs: dict[str, Any],
    **kwargs: Any,
) -> str: ...
```

**Actual implementation:**
```python
def dumps(
    s,
    _dumps=json.dumps,
    cls=JSONEncoder,
    default_kwargs=None,
    **kwargs
):
    """Serialize object to json string."""
    ...
```

**Issues:**
1. First parameter is named `s` but accepts any object to serialize, not just strings - should be something like `obj: Any`
2. All parameters except the first have default values, but stub declares them as required
3. `default_kwargs` can be `None`

**Fix:**
```python
def dumps(
    obj: Any,
    _dumps: Callable[..., str] = ...,
    cls: type[json.JSONEncoder] = ...,
    default_kwargs: dict[str, Any] | None = ...,
    **kwargs: Any,
) -> str: ...
```

---

## 3. `kombu.utils.json.object_hook` - Incorrect Return Type

**File:** `kombu-stubs/utils/json.pyi`

**Current stub (incorrect):**
```python
def object_hook(o: dict[Any, Any]) -> None: ...
```

**Actual implementation:**
```python
def object_hook(o: dict):
    """Hook function to perform custom deserialization."""
    if o.keys() == {"__type__", "__value__"}:
        decoder = _decoders.get(o["__type__"])
        if decoder:
            return decoder(o["__value__"])
        else:
            raise ValueError("Unsupported type", type, o)
    else:
        return o
```

**Issue:** The function returns a value (either the decoded object or the original dict), but the stub says it returns `None`.

**Fix:**
```python
def object_hook(o: dict[Any, Any]) -> Any: ...
```

---

## 4. `kombu.transport.base.Transport` - Missing Class Attributes

**File:** `kombu-stubs/transport/base.pyi`

**Current stub (incomplete):**
```python
class Transport: ...
```

**Actual implementation has these class attributes:**
```python
class Transport:
    ...
    connection_errors = (ConnectionError,)
    channel_errors = (ChannelError,)
    driver_type = 'N/A'
    driver_name = 'N/A'
    implements = default_transport_capabilities.extend()
    ...
```

**Issue:** The stub is essentially empty. Code that accesses `Transport.connection_errors` or `Transport.channel_errors` (which is common when building custom transports) fails type checking.

**Fix:**
```python
from kombu.exceptions import ChannelError, ConnectionError

class Transport:
    connection_errors: tuple[type[Exception], ...]
    channel_errors: tuple[type[Exception], ...]
    driver_type: str
    driver_name: str

    client: Any

    def __init__(self, client: Any, **kwargs: Any) -> None: ...
    def establish_connection(self) -> Any: ...
    def close_connection(self, connection: Any) -> None: ...
    def create_channel(self, connection: Any) -> Any: ...
    def close_channel(self, connection: Any) -> None: ...
    def drain_events(self, connection: Any, **kwargs: Any) -> None: ...
    # ... other methods
```

---

## 5. Missing `kombu.transport.virtual` Module Stubs

**Issue:** There are no stubs for `kombu.transport.virtual` module, which contains `virtual.Transport` and `virtual.Channel` - the base classes used by most non-AMQP transports (Redis, SQS, etc.).

The actual module defines:
- `kombu.transport.virtual.base.Transport` (extends `base.Transport`)
- `kombu.transport.virtual.base.Channel` (extends `base.StdChannel`)
- `kombu.transport.virtual.base.QoS`
- Various exchange types, broker state, etc.

**Impact:** Any code building a custom transport that inherits from `virtual.Transport` cannot be properly type-checked.

**Fix:** Add `kombu-stubs/transport/virtual/__init__.pyi` and `kombu-stubs/transport/virtual/base.pyi` with appropriate stubs.

---

## Summary

| Issue | Severity | Type |
|-------|----------|------|
| `loads()` required params | High | False positives on common usage |
| `dumps()` required params + wrong type | High | False positives on common usage |
| `object_hook()` return type | Low | Incorrect but rarely affects user code |
| `Transport` missing attributes | High | Blocks custom transport development |
| Missing `virtual` module | High | Blocks custom transport development |

## Versions Tested

- `celery-types==0.24.0`
- `kombu==5.5.3`
- Type checker: `ty==0.0.8`
