# Python API

```python
from memd import Memory, Kind, Source, Scope, MemoryRecord
from memd.sdk import HostedMemory, HostedError
```

`Memory` is the engine. `Memory(path)` runs it embedded, on a directory or
on an `s3://` root. `Memory(api_key=..., base_url=...)` is the same facade
over the REST API (it sends its calls to `HostedMemory`). Thus, code can move
between the two without changes.

Every method accepts `namespace=`, to operate on a namespace that is not the
default namespace of the facade. `Memory(path, read_only=True)` opens every
namespace as a read replica that follows its writer (another process).
`search` and `get` accept `consistency="eventual"` and `max_staleness_ms`.

The docs build generates this page from the docstrings in `src/memd`.

## Memory

::: memd.Memory
    options:
      members:
        - __init__
        - add
        - add_events
        - remember
        - observe
        - search
        - pack
        - get
        - close_session
        - delete
        - delete_many
        - find_ids
        - forget
        - export_jsonl
        - export_stream
        - compact
        - stats
        - status
        - reembed
        - destroy_namespace
        - flush
        - close

## Search results

::: memd.engine.memory.SearchResult

::: memd.engine.memory.SearchHit

::: memd.engine.memory.forget_fingerprint

## Records

::: memd.MemoryRecord
    options:
      members: false

::: memd.Scope
    options:
      members:
        - contains

::: memd.Kind

::: memd.Source
    options:
      members: false

## Hosted client

::: memd.sdk.HostedMemory

::: memd.sdk.HostedError

## Errors

::: memd.storage.engine.NamespaceBusyError

::: memd.storage.crypto.KeyCustodyError

::: memd.storage.objectstore.ReadOnlyError

::: memd.storage.replica.ReplicaUnavailableError

::: memd.engine.memory.ForgetPreviewMismatch
