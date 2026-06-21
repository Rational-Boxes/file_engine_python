# FileEngine Python Client — Developer Guide

A Python client for the FileEngine gRPC service (`fileengine_rpc` protocol,
defined in `file_engine_core/proto/fileservice.proto`). It exposes a
filesystem-like API with UUID-identified entities and timestamp-string
versioning.

The JS/TS client (`../javascript_interface`) exposes the **same operation set**
with equivalent behaviour; method names differ only by language convention
(`snake_case` here, `camelCase` there).

---

## Installation

```bash
pip install -r requirements.txt      # grpcio, grpcio-tools, protobuf, pydantic
```

Regenerate the gRPC stubs only if the proto changes:

```bash
cd fileengine
python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. fileservice.proto
# then fix the stub import to be package-relative:
#   from . import fileservice_pb2 as fileservice__pb2
```

---

## Connecting & authentication

Authentication is a **trusted-upstream** model: the `user`, `roles`, `tenant`,
and `claims` you pass are sent verbatim in every request. The server enforces
ACLs against them — the client does no local permission evaluation.

```python
from fileengine import ManagedFiles

mf = ManagedFiles(
    user_name="alice",
    user_roles=["system_admin"],     # see "Administration" below
    user_claims=["read", "write"],   # list[str] | list[(k, v)] | list[dict]
    server_address="localhost:50051",
    tenant="default",                # "" maps to the default tenant
)
# ... use mf ...
mf.close()                            # or: with ManagedFiles(...) as mf: ...
```

### Administration is role-based

Privileged operations — **creating directly under the filesystem root** and all
**role/ACL administration** — require the `system_admin` *role*. There is no
special "root" user; grant `system_admin` in `user_roles` (or per call via the
`roles=` argument).

### The filesystem root

The root may be referenced as the empty string `""` **or** the all-zeros UUID
`00000000-0000-0000-0000-000000000000` (exported as `ROOT_UID` / `ZERO_UID`).

---

## Quickstart

```python
from fileengine import ManagedFiles

with ManagedFiles(user_name="alice", user_roles=["system_admin"]) as mf:
    workspace = mf.mkdir("", "project")          # create under root (needs system_admin)
    doc = mf.touch(workspace, "notes.txt")
    mf.put(doc, b"hello world")
    print(mf.get(doc).getvalue())                # b"hello world"

    for entry in mf.dir(workspace):              # List[DirectoryEntry]
        print(entry.name, entry.is_container, entry.size)
```

Error convention: mutating calls return `False` on failure; getters return
`None` / `[]` / `{}`. RPC/transport errors are caught and surfaced the same way
(no exceptions escape for normal "operation failed" cases).

---

## Data models (Pydantic)

`FileInfo`, `DirectoryEntry`, `Revision`, and `StorageUsage` are
`pydantic.BaseModel` subclasses (strongly typed, validated).

| Model | Key fields |
|-------|-----------|
| `FileInfo` | `uid, name, parent_uid, type, size, owner, permissions, created_at, modified_at, version`; property `is_dir` |
| `DirectoryEntry` | `uid, name, type, size, created_at, modified_at, version_count`; property `is_container` |
| `Revision` | `version` (timestamp string), `name`, `user` |
| `StorageUsage` | `total_space, used_space, available_space, usage_percentage` |

`FileType` exposes `REGULAR_FILE`, `DIRECTORY`, `SYMLINK`.

---

## API reference

All methods accept optional per-call `user=`, `tenant=`, `roles=`, `claims=`
overrides (defaulting to the instance's values).

### Filesystem
| Method | Returns | Notes |
|--------|---------|-------|
| `mkdir(parent_uuid, name)` | `uid \| False` | root parent needs `system_admin` |
| `touch(parent_uuid, name)` | `uid \| False` | empty file |
| `put(uid, payload)` | `float \| False` | bytes/str; writes a new version |
| `get(uid, back=0)` | `BytesIO \| False` | `back` = versions back (0 = latest) |
| `dir(uid, show_deleted=False)` | `List[DirectoryEntry] \| False` | `show_deleted` → `ListDirectoryWithDeleted` |
| `list_deleted(uid)` | — | convenience for `dir(uid, show_deleted=True)` |
| `entity_exists(uid)` | `bool` | |
| `stat(uid)` | `FileInfo \| None` | |
| `is_dir(uid)` | `bool` | |
| `get_parent(uid)` | `str` | parent UID ("" for root) |
| `file_name(uid)` | `[name] \| []` | |
| `get_file_mtime(uid)` / `get_folder_cdate(uid)` | `datetime \| None` | |
| `rename(uid, new_name)` | `bool` | |
| `move(source_uid, destination_uid, new_name=None)` | `bool` | dest = new parent |
| `copy(source_uid, destination_uid)` | `bool` | recursive for dirs |
| `remove(uid)` | `bool` | soft delete (dir or file) |
| `undelete_file(uid)` | `bool` | restore a soft-deleted file |

> Copying or moving a directory into itself or its own subtree is rejected by
> the server (returns `False`) — it does not crash or recurse.

### Versioning
| Method | Returns |
|--------|---------|
| `revisions(uid)` | `List[Revision]` (newest first) |
| `restore_to_version(uid, version_timestamp)` | `restored_version \| False` |
| `purge_old_versions(uid, keep_count)` | `bool` (keeps the N most recent) |

### Metadata
`set_metadata_value(uid, key, value)`, `get_metadata_value(uid, key)`,
`get_metadata_values(uid)` → `dict`, `delete_metadata_value(uid, key)`,
`get_metadata_for_version(uid, version, key)`,
`get_all_metadata_for_version(uid, version)` → `dict`. Use `version="current"`
for the live version's metadata.

### Permissions / ACL
| Method | Returns | Notes |
|--------|---------|-------|
| `check_permission(resource_uid, permission, ...)` | `bool` | evaluates the **acting** identity (`user`/`roles`) |
| `grant_permission(resource_uid, principal, permission, effect="allow")` | `bool` | needs `MANAGE_ACL` (or `system_admin`) |
| `revoke_permission(resource_uid, principal, permission, effect="allow")` | `bool` | |

`permission` accepts a `fileservice_pb2.Permission` int, an enum name
(`"READ"`), or a single letter (`r w x d l u v b s m i`). Prefix a `principal`
with `role:` to target a role. `effect` is `"allow"` (default) or `"deny"`; a
matching DENY always wins over an ALLOW.

### Roles
`create_role(role)`, `delete_role(role)`,
`assign_user_to_role(target_user, role)`,
`remove_user_from_role(target_user, role)`,
`get_roles_for_user(target_user)` → `list`,
`get_users_for_role(role)` → `list`, `get_all_roles()` → `list`.
All require `system_admin`.

### Administrative
`get_storage_usage()` → `StorageUsage \| None`, `trigger_sync()` → `bool`.

---

## Testing

A live server is required for the integration suite (offline unit tests run
regardless and skip the integration class if no server is reachable):

```bash
python -m pytest tests/ -v
# point at a non-default server:
FILEENGINE_SERVER=host:50051 python -m pytest tests/test_integration_full.py -v
```

- `tests/test_unit_models.py` — offline: Pydantic models, auth-context
  conversion, permission/effect coercion.
- `tests/test_integration_full.py` — full coverage against a running server,
  mirroring the C++ CLI suite and the JS `test_client.js`.
