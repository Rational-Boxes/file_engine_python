# FileEngine Python Client

> ⚠️ **Active development — not production-ready.** This project is under active development and should **not** be considered safe for mission-critical use.

A Python client library for the FileEngine gRPC service that provides a familiar filesystem-like interface.

## Installation

```bash
pip install -r requirements.txt
```

## Usage

```python
from fileengine import ManagedFiles

# Connect to the FileEngine gRPC service
mf = ManagedFiles(
    user_name="alice",
    user_roles=["admin", "user"],
    user_claims=["read", "write"],
    server_address="localhost:50051",
    tenant="default"
)

# Create a directory
dir_uid = mf.mkdir("", "my_directory")

# Create a file
file_uid = mf.touch(dir_uid, "my_file.txt")

# Write content to the file
mf.put(file_uid, b"Hello, World!")

# Read content from the file
content = mf.get(file_uid)
print(content.getvalue())  # b"Hello, World!"

# Close the connection
mf.close()
```

## Error handling

Methods **raise a typed exception on failure** rather than returning a falsy
value. Every exception derives from `FileEngineError` and carries structured
context (`operation`, `uid`, `status_code`, `server_error`, `transient`):

| Exception | Raised when |
|-----------|-------------|
| `ServerUnreachableError` | the server can't be reached / timed out *(transient)* |
| `ServiceUnavailableError` | the server is up but can't serve the request *(transient)* |
| `WriteUnavailableError` | a **write** was rejected because the server is temporarily read-only during a primary-database failover *(transient)* — retry once the primary recovers |
| `AuthenticationError` | the identity could not be authenticated |
| `PermissionDeniedError` | authenticated but not authorized |
| `NotFoundError` | the entity / version / metadata key does not exist |
| `AlreadyExistsError` | the target already exists |
| `InvalidRequestError` | the request was rejected as invalid |
| `OperationError` | any other server-reported failure |

`WriteUnavailableError` ⊂ `ServiceUnavailableError` ⊂ `FileEngineError`. Use the
`transient` flag to decide whether to retry:

```python
from fileengine import WriteUnavailableError, NotFoundError

try:
    mf.put(file_uid, b"new content")
except WriteUnavailableError as e:
    assert e.transient            # primary-DB failover — safe to retry later
    schedule_retry(file_uid)
except NotFoundError:
    ...                           # the file is gone

# Existence/permission predicates still answer without raising:
if mf.entity_exists(file_uid):    # False (not an exception) when absent
    ...
```

> Note: `FileSystemError` remains as a backwards-compatible alias for the base
> `FileEngineError`.

## Features

- UUID-based file identification for distributed handling
- Automatic versioning with timestamp-based versions
- POSIX-compliant ACLs for granular access control
- Support for multitenancy
- Streaming support for large files
- Metadata operations
- Permission management

## API Compatibility

This client maintains compatibility with the original ManagedFiles class while adding support for:
- gRPC service connection parameters
- User roles and claims for permissions
- Tenant isolation
- Enhanced authentication context

## License

Copyright (C) 2026 James Hickman <james@rationalboxes.com>

This library is licensed under the **GNU Lesser General Public License, version 3
(or later)** — see the [LICENSE](LICENSE) file. The LGPL builds on the GPL, included
as [LICENSE.GPL-3.0](LICENSE.GPL-3.0).
