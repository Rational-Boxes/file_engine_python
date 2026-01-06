# FileEngine Python Client

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