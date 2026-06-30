"""
FileEngine Python Client

A Python adapter for the FileEngine gRPC service that provides a familiar
filesystem-like interface.

On failure, client methods raise a typed exception derived from
:class:`FileEngineError` (see ``exceptions``) rather than returning a falsy
value. In particular, write operations raise :class:`WriteUnavailableError`
while the server is temporarily read-only during a primary-database failover —
a transient condition the caller may retry.
"""

from .client import (
    ManagedFiles, FileType, FileInfo, DirectoryEntry, Revision, StorageUsage,
    ROOT_UID, ZERO_UID,
)
from .exceptions import (
    FileEngineError, FileSystemError,
    ServerUnreachableError, ServiceUnavailableError, WriteUnavailableError,
    AuthenticationError, PermissionDeniedError,
    NotFoundError, AlreadyExistsError, InvalidRequestError, OperationError,
)

__version__ = "1.1.0"
__all__ = [
    "ManagedFiles", "FileType", "FileInfo", "DirectoryEntry", "Revision",
    "StorageUsage", "ROOT_UID", "ZERO_UID",
    # exceptions
    "FileEngineError", "FileSystemError",
    "ServerUnreachableError", "ServiceUnavailableError", "WriteUnavailableError",
    "AuthenticationError", "PermissionDeniedError",
    "NotFoundError", "AlreadyExistsError", "InvalidRequestError", "OperationError",
]
