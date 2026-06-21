"""
FileEngine Python Client

A Python adapter for the FileEngine gRPC service that provides a familiar
filesystem-like interface.
"""

from .client import (
    ManagedFiles, FileType, FileInfo, DirectoryEntry, Revision, StorageUsage,
    FileSystemError, ROOT_UID, ZERO_UID,
)

__version__ = "1.0.0"
__all__ = [
    "ManagedFiles", "FileType", "FileInfo", "DirectoryEntry", "Revision",
    "StorageUsage", "FileSystemError", "ROOT_UID", "ZERO_UID",
]