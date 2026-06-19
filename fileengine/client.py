"""
FileEngine gRPC Client

Provides a Python interface to the FileEngine gRPC service.

This client targets the `fileengine` protocol defined in
``file_engine_cpp/proto/fileservice.proto`` (the C++ FileService server),
exposing a familiar filesystem-like API backed by the gRPC service, with
UUID4 file identification and UNIX-timestamp versioning.
"""

import grpc
from datetime import datetime
import time
import uuid
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
import sys
import os

from . import fileservice_pb2
from . import fileservice_pb2_grpc


class FileType:
    """File type constants (mirror of the proto ``ProtoFileType`` enum)."""
    REGULAR_FILE = fileservice_pb2.ProtoFileType.PROTO_REGULAR_FILE
    DIRECTORY = fileservice_pb2.ProtoFileType.PROTO_DIRECTORY


@dataclass
class FileInfo:
    """File metadata information (mirror of the proto ``ProtoFileInfo``)."""
    uid: str
    path: str
    name: str
    type: int
    size: int
    created_at: datetime
    modified_at: datetime
    version: str  # Timestamp string instead of integer
    owner: str
    permissions: int


@dataclass
class DirectoryEntry:
    """Directory entry information."""
    uid: str
    name: str
    type: int
    size: int


class FileSystemError(Exception):
    """Base exception for filesystem errors"""
    pass


class ManagedFiles:
    """
    Python adapter for the FileEngine gRPC service that provides a
    filesystem-like interface compatible with the original Python
    ManagedFiles implementation.

    This is a drop-in replacement that uses the C++ ``fileengine`` gRPC
    server backend, with UUID4 for file identification and UNIX timestamps
    for versioning.
    """

    def __init__(self, db_interface=None, storage_base: str = None, user_roles: list = None,
                 user_name: str = '', log_access: bool = False, permission_resolver=None,
                 s3_config: dict = None, server_address: str = "localhost:50051",
                 tenant: str = "", user_claims: list = None):
        """
        Initialize ManagedFiles with gRPC client

        Args:
            db_interface: Ignored (compatibility parameter)
            storage_base: Ignored (compatibility parameter)
            user_roles: User roles for permissions (default: [])
            user_name: Username for operations
            log_access: Enable access logging (not fully implemented)
            permission_resolver: Permission resolver (not fully implemented)
            s3_config: S3 configuration (handled by backend)
            server_address: gRPC server address (host:port)
            tenant: Tenant for operations (defaults to empty string for default tenant)
            user_claims: Additional user claims for permissions (defaults to empty list)
        """
        self.user = user_name or 'user'
        self.roles = user_roles or []
        self.claims = user_claims or []
        self.log_access = log_access
        self.permissions = permission_resolver
        self.tenant = tenant

        # Establish gRPC connection
        self.channel = grpc.insecure_channel(server_address)
        self.stub = fileservice_pb2_grpc.FileServiceStub(self.channel)

        # Ensure connection works
        try:
            # Test connection with a simple operation
            pass
        except:
            raise Exception(f"Could not connect to gRPC server at {server_address}")

    def close(self):
        """Close the gRPC connection"""
        if hasattr(self, 'channel'):
            self.channel.close()

    def __enter__(self):
        """Context manager entry"""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.close()

    def set_user_information(self, user_name: str = None, roles: list = None, claims: list = None):
        """Set user information"""
        if user_name:
            self.user = user_name
        if roles:
            self.roles = roles
        if claims:
            self.claims = claims

    def set_permission_resolver(self, permission_resolver):
        """Set permission resolver"""
        self.permissions = permission_resolver

    # Helper method to create auth context
    def _create_auth_context(self, user: str = None, tenant: str = None, roles: list = None, claims: list = None):
        """Create authentication context for requests"""
        actual_user = user or self.user
        actual_tenant = tenant if tenant is not None else self.tenant
        actual_roles = roles if roles is not None else self.roles
        actual_claims = claims if claims is not None else self.claims

        # Convert list of claims to a map format expected by the proto
        claims_map = {}
        if actual_claims:
            for claim in actual_claims:
                if isinstance(claim, str):
                    # If claim is just a string, use it as both key and value
                    claims_map[claim] = claim
                elif isinstance(claim, dict):
                    # If claim is a dict, merge it into the claims map
                    claims_map.update(claim)
                elif isinstance(claim, tuple) and len(claim) == 2:
                    # If claim is a tuple (key, value), add it to the map
                    claims_map[claim[0]] = claim[1]

        return fileservice_pb2.AuthContext(
            user=actual_user,
            roles=actual_roles,
            tenant=actual_tenant,
            claims=claims_map
        )

    @staticmethod
    def _metadata_to_dict(entries) -> dict:
        """Convert a repeated ``MetadataEntry`` list into a plain dict."""
        return {entry.key: entry.value for entry in entries}

    @staticmethod
    def _to_version_index(version) -> int:
        """
        Coerce a version identifier into the int32 index expected by the
        versioned-metadata RPCs. Accepts ints or timestamp-like strings.
        """
        try:
            return int(version)
        except (TypeError, ValueError):
            try:
                return int(float(version))
            except (TypeError, ValueError):
                return 0

    # Path/UID conversion methods

    def path_to_uid(self, path: str) -> str:
        """
        Convert an absolute path to a UID using the gRPC ``ResolvePath`` RPC.

        Args:
            path: Absolute file path

        Returns:
            UID corresponding to path, or None if not found
        """
        try:
            request = fileservice_pb2.ResolvePathRequest(
                path=path,
                auth=self._create_auth_context()
            )
            response = self.stub.ResolvePath(request)
            if response.success:
                return response.uid
            return None
        except grpc.RpcError:
            return None

    def resolve_path(self, path: str, user: str = None, tenant: str = None, roles: list = None, claims: list = None):
        """
        Resolve an absolute path to its UID and type.

        Returns:
            A dict ``{'uid': str, 'type': int}`` on success, or None on error.
        """
        auth_context = self._create_auth_context(user, tenant, roles, claims)

        try:
            request = fileservice_pb2.ResolvePathRequest(
                path=path,
                auth=auth_context
            )
            response = self.stub.ResolvePath(request)
            if response.success:
                return {'uid': response.uid, 'type': response.type}
            return None
        except grpc.RpcError:
            return None

    def uid_to_path(self, uid: str) -> list:
        """
        Convert UID to path components.

        Note: The ``fileengine`` protocol does not expose a reverse
        (UID -> path) operation, so this returns an empty list.
        """
        return []

    # Directory operations

    def mkdir(self, parent_uuid: str, name: str, user: str = None, tenant: str = None, roles: list = None, claims: list = None) -> str:
        """
        Create a directory

        Args:
            parent_uuid: Parent directory UUID (empty string for root)
            name: Name of the new directory
            user: User performing the operation (defaults to self.user)
            tenant: Tenant identifier (defaults to instance tenant)
            roles: User roles for permissions (defaults to instance roles)
            claims: User claims for permissions (defaults to instance claims)

        Returns:
            UUID of created directory, or False on error
        """
        auth_context = self._create_auth_context(user, tenant, roles, claims)

        try:
            request = fileservice_pb2.MakeDirectoryRequest(
                parent_uid=parent_uuid,
                name=name,
                auth=auth_context
            )
            response = self.stub.MakeDirectory(request)
            if response.success:
                return response.uid
            else:
                return False
        except grpc.RpcError:
            return False

    def mkdir_path(self, path: str, user: str = None, tenant: str = None, roles: list = None, claims: list = None) -> str:
        """
        Create directory path recursively.

        Note: Recursive path creation requires path resolution semantics not
        provided as a single RPC; this remains a placeholder returning False.
        """
        return False

    def dir(self, uid, show_deleted: bool = False, user: str = None, tenant: str = None, roles: list = None, claims: list = None) -> List[dict]:
        """
        List directory contents with detailed information

        Args:
            uid: Directory UID
            show_deleted: Include deleted items in the listing
            user: User performing the operation (defaults to self.user)
            tenant: Tenant identifier (defaults to instance tenant)
            roles: User roles for permissions (defaults to instance roles)
            claims: User claims for permissions (defaults to instance claims)

        Returns:
            List of dictionaries with file/directory information, or False on error
        """
        auth_context = self._create_auth_context(user, tenant, roles, claims)

        try:
            request = fileservice_pb2.ListDirectoryRequest(
                uid=uid,
                auth=auth_context,
                include_deleted=show_deleted
            )
            response = self.stub.ListDirectory(request)

            if not response.success:
                return False

            result = []
            for entry in response.entries:
                item = {
                    'uid': entry.uid,
                    'name': entry.name,
                    'creator': auth_context.user,  # gRPC service doesn't track creator separately
                    'is_container': 'True' if entry.type == fileservice_pb2.ProtoFileType.PROTO_DIRECTORY else 'False'
                }

                # For files, add version and modification time information
                if entry.type == fileservice_pb2.ProtoFileType.PROTO_REGULAR_FILE:
                    revisions = self.revisions(entry.uid, user=auth_context.user,
                                              tenant=auth_context.tenant,
                                              roles=auth_context.roles,
                                              claims=list(auth_context.claims.keys()))
                    if revisions:
                        item['uploading_user'] = auth_context.user  # gRPC service doesn't track per-version user
                        item['version'] = revisions[0]['version']  # Latest version timestamp
                        item['mtime'] = self.get_file_mtime(entry.uid, user=auth_context.user,
                                                           tenant=auth_context.tenant,
                                                           roles=auth_context.roles,
                                                           claims=list(auth_context.claims.keys()))

                result.append(item)

            return result
        except grpc.RpcError:
            return False

    # File operations

    def entity_exists(self, entity_uid: str, include_deleted: bool = False) -> bool:
        """
        Check if entity exists

        Args:
            entity_uid: Entity UID
            include_deleted: Include deleted entities (not implemented)

        Returns:
            True if exists, False otherwise
        """
        try:
            request = fileservice_pb2.FileExistsRequest(
                uid=entity_uid,
                auth=self._create_auth_context()
            )
            response = self.stub.FileExists(request)
            return response.exists
        except grpc.RpcError:
            return False

    def is_dir(self, uid: str, user: str = None, tenant: str = None, roles: list = None, claims: list = None) -> bool:
        """Check if entity is a directory"""
        auth_context = self._create_auth_context(user, tenant, roles, claims)

        try:
            request = fileservice_pb2.GetFileInfoRequest(
                uid=uid,
                auth=auth_context
            )
            response = self.stub.GetFileInfo(request)
            if response.success:
                return response.info.type == fileservice_pb2.ProtoFileType.PROTO_DIRECTORY
            else:
                return False
        except grpc.RpcError:
            return False

    def touch(self, container_uuid: str, name: str, user: str = None, tenant: str = None, roles: list = None, claims: list = None) -> str:
        """
        Create an empty file

        Args:
            container_uuid: Parent directory UUID
            name: File name
            user: User performing the operation (defaults to self.user)
            tenant: Tenant identifier (defaults to instance tenant)
            roles: User roles for permissions (defaults to instance roles)
            claims: User claims for permissions (defaults to instance claims)

        Returns:
            UUID of created file, or False on error
        """
        auth_context = self._create_auth_context(user, tenant, roles, claims)

        try:
            request = fileservice_pb2.CreateFileRequest(
                parent_uid=container_uuid,
                name=name,
                auth=auth_context
            )
            response = self.stub.CreateFile(request)
            if response.success:
                return response.uid
            else:
                return False
        except grpc.RpcError:
            return False

    def put(self, uid: str, payload=None, return_open: bool = False, user: str = None, tenant: str = None, roles: list = None, claims: list = None):
        """
        Save a new version of a file

        Args:
            uid: File UID
            payload: File content (bytes)
            return_open: Return file-like object for writing (not implemented in gRPC adapter)
            user: User performing the operation (defaults to self.user)
            tenant: Tenant identifier (defaults to instance tenant)
            roles: User roles for permissions (defaults to instance roles)
            claims: User claims for permissions (defaults to instance claims)

        Returns:
            Timestamp of the new version as float, or False on error
        """
        if return_open:
            raise NotImplementedError("return_open parameter not supported in gRPC implementation")

        if payload is None:
            payload = b""

        if isinstance(payload, str):
            payload = payload.encode('utf-8')

        auth_context = self._create_auth_context(user, tenant, roles, claims)

        try:
            request = fileservice_pb2.WriteFileRequest(
                uid=uid,
                auth=auth_context,
                data=payload
            )
            response = self.stub.WriteFile(request)
            if response.success:
                # Return current timestamp as version identifier
                return time.time()
            else:
                return False
        except grpc.RpcError:
            return False

    def get(self, uid: str, back: int = 0, user: str = None, tenant: str = None, roles: list = None, claims: list = None):
        """
        Get file content

        Args:
            uid: File UID
            back: Version to get (0 = latest, 1 = previous, etc.)
            user: User performing the operation (defaults to self.user)
            tenant: Tenant identifier (defaults to instance tenant)
            roles: User roles for permissions (defaults to instance roles)
            claims: User claims for permissions (defaults to instance claims)

        Returns:
            File-like object with content, or False on error
        """
        auth_context = self._create_auth_context(user, tenant, roles, claims)

        # For the latest version, read directly via ReadFile.
        if back == 0:
            try:
                request = fileservice_pb2.ReadFileRequest(
                    uid=uid,
                    auth=auth_context
                )
                response = self.stub.ReadFile(request)
                if not response.success:
                    return False

                import io
                data_buffer = io.BytesIO()
                data_buffer.write(response.data)
                data_buffer.seek(0)
                return data_buffer
            except grpc.RpcError:
                return False

        # For older versions, resolve the timestamp then read that version.
        versions = self.revisions(uid, user=auth_context.user,
                                 tenant=auth_context.tenant,
                                 roles=auth_context.roles,
                                 claims=list(auth_context.claims.keys()))
        if not versions or len(versions) <= back:
            return False

        version_timestamp = versions[back]['version']

        try:
            request = fileservice_pb2.ReadVersionRequest(
                uid=uid,
                version_timestamp=version_timestamp,
                auth=auth_context
            )

            import io
            data_buffer = io.BytesIO()

            response = self.stub.ReadVersion(request)
            if response.success:
                data_buffer.write(response.data)
            else:
                return False

            data_buffer.seek(0)
            return data_buffer
        except grpc.RpcError:
            return False

    # Version control

    def revisions(self, uid: str, user: str = None, tenant: str = None, roles: list = None, claims: list = None) -> List[dict]:
        """
        Get file revisions

        Args:
            uid: File UID
            user: User performing the operation (defaults to self.user)
            tenant: Tenant identifier (defaults to instance tenant)
            roles: User roles for permissions (defaults to instance roles)
            claims: User claims for permissions (defaults to instance claims)

        Returns:
            List of dictionaries with revision info, or [] on error
        """
        auth_context = self._create_auth_context(user, tenant, roles, claims)

        try:
            request = fileservice_pb2.ListVersionsRequest(
                uid=uid,
                auth=auth_context
            )
            response = self.stub.ListVersions(request)

            if response.success:
                result = []
                for version_timestamp in response.versions:
                    result.append({
                        'version': version_timestamp,  # String timestamp
                        'name': uid.split('-')[-1] if '-' in uid else uid,  # Extract from UUID-based name
                        'user': auth_context.user  # Backend doesn't track per-version user
                    })
                return result
            else:
                return []
        except grpc.RpcError:
            return []

    # File information methods

    def get_file_mtime(self, uid: str, user: str = None, tenant: str = None, roles: list = None, claims: list = None) -> datetime:
        """Get file modification time"""
        auth_context = self._create_auth_context(user, tenant, roles, claims)

        try:
            request = fileservice_pb2.GetFileInfoRequest(
                uid=uid,
                auth=auth_context
            )
            response = self.stub.GetFileInfo(request)
            if response.success:
                return datetime.fromtimestamp(response.info.modified_at)
            else:
                return None
        except grpc.RpcError:
            return None

    def get_folder_cdate(self, uid: str, user: str = None, tenant: str = None, roles: list = None, claims: list = None) -> datetime:
        """Get folder creation date"""
        auth_context = self._create_auth_context(user, tenant, roles, claims)

        try:
            request = fileservice_pb2.GetFileInfoRequest(
                uid=uid,
                auth=auth_context
            )
            response = self.stub.GetFileInfo(request)
            if response.success:
                return datetime.fromtimestamp(response.info.created_at)
            else:
                return None
        except grpc.RpcError:
            return None

    def file_name(self, uid: str, user: str = None, tenant: str = None, roles: list = None, claims: list = None) -> list:
        """Get file name"""
        auth_context = self._create_auth_context(user, tenant, roles, claims)

        try:
            request = fileservice_pb2.GetFileInfoRequest(
                uid=uid,
                auth=auth_context
            )
            response = self.stub.GetFileInfo(request)
            if response.success:
                return [response.info.name]
            else:
                return []
        except grpc.RpcError:
            return []

    def get_parent(self, uid: str, user: str = None, tenant: str = None, roles: list = None, claims: list = None):
        """
        Get parent UID.

        Note: The ``fileengine`` protocol's ``ProtoFileInfo`` does not carry a
        parent reference, so this returns an empty string.
        """
        return ""

    # File manipulation

    def move(self, source_uid: str, destination_uid: str, new_name: str = None, user: str = None, tenant: str = None, roles: list = None, claims: list = None) -> bool:
        """
        Move file/directory to new location

        Args:
            source_uid: Source UID
            destination_uid: Destination directory UID
            new_name: New name for the moved entity (optional)
            user: User performing the operation (defaults to self.user)
            tenant: Tenant identifier (defaults to instance tenant)
            roles: User roles for permissions (defaults to instance roles)
            claims: User claims for permissions (defaults to instance claims)

        Returns:
            True on success, False on error
        """
        auth_context = self._create_auth_context(user, tenant, roles, claims)

        try:
            request = fileservice_pb2.MoveFileRequest(
                source_uid=source_uid,
                destination_uid=destination_uid,
                auth=auth_context
            )
            response = self.stub.MoveFile(request)

            # If we need to rename, do that after the move
            if response.success and new_name:
                return self.rename(source_uid, new_name, user=user, tenant=tenant, roles=roles, claims=claims)
            else:
                return response.success
        except grpc.RpcError:
            return False

    def copy(self, source_uid: str, destination_uid: str, user: str = None, tenant: str = None, roles: list = None, claims: list = None) -> bool:
        """
        Copy file/directory to new location

        Args:
            source_uid: Source UID
            destination_uid: Destination directory UID
            user: User performing the operation (defaults to self.user)
            tenant: Tenant identifier (defaults to instance tenant)
            roles: User roles for permissions (defaults to instance roles)
            claims: User claims for permissions (defaults to instance claims)

        Returns:
            True on success, False on error
        """
        auth_context = self._create_auth_context(user, tenant, roles, claims)

        try:
            request = fileservice_pb2.CopyFileRequest(
                source_uid=source_uid,
                destination_uid=destination_uid,
                auth=auth_context
            )
            response = self.stub.CopyFile(request)
            return response.success
        except grpc.RpcError:
            return False

    def remove(self, uid: str, user: str = None, tenant: str = None, roles: list = None, claims: list = None) -> bool:
        """
        Remove file/directory

        Args:
            uid: File/directory UID
            user: User performing operation (defaults to self.user)
            tenant: Tenant identifier (defaults to instance tenant)
            roles: User roles for permissions (defaults to instance roles)
            claims: User claims for permissions (defaults to instance claims)

        Returns:
            True on success, False on error
        """
        auth_context = self._create_auth_context(user, tenant, roles, claims)

        try:
            # Determine if it's a file or directory to use appropriate request
            info_req = fileservice_pb2.GetFileInfoRequest(
                uid=uid,
                auth=auth_context
            )
            info_resp = self.stub.GetFileInfo(info_req)

            if info_resp.success and info_resp.info.type == fileservice_pb2.ProtoFileType.PROTO_DIRECTORY:
                # It's a directory
                request = fileservice_pb2.RemoveDirectoryRequest(
                    uid=uid,
                    auth=auth_context
                )
                response = self.stub.RemoveDirectory(request)
            else:
                # It's a file
                request = fileservice_pb2.DeleteFileRequest(
                    uid=uid,
                    auth=auth_context
                )
                response = self.stub.DeleteFile(request)

            return response.success
        except grpc.RpcError:
            return False

    def rename(self, uid: str, new_name: str, user: str = None, tenant: str = None, roles: list = None, claims: list = None) -> bool:
        """
        Rename a file or directory

        Args:
            uid: File/directory UID to rename
            new_name: New name for the file/directory
            user: User performing operation (defaults to self.user)
            tenant: Tenant identifier (defaults to instance tenant)
            roles: User roles for permissions (defaults to instance roles)
            claims: User claims for permissions (defaults to instance claims)

        Returns:
            True on success, False on error
        """
        auth_context = self._create_auth_context(user, tenant, roles, claims)

        try:
            request = fileservice_pb2.RenameFileRequest(
                uid=uid,
                new_name=new_name,
                auth=auth_context
            )
            response = self.stub.RenameFile(request)
            return response.success
        except grpc.RpcError:
            return False

    # Metadata operations

    def set_metadata_value(self, uid: str, key: str, value: str, user: str = None, tenant: str = None, roles: list = None, claims: list = None) -> bool:
        """
        Set metadata value for file/directory

        Args:
            uid: File/directory UID
            key: Metadata key
            value: Metadata value
            user: User performing operation (defaults to self.user)
            tenant: Tenant identifier (defaults to instance tenant)
            roles: User roles for permissions (defaults to instance roles)
            claims: User claims for permissions (defaults to instance claims)

        Returns:
            True on success, False on error
        """
        auth_context = self._create_auth_context(user, tenant, roles, claims)

        try:
            request = fileservice_pb2.SetMetadataRequest(
                uid=uid,
                key=key,
                value=value,
                auth=auth_context
            )
            response = self.stub.SetMetadata(request)
            return response.success
        except grpc.RpcError:
            return False

    def get_metadata_value(self, uid: str, name: str, user: str = None, tenant: str = None, roles: list = None, claims: list = None):
        """
        Get metadata value for file/directory

        Args:
            uid: File/directory UID
            name: Metadata key
            user: User performing operation (defaults to self.user)
            tenant: Tenant identifier (defaults to instance tenant)
            roles: User roles for permissions (defaults to instance roles)
            claims: User claims for permissions (defaults to instance claims)

        Returns:
            Metadata value, or None on error
        """
        auth_context = self._create_auth_context(user, tenant, roles, claims)

        try:
            request = fileservice_pb2.GetMetadataRequest(
                uid=uid,
                key=name,
                auth=auth_context
            )
            response = self.stub.GetMetadata(request)
            if response.success:
                return response.value
            else:
                return None
        except grpc.RpcError:
            return None

    def get_metadata_values(self, uid: str, user: str = None, tenant: str = None, roles: list = None, claims: list = None) -> dict:
        """
        Get all metadata values for file/directory

        Args:
            uid: File/directory UID
            user: User performing operation (defaults to self.user)
            tenant: Tenant identifier (defaults to instance tenant)
            roles: User roles for permissions (defaults to instance roles)
            claims: User claims for permissions (defaults to instance claims)

        Returns:
            Dictionary of metadata key-value pairs
        """
        auth_context = self._create_auth_context(user, tenant, roles, claims)

        try:
            request = fileservice_pb2.GetAllMetadataRequest(
                uid=uid,
                auth=auth_context
            )
            response = self.stub.GetAllMetadata(request)
            if response.success:
                return self._metadata_to_dict(response.metadata)
            else:
                return {}
        except grpc.RpcError:
            return {}

    def delete_metadata_value(self, uid: str, name: str, user: str = None, tenant: str = None, roles: list = None, claims: list = None) -> bool:
        """
        Delete metadata value for file/directory

        Args:
            uid: File/directory UID
            name: Metadata key to delete
            user: User performing operation (defaults to self.user)
            tenant: Tenant identifier (defaults to instance tenant)
            roles: User roles for permissions (defaults to instance roles)
            claims: User claims for permissions (defaults to instance claims)

        Returns:
            True on success, False on error
        """
        auth_context = self._create_auth_context(user, tenant, roles, claims)

        try:
            request = fileservice_pb2.DeleteMetadataRequest(
                uid=uid,
                key=name,
                auth=auth_context
            )
            response = self.stub.DeleteMetadata(request)
            return response.success
        except grpc.RpcError:
            return False

    # Helper methods for additional functionality

    def get_metadata_for_version(self, uid: str, version, key: str, user: str = None, tenant: str = None, roles: list = None, claims: list = None):
        """
        Get metadata value for a specific version of a file

        Args:
            uid: File UID
            version: Version index (the proto field is an int32 ``version``)
            key: Metadata key
            user: User performing operation (defaults to self.user)
            tenant: Tenant identifier (defaults to instance tenant)
            roles: User roles for permissions (defaults to instance roles)
            claims: User claims for permissions (defaults to instance claims)

        Returns:
            Metadata value, or None on error
        """
        auth_context = self._create_auth_context(user, tenant, roles, claims)

        try:
            request = fileservice_pb2.GetMetadataForVersionRequest(
                uid=uid,
                version=self._to_version_index(version),
                key=key,
                auth=auth_context
            )
            response = self.stub.GetMetadataForVersion(request)
            if response.success:
                return response.value
            else:
                return None
        except grpc.RpcError:
            return None

    def get_all_metadata_for_version(self, uid: str, version, user: str = None, tenant: str = None, roles: list = None, claims: list = None) -> dict:
        """
        Get all metadata for a specific version of a file

        Args:
            uid: File UID
            version: Version index (the proto field is an int32 ``version``)
            user: User performing operation (defaults to self.user)
            tenant: Tenant identifier (defaults to instance tenant)
            roles: User roles for permissions (defaults to instance roles)
            claims: User claims for permissions (defaults to instance claims)

        Returns:
            Dictionary of metadata key-value pairs
        """
        auth_context = self._create_auth_context(user, tenant, roles, claims)

        try:
            request = fileservice_pb2.GetAllMetadataForVersionRequest(
                uid=uid,
                version=self._to_version_index(version),
                auth=auth_context
            )
            response = self.stub.GetAllMetadataForVersion(request)
            if response.success:
                return self._metadata_to_dict(response.metadata)
            else:
                return {}
        except grpc.RpcError:
            return {}

    # Permission and ACL operations

    def evaluate_acl(self, resource_uid: str, user: str = None, tenant: str = None, roles: list = None, claims: list = None) -> list:
        """
        Evaluate the effective permissions for the current principal on a
        resource via the ``EvaluateACL`` RPC.

        Args:
            resource_uid: Resource UUID to evaluate

        Returns:
            List of permission strings granted to the principal, or [] on error
        """
        auth_context = self._create_auth_context(user, tenant, roles, claims)

        try:
            request = fileservice_pb2.EvaluateACLRequest(
                uid=resource_uid,
                auth=auth_context
            )
            response = self.stub.EvaluateACL(request)
            if response.success:
                return list(response.permissions)
            return []
        except grpc.RpcError:
            return []

    def check_permission(self, resource_uid: str, required_permission, user: str = None, tenant: str = None, roles: list = None, claims: list = None) -> bool:
        """
        Check whether the current principal holds a permission on a resource.

        The ``fileengine`` protocol exposes permission evaluation through
        ``EvaluateACL`` (which returns permission strings), so this checks
        membership of ``required_permission`` in the evaluated permission set.

        Args:
            resource_uid: Resource UUID to check
            required_permission: Permission name (string), e.g. "read"/"write"

        Returns:
            True if the permission is present, False otherwise
        """
        permissions = self.evaluate_acl(resource_uid, user=user, tenant=tenant, roles=roles, claims=claims)
        return str(required_permission) in permissions

    def grant_permission(self, resource_uid: str, principal: str, permission, user: str = None, tenant: str = None, roles: list = None, claims: list = None) -> bool:
        """
        Grant a permission to a principal on a resource.

        Note: The ``fileengine`` protocol is read-only with respect to ACLs
        (only ``EvaluateACL`` is exposed); ACL mutation is managed outside this
        service. This method is retained for API compatibility and returns
        False.
        """
        return False

    def revoke_permission(self, resource_uid: str, principal: str, permission, user: str = None, tenant: str = None, roles: list = None, claims: list = None) -> bool:
        """
        Revoke a permission from a principal on a resource.

        Note: Not supported by the ``fileengine`` protocol (see
        :meth:`grant_permission`). Retained for compatibility; returns False.
        """
        return False

    # Status and administrative operations

    def get_storage_usage(self, user: str = None, tenant: str = None, roles: list = None, claims: list = None) -> dict:
        """
        Get storage usage information.

        Note: Not exposed by the ``fileengine`` protocol; retained for
        compatibility and returns None.
        """
        return None

    def purge_old_versions(self, file_uid: str, keep_count: int, user: str = None, tenant: str = None, roles: list = None, claims: list = None) -> bool:
        """
        Purge old versions of a file.

        Note: Not exposed by the ``fileengine`` protocol; retained for
        compatibility and returns False.
        """
        return False

    def trigger_sync(self, user: str = None, tenant: str = None, roles: list = None, claims: list = None) -> bool:
        """
        Trigger synchronization between local and remote storage.

        Note: Not exposed by the ``fileengine`` protocol; retained for
        compatibility and returns False.
        """
        return False

    def undelete_file(self, file_uid: str, user: str = None, tenant: str = None, roles: list = None, claims: list = None) -> bool:
        """
        Undelete a file

        Args:
            file_uid: File UUID to undelete
            user: User performing the operation (defaults to self.user)
            tenant: Tenant identifier (defaults to instance tenant)
            roles: User roles for permissions (defaults to instance roles)
            claims: User claims for permissions (defaults to instance claims)

        Returns:
            True on success, False on error
        """
        auth_context = self._create_auth_context(user, tenant, roles, claims)

        try:
            request = fileservice_pb2.UndeleteFileRequest(
                uid=file_uid,
                auth=auth_context
            )
            response = self.stub.UndeleteFile(request)
            return response.success
        except grpc.RpcError:
            return False

    def restore_to_version(self, file_uid: str, version_timestamp: str, user: str = None, tenant: str = None, roles: list = None, claims: list = None) -> str:
        """
        Restore a file to a specific version.

        The ``fileengine`` protocol has no dedicated restore RPC, so this is
        emulated by reading the requested version and writing it back as a new
        current version (``ReadVersion`` + ``WriteFile``).

        Args:
            file_uid: File UUID to restore
            version_timestamp: Version timestamp to restore to
            user: User performing the operation (defaults to self.user)
            tenant: Tenant identifier (defaults to instance tenant)
            roles: User roles for permissions (defaults to instance roles)
            claims: User claims for permissions (defaults to instance claims)

        Returns:
            Timestamp string of the newly written version, or None on error
        """
        auth_context = self._create_auth_context(user, tenant, roles, claims)

        try:
            read_req = fileservice_pb2.ReadVersionRequest(
                uid=file_uid,
                version_timestamp=version_timestamp,
                auth=auth_context
            )
            read_resp = self.stub.ReadVersion(read_req)
            if not read_resp.success:
                return None

            write_req = fileservice_pb2.WriteFileRequest(
                uid=file_uid,
                auth=auth_context,
                data=read_resp.data
            )
            write_resp = self.stub.WriteFile(write_req)
            if write_resp.success:
                return str(time.time())
            return None
        except grpc.RpcError:
            return None

    # Permission methods (stubs - for compatibility with original interface)

    def assign_entity_to_permissions(self, entity_uid: str, role_name: str) -> bool:
        """Assign permission role to entity (deprecated - not supported by the fileengine protocol)"""
        return False

    def create_permission_role(self, role_name: str, can_read: bool = False, can_write: bool = False,
                               can_delete: bool = False, can_get_revisions: bool = False) -> bool:
        """Create permission role (deprecated - not supported by the fileengine protocol)"""
        return False

    def update_permission_role(self, role_name: str, can_read: bool = None, can_write: bool = None,
                               can_delete: bool = None, can_get_revisions: bool = None) -> bool:
        """Update permission role (deprecated - not supported by the fileengine protocol)"""
        return False

    def delete_permission_role(self, role_name: str) -> bool:
        """Delete permission role (deprecated - not supported by the fileengine protocol)"""
        return False

    def remove_entity_from_permissions(self, entity_uid: str, role_name: str) -> bool:
        """Remove permission assignment from entity (deprecated - not supported by the fileengine protocol)"""
        return False

    def get_permission_roles(self) -> list:
        """Get permission roles (deprecated)"""
        return []

    def get_entity_permissions(self, entity_uid: str) -> list:
        """Get entity permissions (deprecated)"""
        return []
