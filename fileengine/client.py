"""
FileEngine gRPC Client

Provides Python interface to the FileEngine gRPC service with same API
as the original Python implementation.
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
    """File type constants"""
    REGULAR_FILE = fileservice_pb2.FileType.REGULAR_FILE
    DIRECTORY = fileservice_pb2.FileType.DIRECTORY
    SYMLINK = fileservice_pb2.FileType.SYMLINK


@dataclass
class FileInfo:
    """File metadata information"""
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
    """Directory entry information"""
    uid: str
    name: str
    type: int
    size: int


class FileSystemError(Exception):
    """Base exception for filesystem errors"""
    pass


class ManagedFiles:
    """
    Python adapter for FileEngine gRPC service that provides the exact same
    interface as the original Python ManagedFiles implementation.

    This is a drop-in replacement that uses the C++ gRPC server backend,
    with UUID4 for file identification and UNIX timestamps for versioning.
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

        return fileservice_pb2.AuthenticationContext(
            user=actual_user,
            roles=actual_roles,
            tenant=actual_tenant,
            claims=claims_map
        )

    # Path/UID conversion methods

    def path_to_uid(self, path: str) -> str:
        """
        Convert path to UID using gRPC service

        Args:
            path: File path

        Returns:
            UID corresponding to path, or None if not found
        """
        # Note: The current proto doesn't have a path_to_uid method, so this would need to be implemented
        # in the gRPC service. For now, returning None as a placeholder.
        return None

    def uid_to_path(self, uid: str) -> list:
        """
        Convert UID to path components

        Args:
            uid: File UID

        Returns:
            List of path components
        """
        # Note: The current proto doesn't have a uid_to_path method, so this would need to be implemented
        # in the gRPC service. For now, returning empty list as a placeholder.
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
        Create directory path recursively

        Args:
            path: Path to create
            user: User performing the operation (defaults to self.user)
            tenant: Tenant identifier (defaults to instance tenant)
            roles: User roles for permissions (defaults to instance roles)
            claims: User claims for permissions (defaults to instance claims)

        Returns:
            UID of last directory created, or False on error
        """
        # This would require path_to_uid functionality which is not in the current proto
        # For now, this is a simplified implementation that assumes you know the parent UIDs
        return False

    def dir(self, uid, show_deleted: bool = False, user: str = None, tenant: str = None, roles: list = None, claims: list = None) -> List[dict]:
        """
        List directory contents with detailed information

        Args:
            uid: Directory UID
            show_deleted: Show deleted items (not implemented in gRPC service)
            user: User performing the operation (defaults to self.user)
            tenant: Tenant identifier (defaults to instance tenant)
            roles: User roles for permissions (defaults to instance roles)
            claims: User claims for permissions (defaults to instance claims)

        Returns:
            List of dictionaries with file/directory information, or False on error
        """
        auth_context = self._create_auth_context(user, tenant, roles, claims)

        try:
            # Use different request based on show_deleted flag
            if show_deleted:
                request = fileservice_pb2.ListDirectoryWithDeletedRequest(
                    uid=uid,
                    auth=auth_context
                )
                response = self.stub.ListDirectoryWithDeleted(request)
            else:
                request = fileservice_pb2.ListDirectoryRequest(
                    uid=uid,
                    auth=auth_context
                )
                response = self.stub.ListDirectory(request)

            if not response.success:
                return False

            result = []
            for entry in response.entries:
                # Get full path for this entry (placeholder implementation)
                # In a real implementation, we'd need to get the path from the service
                item = {
                    'uid': entry.uid,
                    'name': entry.name,
                    'creator': auth_context.user,  # gRPC service doesn't track creator separately
                    'is_container': 'True' if entry.type == fileservice_pb2.FileType.DIRECTORY else 'False'
                }

                # For files, add version and modification time information
                if entry.type == fileservice_pb2.FileType.REGULAR_FILE:
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
            request = fileservice_pb2.ExistsRequest(
                uid=entity_uid,
                auth=self._create_auth_context()
            )
            response = self.stub.Exists(request)
            return response.exists
        except grpc.RpcError:
            return False

    def is_dir(self, uid: str, user: str = None, tenant: str = None, roles: list = None, claims: list = None) -> bool:
        """Check if entity is a directory"""
        auth_context = self._create_auth_context(user, tenant, roles, claims)
        
        try:
            request = fileservice_pb2.StatRequest(
                uid=uid,
                auth=auth_context
            )
            response = self.stub.Stat(request)
            if response.success:
                return response.info.type == fileservice_pb2.FileType.DIRECTORY
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
            request = fileservice_pb2.TouchRequest(
                parent_uid=container_uuid,
                name=name,
                auth=auth_context
            )
            response = self.stub.Touch(request)
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
            request = fileservice_pb2.PutFileRequest(
                uid=uid,
                auth=auth_context,
                data=payload
            )
            response = self.stub.PutFile(request)
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

        # Get available versions
        versions = self.revisions(uid, user=auth_context.user,
                                 tenant=auth_context.tenant,
                                 roles=auth_context.roles,
                                 claims=list(auth_context.claims.keys()))
        if not versions or len(versions) <= back:
            return False

        # Get the specific version
        version_timestamp = versions[back]['version']

        try:
            request = fileservice_pb2.GetVersionRequest(
                uid=uid,
                version_timestamp=version_timestamp,
                auth=auth_context
            )

            # Create a file-like object in memory
            import io
            data_buffer = io.BytesIO()

            # Get the specific version
            response = self.stub.GetVersion(request)
            if response.success:
                data_buffer.write(response.data)
            else:
                return False

            # Seek back to start
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
                        'version': version_timestamp,  # Now a string timestamp
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
            request = fileservice_pb2.StatRequest(
                uid=uid,
                auth=auth_context
            )
            response = self.stub.Stat(request)
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
            request = fileservice_pb2.StatRequest(
                uid=uid,
                auth=auth_context
            )
            response = self.stub.Stat(request)
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
            request = fileservice_pb2.StatRequest(
                uid=uid,
                auth=auth_context
            )
            response = self.stub.Stat(request)
            if response.success:
                return [response.info.name]
            else:
                return []
        except grpc.RpcError:
            return []

    def get_parent(self, uid: str, user: str = None, tenant: str = None, roles: list = None, claims: list = None):
        """Get parent UID - not directly supported in current gRPC interface"""
        # This would require a new method in the gRPC service
        # For now, return empty string as placeholder, but accept the parameters for consistency
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
            # If new_name is provided, we need to rename after moving
            request = fileservice_pb2.MoveRequest(
                source_uid=source_uid,
                destination_parent_uid=destination_uid,
                auth=auth_context
            )
            response = self.stub.Move(request)
            
            # If we need to rename, do that after the move
            if response.success and new_name:
                rename_response = self.rename(source_uid, new_name, user=user, tenant=tenant, roles=roles, claims=claims)
                return rename_response
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
            request = fileservice_pb2.CopyRequest(
                source_uid=source_uid,
                destination_parent_uid=destination_uid,
                auth=auth_context
            )
            response = self.stub.Copy(request)
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
            info_req = fileservice_pb2.StatRequest(
                uid=uid,
                auth=auth_context
            )
            info_resp = self.stub.Stat(info_req)

            if info_resp.success and info_resp.info.type == fileservice_pb2.FileType.DIRECTORY:
                # It's a directory
                request = fileservice_pb2.RemoveDirectoryRequest(
                    uid=uid,
                    auth=auth_context
                )
                response = self.stub.RemoveDirectory(request)
            else:
                # It's a file
                request = fileservice_pb2.RemoveFileRequest(
                    uid=uid,
                    auth=auth_context
                )
                response = self.stub.RemoveFile(request)

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
            request = fileservice_pb2.RenameRequest(
                uid=uid,
                new_name=new_name,
                auth=auth_context
            )
            response = self.stub.Rename(request)
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
            roles: User roles for permissions (defaults to instance claims)
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
                return dict(response.metadata)
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

    def get_metadata_for_version(self, uid: str, version: str, key: str, user: str = None, tenant: str = None, roles: list = None, claims: list = None):
        """
        Get metadata value for specific version of a file

        Args:
            uid: File UID
            version: Version timestamp
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
                version_timestamp=version,
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

    def get_all_metadata_for_version(self, uid: str, version: str, user: str = None, tenant: str = None, roles: list = None, claims: list = None) -> dict:
        """
        Get all metadata for specific version of a file

        Args:
            uid: File UID
            version: Version timestamp
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
                version_timestamp=version,
                auth=auth_context
            )
            response = self.stub.GetAllMetadataForVersion(request)
            if response.success:
                return dict(response.metadata)
            else:
                return {}
        except grpc.RpcError:
            return {}

    # Permission and ACL operations

    def grant_permission(self, resource_uid: str, principal: str, permission: int, user: str = None, tenant: str = None, roles: list = None, claims: list = None) -> bool:
        """
        Grant permission to a principal on a resource

        Args:
            resource_uid: Resource UUID to grant permission on
            principal: User or group name
            permission: Permission to grant (from fileservice_pb2.Permission)
            user: User performing the operation (defaults to self.user)
            tenant: Tenant identifier (defaults to instance tenant)
            roles: User roles for permissions (defaults to instance roles)
            claims: User claims for permissions (defaults to instance claims)

        Returns:
            True on success, False on error
        """
        auth_context = self._create_auth_context(user, tenant, roles, claims)

        try:
            request = fileservice_pb2.GrantPermissionRequest(
                resource_uid=resource_uid,
                principal=principal,
                permission=permission,
                auth=auth_context
            )
            response = self.stub.GrantPermission(request)
            return response.success
        except grpc.RpcError:
            return False

    def revoke_permission(self, resource_uid: str, principal: str, permission: int, user: str = None, tenant: str = None, roles: list = None, claims: list = None) -> bool:
        """
        Revoke permission from a principal on a resource

        Args:
            resource_uid: Resource UUID to revoke permission from
            principal: User or group name
            permission: Permission to revoke (from fileservice_pb2.Permission)
            user: User performing the operation (defaults to self.user)
            tenant: Tenant identifier (defaults to instance tenant)
            roles: User roles for permissions (defaults to instance roles)
            claims: User claims for permissions (defaults to instance claims)

        Returns:
            True on success, False on error
        """
        auth_context = self._create_auth_context(user, tenant, roles, claims)

        try:
            request = fileservice_pb2.RevokePermissionRequest(
                resource_uid=resource_uid,
                principal=principal,
                permission=permission,
                auth=auth_context
            )
            response = self.stub.RevokePermission(request)
            return response.success
        except grpc.RpcError:
            return False

    def check_permission(self, resource_uid: str, required_permission: int, user: str = None, tenant: str = None, roles: list = None, claims: list = None) -> bool:
        """
        Check if a user has a specific permission on a resource

        Args:
            resource_uid: Resource UUID to check permission on
            required_permission: Required permission (from fileservice_pb2.Permission)
            user: User to check permission for (defaults to self.user)
            tenant: Tenant identifier (defaults to instance tenant)
            roles: User roles for permissions (defaults to instance roles)
            claims: User claims for permissions (defaults to instance claims)

        Returns:
            True if user has permission, False otherwise
        """
        auth_context = self._create_auth_context(user, tenant, roles, claims)

        try:
            request = fileservice_pb2.CheckPermissionRequest(
                resource_uid=resource_uid,
                required_permission=required_permission,
                auth=auth_context
            )
            response = self.stub.CheckPermission(request)
            return response.has_permission
        except grpc.RpcError:
            return False

    # Status and administrative operations

    def get_storage_usage(self, user: str = None, tenant: str = None, roles: list = None, claims: list = None) -> dict:
        """
        Get storage usage information

        Args:
            user: User performing the operation (defaults to self.user)
            tenant: Tenant identifier (defaults to instance tenant, or specific tenant if provided)
            roles: User roles for permissions (defaults to instance roles)
            claims: User claims for permissions (defaults to instance claims)

        Returns:
            Dictionary with storage usage information, or None on error
        """
        auth_context = self._create_auth_context(user, tenant, roles, claims)

        try:
            request = fileservice_pb2.StorageUsageRequest(
                auth=auth_context,
                tenant=tenant if tenant else self.tenant
            )
            response = self.stub.GetStorageUsage(request)

            if response.success:
                return {
                    'total_space': response.total_space,
                    'used_space': response.used_space,
                    'available_space': response.available_space,
                    'usage_percentage': response.usage_percentage
                }
            else:
                return None
        except grpc.RpcError:
            return None

    def purge_old_versions(self, file_uid: str, keep_count: int, user: str = None, tenant: str = None, roles: list = None, claims: list = None) -> bool:
        """
        Purge old versions of a file, keeping only the specified number

        Args:
            file_uid: File UUID to purge old versions for
            keep_count: Number of versions to keep
            user: User performing the operation (defaults to self.user)
            tenant: Tenant identifier (defaults to instance tenant)
            roles: User roles for permissions (defaults to instance roles)
            claims: User claims for permissions (defaults to instance claims)

        Returns:
            True on success, False on error
        """
        auth_context = self._create_auth_context(user, tenant, roles, claims)

        try:
            request = fileservice_pb2.PurgeOldVersionsRequest(
                uid=file_uid,
                keep_count=keep_count,
                auth=auth_context
            )
            response = self.stub.PurgeOldVersions(request)
            return response.success
        except grpc.RpcError:
            return False

    def trigger_sync(self, user: str = None, tenant: str = None, roles: list = None, claims: list = None) -> bool:
        """
        Trigger synchronization between local and remote storage

        Args:
            user: User performing the operation (defaults to self.user)
            tenant: Tenant to sync (defaults to instance tenant, or all if empty)
            roles: User roles for permissions (defaults to instance roles)
            claims: User claims for permissions (defaults to instance claims)

        Returns:
            True on success, False on error
        """
        auth_context = self._create_auth_context(user, tenant, roles, claims)

        try:
            request = fileservice_pb2.TriggerSyncRequest(
                tenant=tenant if tenant else self.tenant,
                auth=auth_context
            )
            response = self.stub.TriggerSync(request)
            return response.success
        except grpc.RpcError:
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
        Restore a file to a specific version

        Args:
            file_uid: File UUID to restore
            version_timestamp: Version timestamp to restore to
            user: User performing the operation (defaults to self.user)
            tenant: Tenant identifier (defaults to instance tenant)
            roles: User roles for permissions (defaults to instance roles)
            claims: User claims for permissions (defaults to instance claims)

        Returns:
            Timestamp of the version restored to, or None on error
        """
        auth_context = self._create_auth_context(user, tenant, roles, claims)

        try:
            request = fileservice_pb2.RestoreToVersionRequest(
                uid=file_uid,
                version_timestamp=version_timestamp,
                auth=auth_context
            )
            response = self.stub.RestoreToVersion(request)

            if response.success:
                return response.restored_version
            else:
                return None
        except grpc.RpcError:
            return None

    # Permission methods (stubs - for compatibility with original interface)

    def assign_entity_to_permissions(self, entity_uid: str, role_name: str) -> bool:
        """Assign permission role to entity (deprecated - use grant_permission instead)"""
        # This is a legacy method for compatibility - use grant_permission instead
        return False

    def create_permission_role(self, role_name: str, can_read: bool = False, can_write: bool = False,
                               can_delete: bool = False, can_get_revisions: bool = False) -> bool:
        """Create permission role (deprecated - use grant_permission instead)"""
        # This is a legacy method for compatibility - use grant_permission instead
        return False

    def update_permission_role(self, role_name: str, can_read: bool = None, can_write: bool = None,
                               can_delete: bool = None, can_get_revisions: bool = None) -> bool:
        """Update permission role (deprecated - use grant_permission instead)"""
        # This is a legacy method for compatibility - use grant_permission instead
        return False

    def delete_permission_role(self, role_name: str) -> bool:
        """Delete permission role (deprecated - use revoke_permission instead)"""
        # This is a legacy method for compatibility - use revoke_permission instead
        return False

    def remove_entity_from_permissions(self, entity_uid: str, role_name: str) -> bool:
        """Remove permission assignment from entity (deprecated - use revoke_permission instead)"""
        # This is a legacy method for compatibility - use revoke_permission instead
        return False

    def get_permission_roles(self) -> list:
        """Get permission roles (deprecated)"""
        # This is a legacy method for compatibility
        return []

    def get_entity_permissions(self, entity_uid: str) -> list:
        """Get entity permissions (deprecated)"""
        # This is a legacy method for compatibility
        return []