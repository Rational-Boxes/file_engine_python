"""
FileEngine gRPC Client

Provides a Python interface to the FileEngine gRPC service.

This client targets the canonical ``fileengine_rpc`` protocol defined in
``file_engine_core/proto/fileservice.proto`` (the C++ FileService server),
exposing a familiar filesystem-like API backed by the gRPC service, with
UUID file identification and timestamp-string versioning.

The filesystem root may be referenced either as the empty string ``""`` or as
the all-zeros UUID ``00000000-0000-0000-0000-000000000000``.
"""

import grpc
import io
import time
from datetime import datetime
from typing import Dict, List, Optional, Union

from pydantic import BaseModel, Field

from . import fileservice_pb2
from . import fileservice_pb2_grpc

ROOT_UID = ""
ZERO_UID = "00000000-0000-0000-0000-000000000000"

# Single-letter permission aliases, matching the CLI.
_PERM_LETTERS = {
    'r': 'READ', 'w': 'WRITE', 'x': 'EXECUTE', 'd': 'DELETE',
    'l': 'LIST_DELETED', 'u': 'UNDELETE', 'v': 'VIEW_VERSIONS',
    'b': 'RETRIEVE_BACK_VERSION', 's': 'RESTORE_TO_VERSION',
    'm': 'MANAGE_ACL', 'i': 'ACL_INHERIT',
}


class FileType:
    """File type constants (mirror of the proto ``FileType`` enum)."""
    REGULAR_FILE = fileservice_pb2.REGULAR_FILE
    DIRECTORY = fileservice_pb2.DIRECTORY
    SYMLINK = fileservice_pb2.SYMLINK


class FileInfo(BaseModel):
    """File metadata information (mirror of the proto ``FileInfo``)."""
    uid: str
    name: str
    parent_uid: str = ""
    type: int = FileType.REGULAR_FILE
    size: int = 0
    owner: str = ""
    permissions: int = 0
    created_at: Optional[datetime] = None
    modified_at: Optional[datetime] = None
    version: str = ""
    rendition_count: int = 0  # hidden child renditions (files only; 0 for dirs)

    @property
    def is_dir(self) -> bool:
        return self.type == FileType.DIRECTORY

    @property
    def has_renditions(self) -> bool:
        """True if this file has hidden alternate-format renditions."""
        return self.rendition_count > 0


class DirectoryEntry(BaseModel):
    """Directory entry information (mirror of the proto ``DirectoryEntry``)."""
    uid: str
    name: str
    type: int = FileType.REGULAR_FILE
    size: int = 0
    created_at: Optional[datetime] = None
    modified_at: Optional[datetime] = None
    version_count: int = 0
    rendition_count: int = 0  # hidden child renditions (files only; 0 for dirs)

    @property
    def is_container(self) -> bool:
        return self.type == FileType.DIRECTORY

    @property
    def has_renditions(self) -> bool:
        """True if this file entry has hidden alternate-format renditions."""
        return self.rendition_count > 0


class Revision(BaseModel):
    """A single stored version of a file."""
    version: str  # timestamp string, e.g. "20260619_152218.171"
    name: str
    user: str


class StorageUsage(BaseModel):
    """Tenant/host storage usage figures, in bytes."""
    total_space: int
    used_space: int
    available_space: int
    usage_percentage: float


class FileSystemError(Exception):
    """Base exception for filesystem errors."""
    pass


def _safe_dt(ts):
    """Convert a server epoch-seconds value to a datetime, tolerating the
    out-of-range / wrong-unit values some entries carry. Returns None if the
    value cannot be interpreted as a sane timestamp."""
    if not ts:
        return None
    for divisor in (1, 1_000, 1_000_000, 1_000_000_000):
        try:
            return datetime.fromtimestamp(ts / divisor)
        except (OverflowError, OSError, ValueError):
            continue
    return None


def _coerce_permission(perm):
    """Accept a proto Permission int, an enum name, or a single letter."""
    if isinstance(perm, str):
        key = perm.strip()
        if len(key) == 1 and key.lower() in _PERM_LETTERS:
            key = _PERM_LETTERS[key.lower()]
        return fileservice_pb2.Permission.Value(key.upper())
    return int(perm)


def _coerce_effect(effect):
    """Accept a proto AclEffect int or the strings 'allow'/'deny'."""
    if isinstance(effect, str):
        return fileservice_pb2.DENY if effect.strip().lower() == 'deny' else fileservice_pb2.ALLOW
    return int(effect)


class ManagedFiles:
    """
    Python adapter for the FileEngine gRPC service that provides a
    filesystem-like interface.

    Authentication is the trusted-upstream model: the user, roles, tenant and
    claims supplied here are sent verbatim in every request's
    ``AuthenticationContext``. Creating directly under the filesystem root, and
    role/ACL administration, require the ``system_admin`` role.
    """

    def __init__(self, db_interface=None, storage_base: str = None, user_roles: list = None,
                 user_name: str = '', log_access: bool = False, permission_resolver=None,
                 s3_config: dict = None, server_address: str = "localhost:50051",
                 tenant: str = "", user_claims: list = None):
        """
        Initialize ManagedFiles with a gRPC client.

        Args:
            user_roles: User roles for permissions (default: [])
            user_name: Username for operations
            server_address: gRPC server address (host:port)
            tenant: Tenant for operations (default: "" -> 'default' tenant)
            user_claims: Additional user claims (list of str / (k, v) / dict)
            db_interface/storage_base/log_access/permission_resolver/s3_config:
                accepted for backward-compatibility; ignored.
        """
        self.user = user_name or 'user'
        self.roles = user_roles or []
        self.claims = user_claims or []
        self.log_access = log_access
        self.permissions = permission_resolver
        self.tenant = tenant

        # Allow large file payloads on unary RPCs (GetFile/GetVersion return the
        # whole file in one message). gRPC's default 4 MiB receive cap otherwise
        # silently fails reads of larger files; match the core's 64 MiB limit.
        # (get() also streams via StreamFileDownload, which is unaffected by this.)
        channel_options = [
            ("grpc.max_receive_message_length", 64 * 1024 * 1024),
            ("grpc.max_send_message_length", 64 * 1024 * 1024),
        ]
        self.channel = grpc.insecure_channel(server_address, options=channel_options)
        self.stub = fileservice_pb2_grpc.FileServiceStub(self.channel)

    def close(self):
        """Close the gRPC connection."""
        if hasattr(self, 'channel'):
            self.channel.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def set_user_information(self, user_name: str = None, roles: list = None, claims: list = None):
        """Set the default user/roles/claims used for subsequent operations."""
        if user_name:
            self.user = user_name
        if roles is not None:
            self.roles = roles
        if claims is not None:
            self.claims = claims

    def set_permission_resolver(self, permission_resolver):
        """Retained for compatibility; permission resolution is server-side."""
        self.permissions = permission_resolver

    # ------------------------------------------------------------------ #
    # Internal helpers
    # ------------------------------------------------------------------ #
    def _create_auth_context(self, user: str = None, tenant: str = None, roles: list = None, claims: list = None):
        """Build an AuthenticationContext for a request."""
        actual_user = user or self.user
        actual_tenant = tenant if tenant is not None else self.tenant
        actual_roles = roles if roles is not None else self.roles
        actual_claims = claims if claims is not None else self.claims

        claims_map = {}
        for claim in (actual_claims or []):
            if isinstance(claim, str):
                claims_map[claim] = claim
            elif isinstance(claim, dict):
                claims_map.update(claim)
            elif isinstance(claim, (tuple, list)) and len(claim) == 2:
                claims_map[claim[0]] = claim[1]

        return fileservice_pb2.AuthenticationContext(
            user=actual_user,
            roles=list(actual_roles or []),
            tenant=actual_tenant,
            claims=claims_map,
        )

    # ------------------------------------------------------------------ #
    # Directory operations
    # ------------------------------------------------------------------ #
    def mkdir(self, parent_uuid: str, name: str, user: str = None, tenant: str = None, roles: list = None, claims: list = None):
        """Create a directory. Returns the new UID, or False on error."""
        auth = self._create_auth_context(user, tenant, roles, claims)
        try:
            resp = self.stub.MakeDirectory(fileservice_pb2.MakeDirectoryRequest(
                parent_uid=parent_uuid, name=name, auth=auth))
            return resp.uid if resp.success else False
        except grpc.RpcError:
            return False

    def dir(self, uid, show_deleted: bool = False, user: str = None, tenant: str = None, roles: list = None, claims: list = None) -> Union[List[DirectoryEntry], bool]:
        """
        List directory contents.

        ``show_deleted`` routes to the dedicated ``ListDirectoryWithDeleted``
        RPC (plain ``ListDirectory`` filters soft-deleted entries server-side).

        Returns a list of dicts (uid, name, is_container, size, mtime, ctime,
        version_count), or False on error.
        """
        auth = self._create_auth_context(user, tenant, roles, claims)
        try:
            if show_deleted:
                resp = self.stub.ListDirectoryWithDeleted(
                    fileservice_pb2.ListDirectoryWithDeletedRequest(uid=uid, auth=auth))
            else:
                resp = self.stub.ListDirectory(
                    fileservice_pb2.ListDirectoryRequest(uid=uid, auth=auth))
            if not resp.success:
                return False
            return [
                DirectoryEntry(
                    uid=e.uid, name=e.name, type=e.type, size=e.size,
                    created_at=_safe_dt(e.created_at),
                    modified_at=_safe_dt(e.modified_at),
                    version_count=e.version_count,
                    rendition_count=e.rendition_count,
                )
                for e in resp.entries
            ]
        except grpc.RpcError:
            return False

    def list_deleted(self, uid, user: str = None, tenant: str = None, roles: list = None, claims: list = None):
        """Convenience for ``dir(uid, show_deleted=True)``."""
        return self.dir(uid, show_deleted=True, user=user, tenant=tenant, roles=roles, claims=claims)

    # ------------------------------------------------------------------ #
    # File operations
    # ------------------------------------------------------------------ #
    def touch(self, container_uuid: str, name: str, user: str = None, tenant: str = None, roles: list = None, claims: list = None):
        """Create an empty file. Returns the new UID, or False on error."""
        auth = self._create_auth_context(user, tenant, roles, claims)
        try:
            resp = self.stub.Touch(fileservice_pb2.TouchRequest(
                parent_uid=container_uuid, name=name, auth=auth))
            return resp.uid if resp.success else False
        except grpc.RpcError:
            return False

    def put(self, uid: str, payload=None, return_open: bool = False, user: str = None, tenant: str = None, roles: list = None, claims: list = None):
        """
        Write a new version of a file's content.

        Returns a float timestamp of the write on success, or False on error.
        """
        if return_open:
            raise NotImplementedError("return_open is not supported in the gRPC client")
        if payload is None:
            payload = b""
        if isinstance(payload, str):
            payload = payload.encode('utf-8')
        auth = self._create_auth_context(user, tenant, roles, claims)
        try:
            resp = self.stub.PutFile(fileservice_pb2.PutFileRequest(
                uid=uid, auth=auth, data=payload))
            return time.time() if resp.success else False
        except grpc.RpcError:
            return False

    def get(self, uid: str, back: int = 0, user: str = None, tenant: str = None, roles: list = None, claims: list = None):
        """
        Read file content as a BytesIO. ``back`` selects how many versions back
        (0 = latest). Returns False on error.
        """
        auth = self._create_auth_context(user, tenant, roles, claims)
        try:
            if back == 0:
                # Stream the latest version in chunks (no single-message size cap),
                # accumulating into a BytesIO. An error frame (success=False) ends
                # the stream; an empty file simply yields no data chunks.
                buf = io.BytesIO()
                for resp in self.stub.StreamFileDownload(
                        fileservice_pb2.GetFileRequest(uid=uid, auth=auth)):
                    if not resp.success:
                        return False
                    if resp.data:
                        buf.write(resp.data)
                buf.seek(0)
                return buf

            versions = self.revisions(uid, user=user, tenant=tenant, roles=roles, claims=claims)
            if not versions or len(versions) <= back:
                return False
            ts = versions[back].version
            resp = self.stub.GetVersion(fileservice_pb2.GetVersionRequest(
                uid=uid, version_timestamp=ts, auth=auth))
            if not resp.success:
                return False
            return io.BytesIO(resp.data)
        except grpc.RpcError:
            return False

    def entity_exists(self, entity_uid: str, include_deleted: bool = False) -> bool:
        """Return True if the entity exists."""
        try:
            resp = self.stub.Exists(fileservice_pb2.ExistsRequest(
                uid=entity_uid, auth=self._create_auth_context()))
            return bool(resp.success and resp.exists)
        except grpc.RpcError:
            return False

    def stat(self, uid: str, user: str = None, tenant: str = None, roles: list = None, claims: list = None) -> Optional[FileInfo]:
        """Return a FileInfo for the entity, or None on error."""
        auth = self._create_auth_context(user, tenant, roles, claims)
        try:
            resp = self.stub.Stat(fileservice_pb2.StatRequest(uid=uid, auth=auth))
            if not resp.success:
                return None
            i = resp.info
            return FileInfo(
                uid=i.uid, name=i.name, parent_uid=i.parent_uid, type=i.type,
                size=i.size, owner=i.owner, permissions=i.permissions,
                created_at=_safe_dt(i.created_at),
                modified_at=_safe_dt(i.modified_at),
                version=i.version,
                rendition_count=i.rendition_count)
        except grpc.RpcError:
            return None

    def list_renditions(self, uid: str, user: str = None, tenant: str = None, roles: list = None, claims: list = None):
        """List a file's hidden renditions (alternate-format children).

        Renditions are children of a file entity and are hidden from normal
        directory listings; pass the file's UID here to reveal them. Returns a
        list of DirectoryEntry, or False on error. (Equivalent to listing the
        file's UID directly.)
        """
        return self.dir(uid, user=user, tenant=tenant, roles=roles, claims=claims)

    def is_dir(self, uid: str, user: str = None, tenant: str = None, roles: list = None, claims: list = None) -> bool:
        """Return True if the entity is a directory."""
        info = self.stat(uid, user=user, tenant=tenant, roles=roles, claims=claims)
        return bool(info and info.type == fileservice_pb2.DIRECTORY)

    def get_file_mtime(self, uid: str, user: str = None, tenant: str = None, roles: list = None, claims: list = None):
        """Return the modification time as a datetime, or None."""
        info = self.stat(uid, user=user, tenant=tenant, roles=roles, claims=claims)
        return info.modified_at if info else None

    def get_folder_cdate(self, uid: str, user: str = None, tenant: str = None, roles: list = None, claims: list = None):
        """Return the creation time as a datetime, or None."""
        info = self.stat(uid, user=user, tenant=tenant, roles=roles, claims=claims)
        return info.created_at if info else None

    def file_name(self, uid: str, user: str = None, tenant: str = None, roles: list = None, claims: list = None) -> list:
        """Return ``[name]`` for the entity, or ``[]`` on error."""
        info = self.stat(uid, user=user, tenant=tenant, roles=roles, claims=claims)
        return [info.name] if info else []

    def get_parent(self, uid: str, user: str = None, tenant: str = None, roles: list = None, claims: list = None) -> str:
        """Return the parent UID (empty string for root), or '' on error."""
        info = self.stat(uid, user=user, tenant=tenant, roles=roles, claims=claims)
        return info.parent_uid if info else ""

    # ------------------------------------------------------------------ #
    # File manipulation
    # ------------------------------------------------------------------ #
    def move(self, source_uid: str, destination_uid: str, new_name: str = None, user: str = None, tenant: str = None, roles: list = None, claims: list = None) -> bool:
        """Move an entity under a new parent (optionally renaming it)."""
        auth = self._create_auth_context(user, tenant, roles, claims)
        try:
            resp = self.stub.Move(fileservice_pb2.MoveRequest(
                source_uid=source_uid, destination_parent_uid=destination_uid, auth=auth))
            if resp.success and new_name:
                return self.rename(source_uid, new_name, user=user, tenant=tenant, roles=roles, claims=claims)
            return resp.success
        except grpc.RpcError:
            return False

    def copy(self, source_uid: str, destination_uid: str, user: str = None, tenant: str = None, roles: list = None, claims: list = None) -> bool:
        """Copy an entity under a new parent (recursive for directories)."""
        auth = self._create_auth_context(user, tenant, roles, claims)
        try:
            resp = self.stub.Copy(fileservice_pb2.CopyRequest(
                source_uid=source_uid, destination_parent_uid=destination_uid, auth=auth))
            return resp.success
        except grpc.RpcError:
            return False

    def rename(self, uid: str, new_name: str, user: str = None, tenant: str = None, roles: list = None, claims: list = None) -> bool:
        """Rename an entity in place."""
        auth = self._create_auth_context(user, tenant, roles, claims)
        try:
            resp = self.stub.Rename(fileservice_pb2.RenameRequest(
                uid=uid, new_name=new_name, auth=auth))
            return resp.success
        except grpc.RpcError:
            return False

    def remove(self, uid: str, user: str = None, tenant: str = None, roles: list = None, claims: list = None) -> bool:
        """
        Soft-delete an entity. Directories use RemoveDirectory, files use
        RemoveFile. Returns True on success.
        """
        auth = self._create_auth_context(user, tenant, roles, claims)
        try:
            info = self.stub.Stat(fileservice_pb2.StatRequest(uid=uid, auth=auth))
            if info.success and info.info.type == fileservice_pb2.DIRECTORY:
                resp = self.stub.RemoveDirectory(
                    fileservice_pb2.RemoveDirectoryRequest(uid=uid, auth=auth))
            else:
                resp = self.stub.RemoveFile(
                    fileservice_pb2.RemoveFileRequest(uid=uid, auth=auth))
            return resp.success
        except grpc.RpcError:
            return False

    def undelete_file(self, file_uid: str, user: str = None, tenant: str = None, roles: list = None, claims: list = None) -> bool:
        """Restore a soft-deleted file."""
        auth = self._create_auth_context(user, tenant, roles, claims)
        try:
            resp = self.stub.UndeleteFile(fileservice_pb2.UndeleteFileRequest(
                uid=file_uid, auth=auth))
            return resp.success
        except grpc.RpcError:
            return False

    # ------------------------------------------------------------------ #
    # Versioning
    # ------------------------------------------------------------------ #
    def revisions(self, uid: str, user: str = None, tenant: str = None, roles: list = None, claims: list = None) -> List[Revision]:
        """Return the file's versions as ``Revision`` models, newest first."""
        auth = self._create_auth_context(user, tenant, roles, claims)
        try:
            resp = self.stub.ListVersions(fileservice_pb2.ListVersionsRequest(uid=uid, auth=auth))
            if not resp.success:
                return []
            return [Revision(version=ts, name=uid, user=auth.user) for ts in resp.versions]
        except grpc.RpcError:
            return []

    def restore_to_version(self, file_uid: str, version_timestamp: str, user: str = None, tenant: str = None, roles: list = None, claims: list = None):
        """Restore a file to a prior version. Returns the restored version, or False."""
        auth = self._create_auth_context(user, tenant, roles, claims)
        try:
            resp = self.stub.RestoreToVersion(fileservice_pb2.RestoreToVersionRequest(
                uid=file_uid, version_timestamp=version_timestamp, auth=auth))
            return resp.restored_version if resp.success else False
        except grpc.RpcError:
            return False

    def purge_old_versions(self, file_uid: str, keep_count: int, user: str = None, tenant: str = None, roles: list = None, claims: list = None) -> bool:
        """Purge old versions, keeping the ``keep_count`` most recent."""
        auth = self._create_auth_context(user, tenant, roles, claims)
        try:
            resp = self.stub.PurgeOldVersions(fileservice_pb2.PurgeOldVersionsRequest(
                uid=file_uid, keep_count=keep_count, auth=auth))
            return resp.success
        except grpc.RpcError:
            return False

    # ------------------------------------------------------------------ #
    # Metadata
    # ------------------------------------------------------------------ #
    def set_metadata_value(self, uid: str, key: str, value: str, user: str = None, tenant: str = None, roles: list = None, claims: list = None) -> bool:
        auth = self._create_auth_context(user, tenant, roles, claims)
        try:
            resp = self.stub.SetMetadata(fileservice_pb2.SetMetadataRequest(
                uid=uid, key=key, value=value, auth=auth))
            return resp.success
        except grpc.RpcError:
            return False

    def get_metadata_value(self, uid: str, name: str, user: str = None, tenant: str = None, roles: list = None, claims: list = None):
        auth = self._create_auth_context(user, tenant, roles, claims)
        try:
            resp = self.stub.GetMetadata(fileservice_pb2.GetMetadataRequest(
                uid=uid, key=name, auth=auth))
            return resp.value if resp.success else None
        except grpc.RpcError:
            return None

    def get_metadata_values(self, uid: str, user: str = None, tenant: str = None, roles: list = None, claims: list = None) -> dict:
        auth = self._create_auth_context(user, tenant, roles, claims)
        try:
            resp = self.stub.GetAllMetadata(fileservice_pb2.GetAllMetadataRequest(uid=uid, auth=auth))
            return dict(resp.metadata) if resp.success else {}
        except grpc.RpcError:
            return {}

    def delete_metadata_value(self, uid: str, name: str, user: str = None, tenant: str = None, roles: list = None, claims: list = None) -> bool:
        auth = self._create_auth_context(user, tenant, roles, claims)
        try:
            resp = self.stub.DeleteMetadata(fileservice_pb2.DeleteMetadataRequest(
                uid=uid, key=name, auth=auth))
            return resp.success
        except grpc.RpcError:
            return False

    def get_metadata_for_version(self, uid: str, version, key: str, user: str = None, tenant: str = None, roles: list = None, claims: list = None):
        auth = self._create_auth_context(user, tenant, roles, claims)
        try:
            resp = self.stub.GetMetadataForVersion(fileservice_pb2.GetMetadataForVersionRequest(
                uid=uid, version_timestamp=str(version), key=key, auth=auth))
            return resp.value if resp.success else None
        except grpc.RpcError:
            return None

    def get_all_metadata_for_version(self, uid: str, version, user: str = None, tenant: str = None, roles: list = None, claims: list = None) -> dict:
        auth = self._create_auth_context(user, tenant, roles, claims)
        try:
            resp = self.stub.GetAllMetadataForVersion(fileservice_pb2.GetAllMetadataForVersionRequest(
                uid=uid, version_timestamp=str(version), auth=auth))
            return dict(resp.metadata) if resp.success else {}
        except grpc.RpcError:
            return {}

    # ------------------------------------------------------------------ #
    # Permissions / ACL
    # ------------------------------------------------------------------ #
    def check_permission(self, resource_uid: str, required_permission, user: str = None, tenant: str = None, roles: list = None, claims: list = None) -> bool:
        """
        Check whether the acting identity (user/roles) has a permission on a
        resource. ``required_permission`` accepts a proto Permission int, an
        enum name, or a single letter (r/w/x/d/...).
        """
        auth = self._create_auth_context(user, tenant, roles, claims)
        try:
            resp = self.stub.CheckPermission(fileservice_pb2.CheckPermissionRequest(
                resource_uid=resource_uid,
                required_permission=_coerce_permission(required_permission),
                auth=auth))
            return bool(resp.success and resp.has_permission)
        except grpc.RpcError:
            return False

    def get_effective_permissions(self, resource_uid: str, user: str = None, tenant: str = None,
                                  roles: list = None, claims: list = None) -> list:
        """
        Return the principal's full effective permission set on a resource as a
        list of permission names (e.g. ['READ', 'WRITE']) in a single call,
        without accessing the entity. Intended for external systems (e.g. a
        search indexer) that must respect filesystem permissions. The principal
        is (user, roles, claims); returns [] on error or no permissions.
        """
        auth = self._create_auth_context(user, tenant, roles, claims)
        try:
            resp = self.stub.GetEffectivePermissions(fileservice_pb2.GetEffectivePermissionsRequest(
                resource_uid=resource_uid, auth=auth))
            if not resp.success:
                return []
            return [fileservice_pb2.Permission.Name(p) for p in resp.permissions]
        except grpc.RpcError:
            return []

    def grant_permission(self, resource_uid: str, principal: str, permission, effect="allow",
                         user: str = None, tenant: str = None, roles: list = None, claims: list = None) -> bool:
        """
        Grant a permission to a principal on a resource. Prefix the principal
        with ``role:`` to target a role, or ``claim:<key>=<value>`` to target an
        attribute-based (ABAC) claim — the rule then matches any requester whose
        auth claims contain that key/value pair. ``effect`` is 'allow' (default)
        or 'deny'. Requires MANAGE_ACL on the resource (or system_admin).
        """
        auth = self._create_auth_context(user, tenant, roles, claims)
        try:
            resp = self.stub.GrantPermission(fileservice_pb2.GrantPermissionRequest(
                resource_uid=resource_uid, principal=principal,
                permission=_coerce_permission(permission),
                effect=_coerce_effect(effect), auth=auth))
            return resp.success
        except grpc.RpcError:
            return False

    def revoke_permission(self, resource_uid: str, principal: str, permission, effect="allow",
                          user: str = None, tenant: str = None, roles: list = None, claims: list = None) -> bool:
        """Revoke a previously granted permission (mirror of grant_permission)."""
        auth = self._create_auth_context(user, tenant, roles, claims)
        try:
            resp = self.stub.RevokePermission(fileservice_pb2.RevokePermissionRequest(
                resource_uid=resource_uid, principal=principal,
                permission=_coerce_permission(permission),
                effect=_coerce_effect(effect), auth=auth))
            return resp.success
        except grpc.RpcError:
            return False

    # ------------------------------------------------------------------ #
    # Role management
    # ------------------------------------------------------------------ #
    def create_role(self, role: str, user: str = None, tenant: str = None, roles: list = None, claims: list = None) -> bool:
        auth = self._create_auth_context(user, tenant, roles, claims)
        try:
            return self.stub.CreateRole(fileservice_pb2.CreateRoleRequest(role=role, auth=auth)).success
        except grpc.RpcError:
            return False

    def delete_role(self, role: str, user: str = None, tenant: str = None, roles: list = None, claims: list = None) -> bool:
        auth = self._create_auth_context(user, tenant, roles, claims)
        try:
            return self.stub.DeleteRole(fileservice_pb2.DeleteRoleRequest(role=role, auth=auth)).success
        except grpc.RpcError:
            return False

    def assign_user_to_role(self, target_user: str, role: str, user: str = None, tenant: str = None, roles: list = None, claims: list = None) -> bool:
        auth = self._create_auth_context(user, tenant, roles, claims)
        try:
            return self.stub.AssignUserToRole(fileservice_pb2.AssignUserToRoleRequest(
                user=target_user, role=role, auth=auth)).success
        except grpc.RpcError:
            return False

    def remove_user_from_role(self, target_user: str, role: str, user: str = None, tenant: str = None, roles: list = None, claims: list = None) -> bool:
        auth = self._create_auth_context(user, tenant, roles, claims)
        try:
            return self.stub.RemoveUserFromRole(fileservice_pb2.RemoveUserFromRoleRequest(
                user=target_user, role=role, auth=auth)).success
        except grpc.RpcError:
            return False

    def get_roles_for_user(self, target_user: str, user: str = None, tenant: str = None, roles: list = None, claims: list = None) -> list:
        auth = self._create_auth_context(user, tenant, roles, claims)
        try:
            resp = self.stub.GetRolesForUser(fileservice_pb2.GetRolesForUserRequest(user=target_user, auth=auth))
            return list(resp.roles) if resp.success else []
        except grpc.RpcError:
            return []

    def get_users_for_role(self, role: str, user: str = None, tenant: str = None, roles: list = None, claims: list = None) -> list:
        auth = self._create_auth_context(user, tenant, roles, claims)
        try:
            resp = self.stub.GetUsersForRole(fileservice_pb2.GetUsersForRoleRequest(role=role, auth=auth))
            return list(resp.users) if resp.success else []
        except grpc.RpcError:
            return []

    def get_all_roles(self, user: str = None, tenant: str = None, roles: list = None, claims: list = None) -> list:
        auth = self._create_auth_context(user, tenant, roles, claims)
        try:
            resp = self.stub.GetAllRoles(fileservice_pb2.GetAllRolesRequest(auth=auth))
            return list(resp.roles) if resp.success else []
        except grpc.RpcError:
            return []

    # ------------------------------------------------------------------ #
    # Administrative
    # ------------------------------------------------------------------ #
    def get_storage_usage(self, user: str = None, tenant: str = None, roles: list = None, claims: list = None) -> Optional[StorageUsage]:
        """Return a ``StorageUsage`` model, or None on error."""
        auth = self._create_auth_context(user, tenant, roles, claims)
        try:
            resp = self.stub.GetStorageUsage(fileservice_pb2.StorageUsageRequest(
                auth=auth, tenant=(tenant if tenant is not None else self.tenant)))
            if not resp.success:
                return None
            return StorageUsage(
                total_space=resp.total_space,
                used_space=resp.used_space,
                available_space=resp.available_space,
                usage_percentage=resp.usage_percentage,
            )
        except grpc.RpcError:
            return None

    def trigger_sync(self, user: str = None, tenant: str = None, roles: list = None, claims: list = None) -> bool:
        """Trigger object-store synchronization for the tenant."""
        auth = self._create_auth_context(user, tenant, roles, claims)
        try:
            resp = self.stub.TriggerSync(fileservice_pb2.TriggerSyncRequest(
                tenant=(tenant if tenant is not None else self.tenant), auth=auth))
            return resp.success
        except grpc.RpcError:
            return False
