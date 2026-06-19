import unittest
from unittest.mock import Mock, patch, MagicMock
import grpc
from fileengine.client import ManagedFiles


class TestPermissionAndStatusOperations(unittest.TestCase):
    """Test permission and status operations against the fileengine protocol.

    The fileengine protocol (file_engine_cpp) exposes ACL *evaluation* only
    (EvaluateACL) plus UndeleteFile. Permission mutation (grant/revoke),
    storage usage, version purging and sync are not part of the protocol, so
    the client keeps them as compatibility no-ops; restore-to-version is
    emulated via ReadVersion + WriteFile.
    """

    def setUp(self):
        """Set up test fixtures before each test method."""
        # Mock the gRPC channel and stub
        self.mock_channel = Mock()
        self.mock_stub = Mock()

        # Patch the gRPC channel creation
        with patch('fileengine.client.grpc.insecure_channel', return_value=self.mock_channel):
            with patch('fileengine.client.fileservice_pb2_grpc.FileServiceStub', return_value=self.mock_stub):
                self.mf = ManagedFiles(
                    user_name="test_user",
                    user_roles=["user"],
                    user_claims=["read", "write"],
                    server_address="localhost:50051",
                    tenant="test_tenant"
                )

                # Set the stub on the instance
                self.mf.stub = self.mock_stub

    def tearDown(self):
        """Clean up after each test method."""
        self.mf.close()

    # --- ACL evaluation (EvaluateACL) ---

    def test_evaluate_acl_success(self):
        """Test evaluating effective permissions for a resource."""
        mock_response = Mock()
        mock_response.success = True
        mock_response.permissions = ["read", "write"]
        self.mock_stub.EvaluateACL.return_value = mock_response

        result = self.mf.evaluate_acl("resource-uuid")

        self.assertEqual(result, ["read", "write"])
        self.mock_stub.EvaluateACL.assert_called_once()

    def test_evaluate_acl_grpc_error(self):
        """Test ACL evaluation with gRPC error returns empty list."""
        self.mock_stub.EvaluateACL.side_effect = grpc.RpcError("Connection failed")

        result = self.mf.evaluate_acl("resource-uuid")

        self.assertEqual(result, [])

    def test_check_permission_true(self):
        """check_permission returns True when the permission is present."""
        mock_response = Mock()
        mock_response.success = True
        mock_response.permissions = ["read", "write"]
        self.mock_stub.EvaluateACL.return_value = mock_response

        result = self.mf.check_permission("resource-uuid", "read")

        self.assertTrue(result)

    def test_check_permission_false(self):
        """check_permission returns False when the permission is absent."""
        mock_response = Mock()
        mock_response.success = True
        mock_response.permissions = ["read"]
        self.mock_stub.EvaluateACL.return_value = mock_response

        result = self.mf.check_permission("resource-uuid", "write")

        self.assertFalse(result)

    # --- ACL mutation: not in protocol, kept as compatibility no-ops ---

    def test_grant_permission_not_supported(self):
        """grant_permission is not part of the protocol; returns False."""
        result = self.mf.grant_permission("resource-uuid", "test_user", "read")

        self.assertFalse(result)
        self.mock_stub.GrantPermission.assert_not_called()

    def test_revoke_permission_not_supported(self):
        """revoke_permission is not part of the protocol; returns False."""
        result = self.mf.revoke_permission("resource-uuid", "test_user", "read")

        self.assertFalse(result)
        self.mock_stub.RevokePermission.assert_not_called()

    # --- Administrative ops: not in protocol, kept as compatibility no-ops ---

    def test_get_storage_usage_not_supported(self):
        """get_storage_usage is not part of the protocol; returns None."""
        result = self.mf.get_storage_usage()

        self.assertIsNone(result)
        self.mock_stub.GetStorageUsage.assert_not_called()

    def test_purge_old_versions_not_supported(self):
        """purge_old_versions is not part of the protocol; returns False."""
        result = self.mf.purge_old_versions("file-uuid", 5)

        self.assertFalse(result)
        self.mock_stub.PurgeOldVersions.assert_not_called()

    def test_trigger_sync_not_supported(self):
        """trigger_sync is not part of the protocol; returns False."""
        result = self.mf.trigger_sync()

        self.assertFalse(result)
        self.mock_stub.TriggerSync.assert_not_called()

    # --- Undelete (UndeleteFile) ---

    def test_undelete_file_success(self):
        """Test successful file undeletion."""
        mock_response = Mock()
        mock_response.success = True
        self.mock_stub.UndeleteFile.return_value = mock_response

        result = self.mf.undelete_file("file-uuid")

        self.assertTrue(result)
        self.mock_stub.UndeleteFile.assert_called_once()

    def test_undelete_file_failure(self):
        """Test file undeletion failure."""
        mock_response = Mock()
        mock_response.success = False
        self.mock_stub.UndeleteFile.return_value = mock_response

        result = self.mf.undelete_file("file-uuid")

        self.assertFalse(result)

    # --- Restore to version: emulated via ReadVersion + WriteFile ---

    def test_restore_to_version_success(self):
        """restore_to_version reads the version and writes it back."""
        read_response = Mock()
        read_response.success = True
        read_response.data = b"old content"
        self.mock_stub.ReadVersion.return_value = read_response

        write_response = Mock()
        write_response.success = True
        self.mock_stub.WriteFile.return_value = write_response

        result = self.mf.restore_to_version("file-uuid", "1234567890.0")

        self.assertIsInstance(result, str)
        self.mock_stub.ReadVersion.assert_called_once()
        self.mock_stub.WriteFile.assert_called_once()
        # The version we wrote back must match the requested version's content
        write_args = self.mock_stub.WriteFile.call_args[0][0]
        self.assertEqual(write_args.data, b"old content")

    def test_restore_to_version_read_failure(self):
        """restore_to_version returns None when the version cannot be read."""
        read_response = Mock()
        read_response.success = False
        self.mock_stub.ReadVersion.return_value = read_response

        result = self.mf.restore_to_version("file-uuid", "1234567890.0")

        self.assertIsNone(result)
        self.mock_stub.WriteFile.assert_not_called()


if __name__ == '__main__':
    unittest.main()
