import unittest
from unittest.mock import Mock, patch, MagicMock
import grpc
from fileengine.client import ManagedFiles


class TestPermissionAndStatusOperations(unittest.TestCase):
    """Test the new permission and status operations"""

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

    def test_grant_permission_success(self):
        """Test successful permission grant."""
        # Mock the gRPC response
        mock_response = Mock()
        mock_response.success = True
        self.mock_stub.GrantPermission.return_value = mock_response
        
        result = self.mf.grant_permission("resource-uuid", "test_user", 0)  # 0 = READ permission
        
        self.assertTrue(result)
        self.mock_stub.GrantPermission.assert_called_once()

    def test_grant_permission_failure(self):
        """Test permission grant failure."""
        # Mock the gRPC response
        mock_response = Mock()
        mock_response.success = False
        self.mock_stub.GrantPermission.return_value = mock_response
        
        result = self.mf.grant_permission("resource-uuid", "test_user", 0)
        
        self.assertFalse(result)

    def test_grant_permission_grpc_error(self):
        """Test permission grant with gRPC error."""
        self.mock_stub.GrantPermission.side_effect = grpc.RpcError("Connection failed")
        
        result = self.mf.grant_permission("resource-uuid", "test_user", 0)
        
        self.assertFalse(result)

    def test_revoke_permission_success(self):
        """Test successful permission revoke."""
        # Mock the gRPC response
        mock_response = Mock()
        mock_response.success = True
        self.mock_stub.RevokePermission.return_value = mock_response
        
        result = self.mf.revoke_permission("resource-uuid", "test_user", 0)
        
        self.assertTrue(result)
        self.mock_stub.RevokePermission.assert_called_once()

    def test_revoke_permission_failure(self):
        """Test permission revoke failure."""
        # Mock the gRPC response
        mock_response = Mock()
        mock_response.success = False
        self.mock_stub.RevokePermission.return_value = mock_response
        
        result = self.mf.revoke_permission("resource-uuid", "test_user", 0)
        
        self.assertFalse(result)

    def test_check_permission_success(self):
        """Test successful permission check."""
        # Mock the gRPC response
        mock_response = Mock()
        mock_response.success = True
        mock_response.has_permission = True
        self.mock_stub.CheckPermission.return_value = mock_response
        
        result = self.mf.check_permission("resource-uuid", 0)
        
        self.assertTrue(result)
        self.mock_stub.CheckPermission.assert_called_once()

    def test_check_permission_failure(self):
        """Test permission check failure."""
        # Mock the gRPC response
        mock_response = Mock()
        mock_response.success = True
        mock_response.has_permission = False
        self.mock_stub.CheckPermission.return_value = mock_response
        
        result = self.mf.check_permission("resource-uuid", 0)
        
        self.assertFalse(result)

    def test_get_storage_usage_success(self):
        """Test successful storage usage retrieval."""
        # Mock the gRPC response
        mock_response = Mock()
        mock_response.success = True
        mock_response.total_space = 1000000000  # 1GB
        mock_response.used_space = 500000000   # 500MB
        mock_response.available_space = 500000000  # 500MB
        mock_response.usage_percentage = 50.0
        self.mock_stub.GetStorageUsage.return_value = mock_response
        
        result = self.mf.get_storage_usage()
        
        self.assertIsNotNone(result)
        self.assertEqual(result['total_space'], 1000000000)
        self.assertEqual(result['used_space'], 500000000)
        self.assertEqual(result['available_space'], 500000000)
        self.assertEqual(result['usage_percentage'], 50.0)

    def test_get_storage_usage_failure(self):
        """Test storage usage retrieval failure."""
        # Mock the gRPC response
        mock_response = Mock()
        mock_response.success = False
        self.mock_stub.GetStorageUsage.return_value = mock_response
        
        result = self.mf.get_storage_usage()
        
        self.assertIsNone(result)

    def test_purge_old_versions_success(self):
        """Test successful purging of old versions."""
        # Mock the gRPC response
        mock_response = Mock()
        mock_response.success = True
        self.mock_stub.PurgeOldVersions.return_value = mock_response
        
        result = self.mf.purge_old_versions("file-uuid", 5)
        
        self.assertTrue(result)
        self.mock_stub.PurgeOldVersions.assert_called_once()

    def test_trigger_sync_success(self):
        """Test successful sync trigger."""
        # Mock the gRPC response
        mock_response = Mock()
        mock_response.success = True
        self.mock_stub.TriggerSync.return_value = mock_response
        
        result = self.mf.trigger_sync()
        
        self.assertTrue(result)
        self.mock_stub.TriggerSync.assert_called_once()

    def test_undelete_file_success(self):
        """Test successful file undeletion."""
        # Mock the gRPC response
        mock_response = Mock()
        mock_response.success = True
        self.mock_stub.UndeleteFile.return_value = mock_response
        
        result = self.mf.undelete_file("file-uuid")
        
        self.assertTrue(result)
        self.mock_stub.UndeleteFile.assert_called_once()

    def test_restore_to_version_success(self):
        """Test successful restore to version."""
        # Mock the gRPC response
        mock_response = Mock()
        mock_response.success = True
        mock_response.restored_version = "1234567890.0"
        self.mock_stub.RestoreToVersion.return_value = mock_response
        
        result = self.mf.restore_to_version("file-uuid", "1234567890.0")
        
        self.assertEqual(result, "1234567890.0")
        self.mock_stub.RestoreToVersion.assert_called_once()

    def test_restore_to_version_failure(self):
        """Test restore to version failure."""
        # Mock the gRPC response
        mock_response = Mock()
        mock_response.success = False
        self.mock_stub.RestoreToVersion.return_value = mock_response
        
        result = self.mf.restore_to_version("file-uuid", "1234567890.0")
        
        self.assertIsNone(result)


if __name__ == '__main__':
    unittest.main()