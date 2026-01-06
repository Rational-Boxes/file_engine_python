import unittest
from unittest.mock import Mock, patch, MagicMock
import grpc
from datetime import datetime
import io

from fileengine.client import ManagedFiles, FileType, FileInfo, DirectoryEntry, FileSystemError


class TestManagedFiles(unittest.TestCase):
    """Test suite for the ManagedFiles class"""

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

    def test_init_with_parameters(self):
        """Test initialization with all parameters."""
        mf = ManagedFiles(
            user_name="alice",
            user_roles=["admin", "user"],
            user_claims=["read", "write", "delete"],
            server_address="localhost:50051",
            tenant="my_tenant"
        )
        
        self.assertEqual(mf.user, "alice")
        self.assertEqual(mf.roles, ["admin", "user"])
        self.assertEqual(mf.claims, ["read", "write", "delete"])
        self.assertEqual(mf.tenant, "my_tenant")
        
        mf.close()

    def test_set_user_information(self):
        """Test setting user information."""
        self.mf.set_user_information(
            user_name="new_user",
            roles=["new_role"],
            claims=["new_claim"]
        )
        
        self.assertEqual(self.mf.user, "new_user")
        self.assertEqual(self.mf.roles, ["new_role"])
        self.assertEqual(self.mf.claims, ["new_claim"])

    def test_mkdir_success(self):
        """Test successful directory creation."""
        # Mock the gRPC response
        mock_response = Mock()
        mock_response.success = True
        mock_response.uid = "test-uuid-123"
        self.mock_stub.MakeDirectory.return_value = mock_response
        
        result = self.mf.mkdir("parent-uuid", "test_dir")
        
        self.assertEqual(result, "test-uuid-123")
        self.mock_stub.MakeDirectory.assert_called_once()

    def test_mkdir_failure(self):
        """Test directory creation failure."""
        # Mock the gRPC response
        mock_response = Mock()
        mock_response.success = False
        self.mock_stub.MakeDirectory.return_value = mock_response
        
        result = self.mf.mkdir("parent-uuid", "test_dir")
        
        self.assertFalse(result)

    def test_mkdir_grpc_error(self):
        """Test directory creation with gRPC error."""
        self.mock_stub.MakeDirectory.side_effect = grpc.RpcError("Connection failed")
        
        result = self.mf.mkdir("parent-uuid", "test_dir")
        
        self.assertFalse(result)

    def test_touch_success(self):
        """Test successful file creation."""
        # Mock the gRPC response
        mock_response = Mock()
        mock_response.success = True
        mock_response.uid = "test-uuid-456"
        self.mock_stub.Touch.return_value = mock_response
        
        result = self.mf.touch("parent-uuid", "test_file.txt")
        
        self.assertEqual(result, "test-uuid-456")

    def test_touch_failure(self):
        """Test file creation failure."""
        # Mock the gRPC response
        mock_response = Mock()
        mock_response.success = False
        self.mock_stub.Touch.return_value = mock_response
        
        result = self.mf.touch("parent-uuid", "test_file.txt")
        
        self.assertFalse(result)

    def test_put_success(self):
        """Test successful file upload."""
        # Mock the gRPC response
        mock_response = Mock()
        mock_response.success = True
        self.mock_stub.PutFile.return_value = mock_response
        
        result = self.mf.put("file-uuid", b"test content")
        
        self.assertIsInstance(result, float)  # Should return a timestamp
        self.mock_stub.PutFile.assert_called_once()

    def test_put_with_string_content(self):
        """Test file upload with string content."""
        # Mock the gRPC response
        mock_response = Mock()
        mock_response.success = True
        self.mock_stub.PutFile.return_value = mock_response
        
        result = self.mf.put("file-uuid", "test content")
        
        self.assertIsInstance(result, float)  # Should return a timestamp
        # Verify the string was converted to bytes
        call_args = self.mock_stub.PutFile.call_args[0][0]
        self.assertEqual(call_args.data, b"test content")

    def test_put_failure(self):
        """Test file upload failure."""
        # Mock the gRPC response
        mock_response = Mock()
        mock_response.success = False
        self.mock_stub.PutFile.return_value = mock_response
        
        result = self.mf.put("file-uuid", b"test content")
        
        self.assertFalse(result)

    def test_get_success(self):
        """Test successful file download."""
        # Mock the revisions response
        with patch.object(self.mf, 'revisions', return_value=[{'version': '1234567890.0'}]):
            # Mock the gRPC response
            mock_response = Mock()
            mock_response.success = True
            mock_response.data = b"test content"
            self.mock_stub.GetVersion.return_value = mock_response
            
            result = self.mf.get("file-uuid")
            
            self.assertIsInstance(result, io.BytesIO)
            self.assertEqual(result.getvalue(), b"test content")

    def test_get_failure_no_revisions(self):
        """Test file download when no revisions exist."""
        # Mock the revisions response to return empty list
        with patch.object(self.mf, 'revisions', return_value=[]):
            result = self.mf.get("file-uuid")
            
            self.assertFalse(result)

    def test_get_failure_grpc_error(self):
        """Test file download with gRPC error."""
        # Mock the revisions response
        with patch.object(self.mf, 'revisions', return_value=[{'version': '1234567890.0'}]):
            self.mock_stub.GetVersion.side_effect = grpc.RpcError("Connection failed")
            
            result = self.mf.get("file-uuid")
            
            self.assertFalse(result)

    def test_entity_exists_true(self):
        """Test entity exists returns True."""
        # Mock the gRPC response
        mock_response = Mock()
        mock_response.exists = True
        self.mock_stub.Exists.return_value = mock_response
        
        result = self.mf.entity_exists("entity-uuid")
        
        self.assertTrue(result)

    def test_entity_exists_false(self):
        """Test entity exists returns False."""
        # Mock the gRPC response
        mock_response = Mock()
        mock_response.exists = False
        self.mock_stub.Exists.return_value = mock_response
        
        result = self.mf.entity_exists("entity-uuid")
        
        self.assertFalse(result)

    def test_entity_exists_grpc_error(self):
        """Test entity exists with gRPC error."""
        self.mock_stub.Exists.side_effect = grpc.RpcError("Connection failed")
        
        result = self.mf.entity_exists("entity-uuid")
        
        self.assertFalse(result)

    def test_is_dir_true(self):
        """Test is_dir returns True for directory."""
        # Mock the gRPC response
        mock_info = Mock()
        mock_info.type = 1  # FileType.DIRECTORY
        mock_response = Mock()
        mock_response.success = True
        mock_response.info = mock_info
        self.mock_stub.Stat.return_value = mock_response
        
        result = self.mf.is_dir("dir-uuid")
        
        self.assertTrue(result)

    def test_is_dir_false(self):
        """Test is_dir returns False for file."""
        # Mock the gRPC response
        mock_info = Mock()
        mock_info.type = 0  # FileType.REGULAR_FILE
        mock_response = Mock()
        mock_response.success = True
        mock_response.info = mock_info
        self.mock_stub.Stat.return_value = mock_response
        
        result = self.mf.is_dir("file-uuid")
        
        self.assertFalse(result)

    def test_is_dir_grpc_error(self):
        """Test is_dir with gRPC error."""
        self.mock_stub.Stat.side_effect = grpc.RpcError("Connection failed")
        
        result = self.mf.is_dir("entity-uuid")
        
        self.assertFalse(result)

    def test_revisions_success(self):
        """Test successful retrieval of revisions."""
        # Mock the gRPC response
        mock_response = Mock()
        mock_response.success = True
        mock_response.versions = ["1234567890.0", "1234567891.0"]
        self.mock_stub.ListVersions.return_value = mock_response
        
        result = self.mf.revisions("file-uuid")
        
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]['version'], "1234567890.0")
        self.assertEqual(result[1]['version'], "1234567891.0")

    def test_revisions_failure(self):
        """Test revisions retrieval failure."""
        # Mock the gRPC response
        mock_response = Mock()
        mock_response.success = False
        self.mock_stub.ListVersions.return_value = mock_response
        
        result = self.mf.revisions("file-uuid")
        
        self.assertEqual(result, [])

    def test_revisions_grpc_error(self):
        """Test revisions retrieval with gRPC error."""
        self.mock_stub.ListVersions.side_effect = grpc.RpcError("Connection failed")
        
        result = self.mf.revisions("file-uuid")
        
        self.assertEqual(result, [])

    def test_remove_file_success(self):
        """Test successful file removal."""
        # Mock the Stat response to indicate it's a file
        mock_info = Mock()
        mock_info.type = 0  # FileType.REGULAR_FILE
        stat_response = Mock()
        stat_response.success = True
        stat_response.info = mock_info
        self.mock_stub.Stat.return_value = stat_response
        
        # Mock the RemoveFile response
        remove_response = Mock()
        remove_response.success = True
        self.mock_stub.RemoveFile.return_value = remove_response
        
        result = self.mf.remove("file-uuid")
        
        self.assertTrue(result)
        self.mock_stub.RemoveFile.assert_called_once()

    def test_remove_directory_success(self):
        """Test successful directory removal."""
        # Mock the Stat response to indicate it's a directory
        mock_info = Mock()
        mock_info.type = 1  # FileType.DIRECTORY
        stat_response = Mock()
        stat_response.success = True
        stat_response.info = mock_info
        self.mock_stub.Stat.return_value = stat_response
        
        # Mock the RemoveDirectory response
        remove_response = Mock()
        remove_response.success = True
        self.mock_stub.RemoveDirectory.return_value = remove_response
        
        result = self.mf.remove("dir-uuid")
        
        self.assertTrue(result)
        self.mock_stub.RemoveDirectory.assert_called_once()

    def test_remove_failure(self):
        """Test removal failure."""
        # Mock the Stat response to indicate it's a file
        mock_info = Mock()
        mock_info.type = 0  # FileType.REGULAR_FILE
        stat_response = Mock()
        stat_response.success = True
        stat_response.info = mock_info
        self.mock_stub.Stat.return_value = stat_response
        
        # Mock the RemoveFile response as failure
        remove_response = Mock()
        remove_response.success = False
        self.mock_stub.RemoveFile.return_value = remove_response
        
        result = self.mf.remove("file-uuid")
        
        self.assertFalse(result)

    def test_remove_grpc_error(self):
        """Test removal with gRPC error."""
        self.mock_stub.Stat.side_effect = grpc.RpcError("Connection failed")
        
        result = self.mf.remove("entity-uuid")
        
        self.assertFalse(result)

    def test_rename_success(self):
        """Test successful rename operation."""
        # Mock the gRPC response
        mock_response = Mock()
        mock_response.success = True
        self.mock_stub.Rename.return_value = mock_response
        
        result = self.mf.rename("entity-uuid", "new_name")
        
        self.assertTrue(result)
        self.mock_stub.Rename.assert_called_once()

    def test_rename_failure(self):
        """Test rename operation failure."""
        # Mock the gRPC response
        mock_response = Mock()
        mock_response.success = False
        self.mock_stub.Rename.return_value = mock_response
        
        result = self.mf.rename("entity-uuid", "new_name")
        
        self.assertFalse(result)

    def test_set_metadata_value_success(self):
        """Test successful metadata setting."""
        # Mock the gRPC response
        mock_response = Mock()
        mock_response.success = True
        self.mock_stub.SetMetadata.return_value = mock_response
        
        result = self.mf.set_metadata_value("entity-uuid", "key", "value")
        
        self.assertTrue(result)

    def test_set_metadata_value_failure(self):
        """Test metadata setting failure."""
        # Mock the gRPC response
        mock_response = Mock()
        mock_response.success = False
        self.mock_stub.SetMetadata.return_value = mock_response
        
        result = self.mf.set_metadata_value("entity-uuid", "key", "value")
        
        self.assertFalse(result)

    def test_get_metadata_value_success(self):
        """Test successful metadata retrieval."""
        # Mock the gRPC response
        mock_response = Mock()
        mock_response.success = True
        mock_response.value = "value"
        self.mock_stub.GetMetadata.return_value = mock_response
        
        result = self.mf.get_metadata_value("entity-uuid", "key")
        
        self.assertEqual(result, "value")

    def test_get_metadata_value_failure(self):
        """Test metadata retrieval failure."""
        # Mock the gRPC response
        mock_response = Mock()
        mock_response.success = False
        self.mock_stub.GetMetadata.return_value = mock_response
        
        result = self.mf.get_metadata_value("entity-uuid", "key")
        
        self.assertIsNone(result)

    def test_context_manager(self):
        """Test the context manager functionality."""
        with patch('fileengine.client.grpc.insecure_channel', return_value=self.mock_channel):
            with patch('fileengine.client.fileservice_pb2_grpc.FileServiceStub', return_value=self.mock_stub):
                with ManagedFiles(user_name="test_user", server_address="localhost:50051") as mf:
                    self.assertIsNotNone(mf)
                    # Perform an operation to ensure it works
                    self.mock_stub.Exists.return_value = Mock(exists=True)
                    result = mf.entity_exists("test-uuid")
                    self.assertTrue(result)
                
                # Verify close was called
                self.mock_channel.close.assert_called_once()


if __name__ == '__main__':
    unittest.main()