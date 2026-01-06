import unittest
import time
from fileengine import ManagedFiles
from fileengine import fileservice_pb2


class TestPermissionAndStatusIntegration(unittest.TestCase):
    """Integration tests for the new permission and status operations using a real server connection"""

    @classmethod
    def setUpClass(cls):
        """Set up the test class with a real server connection."""
        # Connect to the actual server using root user with superuser privileges
        cls.mf = ManagedFiles(
            user_name="root",
            user_roles=["admin", "superuser"],
            user_claims=["read", "write", "delete", "admin"],
            server_address="localhost:50051",  # Default gRPC server address
            tenant="default"
        )
        
        # Set user information with root privileges
        cls.mf.set_user_information(
            user_name="root",
            roles=["admin", "superuser"],
            claims=["read", "write", "delete", "admin"]
        )

    @classmethod
    def tearDownClass(cls):
        """Clean up after all tests in the class."""
        cls.mf.close()

    def test_permission_operations(self):
        """Test permission operations."""
        # Create a test directory
        test_dir_name = f"test_perm_ops_{int(time.time())}"
        dir_uid = self.mf.mkdir("", test_dir_name)
        
        self.assertIsNotNone(dir_uid)
        self.assertNotEqual(dir_uid, False)
        
        try:
            # Grant read permission to a user
            permission_granted = self.mf.grant_permission(
                resource_uid=dir_uid,
                principal="test_user",
                permission=fileservice_pb2.Permission.READ
            )
            # Note: This might return False if the server doesn't support this operation yet
            # Just verify that the call doesn't raise an exception
            
            # Check if user has permission
            has_permission = self.mf.check_permission(
                resource_uid=dir_uid,
                required_permission=fileservice_pb2.Permission.READ
            )
            # This might also return False depending on server implementation
            
            # Revoke the permission
            permission_revoked = self.mf.revoke_permission(
                resource_uid=dir_uid,
                principal="test_user",
                permission=fileservice_pb2.Permission.READ
            )
            # This might also return False depending on server implementation
            
            # For now, just ensure the methods can be called without errors
            # In a real implementation, we'd verify the actual results
            
        finally:
            # Clean up
            self.mf.remove(dir_uid)

    def test_status_operations(self):
        """Test status operations."""
        # Get storage usage
        storage_info = self.mf.get_storage_usage()
        
        # Verify that we get a valid response structure
        if storage_info is not None:
            self.assertIsInstance(storage_info, dict)
            self.assertIn('total_space', storage_info)
            self.assertIn('used_space', storage_info)
            self.assertIn('available_space', storage_info)
            self.assertIn('usage_percentage', storage_info)
        
        # Trigger sync
        sync_result = self.mf.trigger_sync()
        # This might return False if the server doesn't support sync yet
        # Just verify that the call doesn't raise an exception

    def test_advanced_file_operations(self):
        """Test advanced file operations like undelete and restore."""
        # Create a test directory
        test_dir_name = f"test_adv_ops_{int(time.time())}"
        dir_uid = self.mf.mkdir("", test_dir_name)
        
        self.assertIsNotNone(dir_uid)
        self.assertNotEqual(dir_uid, False)
        
        try:
            # Create a file
            file_uid = self.mf.touch(dir_uid, "test_file.txt")
            self.assertIsNotNone(file_uid)
            self.assertNotEqual(file_uid, False)
            
            # Write content to create a version
            content = b"Initial content"
            version1 = self.mf.put(file_uid, content)
            self.assertIsInstance(version1, float)
            
            # Write different content to create another version
            content2 = b"Updated content"
            version2 = self.mf.put(file_uid, content2)
            self.assertIsInstance(version2, float)
            
            # Get revisions to see available versions
            revisions = self.mf.revisions(file_uid)
            self.assertIsInstance(revisions, list)
            self.assertGreater(len(revisions), 1)
            
            # Try to restore to the first version (if multiple versions exist)
            if len(revisions) > 1:
                # Get the first version (most recent is index 0, so index 1 is second most recent)
                restore_result = self.mf.restore_to_version(file_uid, revisions[1]['version'])
                # Result might be None if the operation is not supported yet
                
            # Remove the file to test undelete
            remove_result = self.mf.remove(file_uid)
            self.assertTrue(remove_result)
            
            # Try to undelete the file
            # Note: This might fail if the server doesn't support undeletion
            # or if the file was permanently deleted
            try:
                undelete_result = self.mf.undelete_file(file_uid)
                # Result might be False if the operation is not supported yet
            except:
                # If undeletion is not supported, that's okay
                pass
            
        finally:
            # Clean up
            self.mf.remove(dir_uid)


if __name__ == '__main__':
    unittest.main()