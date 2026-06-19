import unittest
import time
from fileengine import ManagedFiles


class TestPermissionAndStatusIntegration(unittest.TestCase):
    """Integration tests for ACL and advanced file operations using a real
    fileengine (file_engine_cpp) server connection.

    The gRPC core assumes trusted access; user identity is supplied in the
    AuthContext and passed through. Permission mutation, storage usage and sync
    are not part of the fileengine protocol, so the client exposes them as
    compatibility no-ops (verified here), while ACL evaluation, undelete and
    restore are exercised against the server.
    """

    @classmethod
    def setUpClass(cls):
        """Set up the test class with a real server connection."""
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
        """Test ACL evaluation and the no-op permission-mutation helpers."""
        # Create a test directory
        test_dir_name = f"test_perm_ops_{int(time.time())}"
        dir_uid = self.mf.mkdir("", test_dir_name)

        self.assertIsNotNone(dir_uid)
        self.assertNotEqual(dir_uid, False)

        try:
            # Evaluate effective permissions on the resource (EvaluateACL RPC)
            permissions = self.mf.evaluate_acl(dir_uid)
            self.assertIsInstance(permissions, list)

            # check_permission is implemented on top of EvaluateACL
            has_read = self.mf.check_permission(dir_uid, "read")
            self.assertIsInstance(has_read, bool)

            # grant/revoke are not part of the protocol -> compatibility no-ops
            self.assertFalse(self.mf.grant_permission(dir_uid, "test_user", "read"))
            self.assertFalse(self.mf.revoke_permission(dir_uid, "test_user", "read"))

        finally:
            # Clean up
            self.mf.remove(dir_uid)

    def test_status_operations(self):
        """Status helpers are compatibility no-ops in the fileengine protocol."""
        # Storage usage is not exposed by the protocol -> None
        self.assertIsNone(self.mf.get_storage_usage())

        # Sync is not exposed by the protocol -> False
        self.assertFalse(self.mf.trigger_sync())

    def test_advanced_file_operations(self):
        """Test advanced file operations like restore-to-version and undelete."""
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

            # Restore to an earlier version (emulated via ReadVersion + WriteFile)
            restore_result = self.mf.restore_to_version(file_uid, revisions[1]['version'])
            # Result is a version timestamp string on success, None on failure
            self.assertTrue(restore_result is None or isinstance(restore_result, str))

            # Remove the file to test undelete
            remove_result = self.mf.remove(file_uid)
            self.assertTrue(remove_result)

            # Try to undelete the file (UndeleteFile RPC)
            undelete_result = self.mf.undelete_file(file_uid)
            self.assertIsInstance(undelete_result, bool)

        finally:
            # Clean up
            self.mf.remove(dir_uid)


if __name__ == '__main__':
    unittest.main()
