import unittest
from unittest.mock import Mock, patch
import grpc

from fileengine.client import FileType, FileInfo, DirectoryEntry, FileSystemError


class TestFileType(unittest.TestCase):
    """Test the FileType class"""
    
    def test_file_type_constants(self):
        """Test that FileType constants are properly defined"""
        # These values should match the protobuf definitions
        self.assertEqual(FileType.REGULAR_FILE, 0)
        self.assertEqual(FileType.DIRECTORY, 1)
        self.assertEqual(FileType.SYMLINK, 2)


class TestFileInfo(unittest.TestCase):
    """Test the FileInfo dataclass"""
    
    def test_file_info_creation(self):
        """Test creating a FileInfo instance"""
        info = FileInfo(
            uid="test-uid",
            path="/test/path",
            name="test.txt",
            type=0,  # REGULAR_FILE
            size=1024,
            created_at=None,
            modified_at=None,
            version="1234567890.0",
            owner="test_user",
            permissions=0o644
        )
        
        self.assertEqual(info.uid, "test-uid")
        self.assertEqual(info.path, "/test/path")
        self.assertEqual(info.name, "test.txt")
        self.assertEqual(info.type, 0)
        self.assertEqual(info.size, 1024)
        self.assertEqual(info.version, "1234567890.0")
        self.assertEqual(info.owner, "test_user")
        self.assertEqual(info.permissions, 0o644)


class TestDirectoryEntry(unittest.TestCase):
    """Test the DirectoryEntry dataclass"""
    
    def test_directory_entry_creation(self):
        """Test creating a DirectoryEntry instance"""
        entry = DirectoryEntry(
            uid="test-uid",
            name="test_dir",
            type=1,  # DIRECTORY
            size=4096
        )
        
        self.assertEqual(entry.uid, "test-uid")
        self.assertEqual(entry.name, "test_dir")
        self.assertEqual(entry.type, 1)
        self.assertEqual(entry.size, 4096)


class TestFileSystemError(unittest.TestCase):
    """Test the FileSystemError exception"""
    
    def test_file_system_error_is_exception(self):
        """Test that FileSystemError is an exception"""
        self.assertTrue(issubclass(FileSystemError, Exception))
        
        # Test raising and catching the exception
        with self.assertRaises(FileSystemError):
            raise FileSystemError("Test error message")
    
    def test_file_system_error_message(self):
        """Test that FileSystemError preserves error messages"""
        try:
            raise FileSystemError("Custom error message")
        except FileSystemError as e:
            self.assertEqual(str(e), "Custom error message")


if __name__ == '__main__':
    unittest.main()