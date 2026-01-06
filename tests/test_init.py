import unittest
from fileengine import ManagedFiles, FileType, FileInfo, DirectoryEntry, FileSystemError


class TestInit(unittest.TestCase):
    """Test the __init__.py exports"""
    
    def test_managed_files_available(self):
        """Test that ManagedFiles is available from the package"""
        self.assertTrue(ManagedFiles is not None)
        self.assertTrue(isinstance(ManagedFiles, type))
    
    def test_file_type_available(self):
        """Test that FileType is available from the package"""
        self.assertTrue(FileType is not None)
        self.assertTrue(isinstance(FileType, type))
    
    def test_file_info_available(self):
        """Test that FileInfo is available from the package"""
        self.assertTrue(FileInfo is not None)
        self.assertTrue(isinstance(FileInfo, type))
    
    def test_directory_entry_available(self):
        """Test that DirectoryEntry is available from the package"""
        self.assertTrue(DirectoryEntry is not None)
        self.assertTrue(isinstance(DirectoryEntry, type))
    
    def test_file_system_error_available(self):
        """Test that FileSystemError is available from the package"""
        self.assertTrue(FileSystemError is not None)
        self.assertTrue(isinstance(FileSystemError, type))
    
    def test_version_available(self):
        """Test that version is available from the package"""
        from fileengine import __version__
        self.assertTrue(__version__ is not None)
        self.assertIsInstance(__version__, str)
        self.assertEqual(__version__, "1.0.0")
    
    def test_all_list_available(self):
        """Test that __all__ is available from the package"""
        from fileengine import __all__
        self.assertTrue(__all__ is not None)
        self.assertIsInstance(__all__, list)
        expected_exports = ["ManagedFiles", "FileType", "FileInfo", "DirectoryEntry", "FileSystemError"]
        self.assertEqual(set(__all__), set(expected_exports))


if __name__ == '__main__':
    unittest.main()