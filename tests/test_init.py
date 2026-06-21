import unittest
from fileengine import (
    ManagedFiles, FileType, FileInfo, DirectoryEntry, Revision, StorageUsage,
    FileSystemError, ROOT_UID, ZERO_UID,
)


class TestInit(unittest.TestCase):
    """Test the __init__.py exports."""

    def test_managed_files_available(self):
        self.assertTrue(isinstance(ManagedFiles, type))

    def test_types_available(self):
        for t in (FileType, FileInfo, DirectoryEntry, Revision, StorageUsage, FileSystemError):
            self.assertTrue(isinstance(t, type))

    def test_root_aliases(self):
        self.assertEqual(ROOT_UID, "")
        self.assertEqual(ZERO_UID, "00000000-0000-0000-0000-000000000000")

    def test_version_available(self):
        from fileengine import __version__
        self.assertEqual(__version__, "1.0.0")

    def test_all_list(self):
        from fileengine import __all__
        expected = ["ManagedFiles", "FileType", "FileInfo", "DirectoryEntry",
                    "Revision", "StorageUsage", "FileSystemError", "ROOT_UID", "ZERO_UID"]
        self.assertEqual(set(__all__), set(expected))


if __name__ == '__main__':
    unittest.main()
