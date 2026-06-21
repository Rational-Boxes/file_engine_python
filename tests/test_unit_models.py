"""Offline unit tests — Pydantic models, auth-context building, and the
permission/effect coercion helpers. These do not require a running server."""
import unittest
from datetime import datetime

from fileengine import FileType, FileInfo, DirectoryEntry, Revision, StorageUsage, ManagedFiles
from fileengine import fileservice_pb2
from fileengine.client import _coerce_permission, _coerce_effect


class TestModels(unittest.TestCase):
    def test_fileinfo_defaults_and_is_dir(self):
        fi = FileInfo(uid="u", name="n", type=FileType.DIRECTORY)
        self.assertTrue(fi.is_dir)
        self.assertEqual(fi.parent_uid, "")          # default
        self.assertEqual(fi.size, 0)                 # default
        f2 = FileInfo(uid="u", name="n", type=FileType.REGULAR_FILE)
        self.assertFalse(f2.is_dir)

    def test_directory_entry_is_container(self):
        de = DirectoryEntry(uid="u", name="d", type=FileType.DIRECTORY)
        self.assertTrue(de.is_container)
        self.assertFalse(DirectoryEntry(uid="u", name="f", type=FileType.REGULAR_FILE).is_container)

    def test_models_are_pydantic(self):
        import pydantic
        for m in (FileInfo, DirectoryEntry, Revision, StorageUsage):
            self.assertTrue(issubclass(m, pydantic.BaseModel))

    def test_storage_usage_validation(self):
        su = StorageUsage(total_space=10, used_space=4, available_space=6, usage_percentage=40.0)
        self.assertEqual(su.used_space, 4)
        with self.assertRaises(Exception):
            StorageUsage(total_space="notanint", used_space=0, available_space=0, usage_percentage=0.0)


class TestCoercion(unittest.TestCase):
    def test_permission_letters_names_ints(self):
        self.assertEqual(_coerce_permission("r"), fileservice_pb2.READ)
        self.assertEqual(_coerce_permission("READ"), fileservice_pb2.READ)
        self.assertEqual(_coerce_permission("m"), fileservice_pb2.MANAGE_ACL)
        self.assertEqual(_coerce_permission(fileservice_pb2.WRITE), fileservice_pb2.WRITE)

    def test_effect(self):
        self.assertEqual(_coerce_effect("deny"), fileservice_pb2.DENY)
        self.assertEqual(_coerce_effect("allow"), fileservice_pb2.ALLOW)
        self.assertEqual(_coerce_effect(fileservice_pb2.DENY), fileservice_pb2.DENY)


class TestAuthContext(unittest.TestCase):
    def test_claims_list_to_map(self):
        mf = ManagedFiles(user_name="alice", user_roles=["system_admin"],
                          user_claims=["read", ("dept", "eng"), {"x": "y"}], tenant="t")
        auth = mf._create_auth_context()
        self.assertEqual(auth.user, "alice")
        self.assertEqual(list(auth.roles), ["system_admin"])
        self.assertEqual(auth.tenant, "t")
        self.assertEqual(auth.claims["read"], "read")
        self.assertEqual(auth.claims["dept"], "eng")
        self.assertEqual(auth.claims["x"], "y")
        mf.close()

    def test_per_call_overrides(self):
        mf = ManagedFiles(user_name="alice", user_roles=["users"])
        auth = mf._create_auth_context(user="bob", roles=["system_admin"], tenant="o")
        self.assertEqual(auth.user, "bob")
        self.assertEqual(list(auth.roles), ["system_admin"])
        self.assertEqual(auth.tenant, "o")
        mf.close()


if __name__ == '__main__':
    unittest.main()
