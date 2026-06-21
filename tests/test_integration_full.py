"""
Full integration test for the FileEngine Python client against a RUNNING
server (default localhost:50051). Mirrors the C++ cli_full_integration suite:
filesystem ops, the copy/move-into-own-subtree guard, root-UUID aliasing,
versioning, metadata (incl. versioned), ACL (r/w/x/d/m + ALLOW/DENY + role
principals), role management, and diagnostics.

Administration is role-based: the admin client authenticates with the
``system_admin`` role (not a hardcoded root user).

Skips itself automatically if no server is reachable.
"""
import os
import time
import unittest

from fileengine import ManagedFiles, FileType, ZERO_UID

SERVER = os.environ.get("FILEENGINE_SERVER", "localhost:50051")


def _server_up() -> bool:
    try:
        mf = ManagedFiles(user_name="probe", user_roles=["system_admin"], server_address=SERVER)
        ok = mf.get_storage_usage() is not None
        mf.close()
        return ok
    except Exception:
        return False


@unittest.skipUnless(_server_up(), f"no FileEngine server reachable at {SERVER}")
class TestFullIntegration(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Admin identity is the system_admin *role* (username is arbitrary).
        cls.admin = ManagedFiles(user_name="admin_user", user_roles=["system_admin"],
                                 server_address=SERVER, tenant="default")
        cls.suffix = f"{int(time.time())}_{os.getpid()}"
        cls.ws = cls.admin.mkdir("", f"pyit_{cls.suffix}")
        assert cls.ws, "could not create workspace (is system_admin honored?)"

    @classmethod
    def tearDownClass(cls):
        try:
            cls.admin.remove(cls.ws)
        finally:
            cls.admin.close()

    def _mkfile(self, name, content=b"data"):
        f = self.admin.touch(self.ws, name)
        self.assertTrue(f)
        self.assertIsNot(self.admin.put(f, content), False)
        return f

    # -- connectivity / root aliasing -------------------------------------
    def test_00_storage_usage(self):
        su = self.admin.get_storage_usage()
        self.assertIsNotNone(su)
        self.assertGreater(su.total_space, 0)

    def test_01_root_aliases(self):
        a = self.admin.dir("")
        b = self.admin.dir(ZERO_UID)
        self.assertIsInstance(a, list)
        self.assertIsInstance(b, list)
        self.assertEqual(len(a), len(b))
        d = self.admin.mkdir(ZERO_UID, f"pyit_zero_{self.suffix}")
        self.assertTrue(d)
        names = [e.name for e in self.admin.dir("")]
        self.assertIn(f"pyit_zero_{self.suffix}", names)
        self.admin.remove(d)

    # -- filesystem -------------------------------------------------------
    def test_10_mkdir_touch_stat_exists(self):
        d = self.admin.mkdir(self.ws, "sub")
        self.assertTrue(d)
        f = self._mkfile("a.txt", b"hello")
        self.assertTrue(self.admin.entity_exists(f))
        self.assertFalse(self.admin.entity_exists("deadbeef-0000-0000-0000-000000000000"))
        self.assertTrue(self.admin.stat(d).is_dir)
        self.assertFalse(self.admin.stat(f).is_dir)
        self.assertEqual(self.admin.get_parent(f), self.ws)

    def test_11_get_roundtrip(self):
        f = self._mkfile("rt.txt", b"roundtrip-content")
        self.assertEqual(self.admin.get(f).getvalue(), b"roundtrip-content")

    def test_12_rename_move_copy(self):
        f = self._mkfile("orig.txt", b"x")
        self.assertTrue(self.admin.rename(f, "renamed.txt"))
        self.assertEqual(self.admin.stat(f).name, "renamed.txt")
        sub = self.admin.mkdir(self.ws, "movedst")
        self.assertTrue(self.admin.copy(f, sub))
        self.assertIn("renamed.txt", [e.name for e in self.admin.dir(sub)])
        sub2 = self.admin.mkdir(self.ws, "movedst2")
        self.assertTrue(self.admin.move(f, sub2))
        self.assertEqual(self.admin.get_parent(f), sub2)

    def test_13_copy_move_subtree_guard(self):
        parent = self.admin.mkdir(self.ws, "guard")
        child = self.admin.mkdir(parent, "child")
        self.assertFalse(self.admin.copy(parent, child))   # would recurse -> rejected
        self.assertFalse(self.admin.move(parent, child))   # cycle -> rejected
        # server must still be alive
        self.assertIsNotNone(self.admin.get_storage_usage())

    def test_14_recursive_dir_copy(self):
        src = self.admin.mkdir(self.ws, "rcsrc")
        self._inner = self.admin.touch(src, "inner.txt")
        self.admin.put(self._inner, b"inner")
        dst = self.admin.mkdir(self.ws, "rcdst")
        self.assertTrue(self.admin.copy(src, dst))
        copied = [e for e in self.admin.dir(dst) if e.name == "rcsrc"]
        self.assertEqual(len(copied), 1)
        self.assertIn("inner.txt", [e.name for e in self.admin.dir(copied[0].uid)])

    def test_15_soft_delete_lsd_undelete(self):
        d = self.admin.mkdir(self.ws, "delbox")
        f = self.admin.touch(d, "gone.txt")
        self.admin.put(f, b"bye")
        self.assertTrue(self.admin.remove(f))
        self.assertNotIn("gone.txt", [e.name for e in self.admin.dir(d)])           # hidden
        self.assertIn("gone.txt", [e.name for e in self.admin.dir(d, show_deleted=True)])  # lsd
        self.assertTrue(self.admin.undelete_file(f))
        self.assertIn("gone.txt", [e.name for e in self.admin.dir(d)])

    # -- versioning -------------------------------------------------------
    def test_20_versions_restore_purge(self):
        f = self._mkfile("ver.txt", b"v1")
        time.sleep(1); self.admin.put(f, b"v2")
        time.sleep(1); self.admin.put(f, b"v3")
        revs = self.admin.revisions(f)
        self.assertGreaterEqual(len(revs), 3)
        self.assertEqual(self.admin.get(f, back=1).getvalue(), b"v2")  # one back
        self.assertTrue(self.admin.restore_to_version(f, revs[1].version))
        self.assertTrue(self.admin.purge_old_versions(f, 1))
        self.assertLessEqual(len(self.admin.revisions(f)), 1)

    # -- metadata ---------------------------------------------------------
    def test_30_metadata(self):
        f = self._mkfile("meta.txt", b"m")
        self.assertTrue(self.admin.set_metadata_value(f, "color", "blue"))
        self.assertEqual(self.admin.get_metadata_value(f, "color"), "blue")
        self.assertEqual(self.admin.get_metadata_values(f).get("color"), "blue")
        self.assertEqual(self.admin.get_all_metadata_for_version(f, "current").get("color"), "blue")
        self.assertEqual(self.admin.get_metadata_for_version(f, "current", "color"), "blue")
        self.assertTrue(self.admin.delete_metadata_value(f, "color"))
        self.assertIsNone(self.admin.get_metadata_value(f, "color"))

    # -- permissions / ACL ------------------------------------------------
    def test_40_permission_letters_and_deny(self):
        f = self._mkfile("acl.txt", b"a")
        for letter in ("r", "w", "x", "d", "m"):
            self.assertTrue(self.admin.grant_permission(f, "dave", letter))
            self.assertTrue(self.admin.check_permission(f, letter, user="dave", roles=[]))
        # ALLOW then DENY -> denied; revoke DENY -> allowed again
        self.admin.grant_permission(f, "erin", "r")
        self.admin.grant_permission(f, "erin", "r", effect="deny")
        self.assertFalse(self.admin.check_permission(f, "r", user="erin", roles=[]))
        self.admin.revoke_permission(f, "erin", "r", effect="deny")
        self.assertTrue(self.admin.check_permission(f, "r", user="erin", roles=[]))

    def test_41_role_based_grant(self):
        f = self._mkfile("rbac.txt", b"a")
        role = f"editors_{self.suffix}"
        self.assertTrue(self.admin.create_role(role))
        self.assertIn(role, self.admin.get_all_roles())
        self.assertTrue(self.admin.assign_user_to_role("carol", role))
        self.assertIn(role, self.admin.get_roles_for_user("carol"))
        self.assertIn("carol", self.admin.get_users_for_role(role))
        self.assertTrue(self.admin.grant_permission(f, f"role:{role}", "r"))
        self.assertTrue(self.admin.check_permission(f, "r", user="carol", roles=[role]))
        self.assertTrue(self.admin.remove_user_from_role("carol", role))
        self.assertNotIn(role, self.admin.get_roles_for_user("carol"))
        self.assertTrue(self.admin.delete_role(role))

    # -- diagnostics ------------------------------------------------------
    def test_50_trigger_sync(self):
        self.assertTrue(self.admin.trigger_sync())


if __name__ == '__main__':
    unittest.main()
