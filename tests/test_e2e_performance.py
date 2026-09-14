"""End-to-end: the performance work, over real gRPC against a live core.

Skips when no core is reachable, like the other integration tests here.

These cover what the unit and DB-level tests cannot: the gRPC handlers, the ACL
filter that runs in the HANDLER rather than the query, the accountability
emission, and the SDK's mapping of the wire types. Every bug this found was in
that gap — a permission_mask in the wrong value space, and an audit INSERT that
poisoned its own transaction.

Run against a core with FILEENGINE_SERVICE_AUTH_REQUIRED=false:
    FILEENGINE_GRPC_ADDR=127.0.0.1:50051 python -m pytest tests/test_e2e_performance.py
"""
import os
import subprocess
import sys
import time
import unittest
import uuid

from fileengine.client import ManagedFiles
from fileengine import PermissionBit as PB

ADDR = os.environ.get("FILEENGINE_GRPC_ADDR", "127.0.0.1:50051")
READ_BIT, WRITE_BIT = PB.Value("PB_READ"), PB.Value("PB_WRITE")


def _core_reachable() -> bool:
    import socket
    host, _, port = ADDR.partition(":")
    try:
        with socket.create_connection((host, int(port or 50051)), timeout=1):
            return True
    except OSError:
        return False


@unittest.skipUnless(_core_reachable(), f"no FileEngine core reachable at {ADDR}")
class PerformanceE2E(unittest.TestCase):
    """One tenant, one tree, exercised through every new path."""

    @classmethod
    def setUpClass(cls):
        cls.tenant = "e2eperf_" + uuid.uuid4().hex[:8]
        cls.admin = cls._client("alice", ["system_admin"])
        cls.root = cls.admin.mkdir("", "root")
        cls.docs = cls.admin.mkdir(cls.root, "docs")
        cls.other = cls.admin.mkdir(cls.root, "other")
        cls.a = cls.admin.touch(cls.docs, "a.txt");  cls.admin.put(cls.a, b"alpha")
        time.sleep(1.1)
        cls.b = cls.admin.touch(cls.docs, "b.txt");  cls.admin.put(cls.b, b"bravo")
        time.sleep(1.1)
        cls.c = cls.admin.touch(cls.other, "c.txt"); cls.admin.put(cls.c, b"charlie")

    @classmethod
    def _client(cls, user, roles=None):
        return ManagedFiles(server_address=ADDR, user_name=user,
                            user_roles=roles or [], tenant=cls.tenant)

    # -- the subtree grant ------------------------------------------------
    def test_01_recursive_grant_applies_a_mask_everywhere(self):
        """One call, one mask, every descendant.

        Two value spaces meet here and mixing them is the trap PermissionBit
        exists to close: `permission` is a proto ORDINAL, `permission_mask` is
        BITS. Passing bits to the former silently grants the wrong thing.
        """
        self.admin.grant_permission(self.root, "bob", "r", recursive=True,
                                    permission_mask=READ_BIT | WRITE_BIT)
        bob = self._client("bob")
        self.assertTrue(bob.check_permission(self.a, "r"), "a descendant file got READ")
        self.assertTrue(bob.check_permission(self.other, "r"), "a sibling folder got it")
        self.assertTrue(bob.check_permission(self.a, "w"), "the mask applied both bits")

    def test_02_recursive_grant_is_one_record(self):
        """However many nodes it reached — see the audit decision in
        PROPOSAL_subtree_acl_apply.md."""
        before = _record_count(self.tenant)
        self.admin.grant_permission(self.docs, "carol", "r", recursive=True,
                                    permission_mask=READ_BIT)
        after = _record_count(self.tenant)
        if before is None:
            self.skipTest("no psql available to inspect the chain")
        self.assertEqual(after - before, 1, "exactly one accountability record")
        self.assertEqual(_last_action(self.tenant), "acl.grant.subtree")

    def test_03_deny_is_honoured_by_the_handler_filter(self):
        """The security-relevant one.

        This system is read-by-default, so an absent grant proves nothing — a
        user with no rules can read everything. An explicit DENY is what must be
        honoured, and it must be honoured by the handler's filter, because the
        SQL underneath knows nothing about ACLs.
        """
        mallory = self._client("mallory")
        self.assertEqual(len(mallory.list_recent_files(limit=10)["entries"]), 3,
                         "read-by-default: she starts able to see everything")
        self.admin.grant_permission(self.root, "mallory", "r", effect="deny",
                                    recursive=True, permission_mask=READ_BIT)
        res = mallory.list_recent_files(limit=10)
        self.assertEqual(len(res["entries"]), 0, "the DENY reaches the feed")
        self.assertGreater(res["examined"], 0,
                           "the query still examined rows — the FILTER dropped them")

    # -- recent files -----------------------------------------------------
    def test_04_recent_files_newest_first_over_the_wire(self):
        res = self.admin.list_recent_files(limit=10)
        names = [e["name"] for e in res["entries"]]
        self.assertEqual(names[0], "c.txt", f"newest first (got {names})")
        self.assertTrue(all(e["version"] for e in res["entries"]),
                        "each entry carries the version that made it recent")
        self.assertEqual(res["entries"][0]["version_count"], 1,
                         "version_count is what distinguishes created from updated")
        self.assertGreaterEqual(res["examined"], len(res["entries"]))

    # -- folder mtime -----------------------------------------------------
    def test_05_folder_mtime_comes_from_the_newest_descendant(self):
        st_docs = self.admin.stat(self.docs)
        st_b = self.admin.stat(self.b)
        self.assertLessEqual(
            abs(int(st_docs.modified_at.timestamp()) - int(st_b.modified_at.timestamp())), 1,
            "a folder reports its newest descendant's mtime")
        st_root = self.admin.stat(self.root)
        st_c = self.admin.stat(self.c)
        self.assertLessEqual(
            abs(int(st_root.modified_at.timestamp()) - int(st_c.modified_at.timestamp())), 1,
            "and it propagates to the grandparent")


def _psql(tenant, sql):
    """Best-effort chain inspection; returns None when psql is unavailable."""
    container = os.environ.get("FILEENGINE_PG_CONTAINER", "audit-test-pg")
    try:
        out = subprocess.run(
            ["podman", "exec", "-e", "PGPASSWORD=test", container,
             "psql", "-U", "test", "-d", "fileengine", "-tAc", sql],
            capture_output=True, text=True, timeout=20)
        return out.stdout.strip() if out.returncode == 0 else None
    except Exception:
        return None


def _record_count(tenant):
    v = _psql(tenant, f"select count(*) from tenant_{tenant}.accountability_record")
    return int(v) if v and v.isdigit() else None


def _last_action(tenant):
    return _psql(tenant, f"select action from tenant_{tenant}.accountability_record "
                         f"order by seq desc limit 1")


if __name__ == "__main__":
    unittest.main()
