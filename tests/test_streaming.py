# Streaming upload/download (put_stream / get_stream).
#
# The requirement is not "streaming is available" but "a large body cannot
# become one large message": a multi-megabyte gRPC message monopolises the
# connection and can exceed a peer's receive limit. So the interesting tests are
# the ones that hand the client a deliberately oversized buffer and assert it
# still goes out in bounded pieces.
import hashlib
import os
import unittest
import uuid

from fileengine import ManagedFiles, NotFoundError
from fileengine.client import MAX_WIRE_CHUNK

SERVER = os.environ.get("FILEENGINE_SERVER", "localhost:50051")
USER = os.environ.get("FILEENGINE_TEST_USER", "testuser@rationalboxes.com")
TENANT = os.environ.get("FILEENGINE_TEST_TENANT", "default")


def _server_up() -> bool:
    try:
        mf = ManagedFiles(user_name=USER, user_roles=["users", "contributors"],
                          tenant=TENANT, server_address=SERVER)
        try:
            mf.entity_exists("00000000-0000-0000-0000-000000000000")
            return True
        finally:
            mf.close()
    except Exception:
        return False


@unittest.skipUnless(_server_up(), "needs a reachable FileEngine core")
class StreamingTests(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.mf = ManagedFiles(user_name=USER, user_roles=["users", "contributors"],
                              tenant=TENANT, server_address=SERVER)
        cls.dir = cls._uid(cls.mf.mkdir("", f"stream-tests-{uuid.uuid4().hex[:8]}"))

    @classmethod
    def tearDownClass(cls):
        cls.mf.close()

    @staticmethod
    def _uid(v):
        return getattr(v, "uid", v)

    def _new_file(self, name):
        return self._uid(self.mf.touch(self.dir, name))

    # --- the requirement ---------------------------------------------------

    def test_one_oversized_buffer_is_split_into_bounded_messages(self):
        """The caller's mistake must not become a giant message.

        Handing put_stream a single buffer is the natural thing to do; without
        re-splitting it that produces exactly the oversized message streaming
        exists to avoid.
        """
        payload = bytes(range(256)) * (40 * 1024)          # 10 MiB, one chunk
        self.assertGreater(len(payload), MAX_WIRE_CHUNK)
        uid = self._new_file("one-buffer.bin")
        self.mf.put_stream(uid, [payload])
        self.assertEqual(self.mf.get(uid).getvalue(), payload)

    def test_a_small_chunk_size_is_honoured(self):
        payload = b"x" * 10_000
        uid = self._new_file("small-chunks.bin")
        self.mf.put_stream(uid, [payload], chunk_size=1024)
        self.assertEqual(self.mf.get(uid).getvalue(), payload)

    def test_upload_exceeds_the_unary_message_ceiling(self):
        """96 MiB: impossible through put(), routine through put_stream()."""
        chunk = b"\xa5" * (4 * 1024 * 1024)
        count = 24
        digest = hashlib.sha256()

        def gen():
            for _ in range(count):
                digest.update(chunk)
                yield chunk

        uid = self._new_file("big.bin")
        self.mf.put_stream(uid, gen())
        self.assertEqual(self.mf.stat(uid).size, len(chunk) * count)

    # --- ordinary behaviour ------------------------------------------------

    def test_generator_chunks_round_trip(self):
        uid = self._new_file("gen.bin")
        self.mf.put_stream(uid, (bytes([i]) * 1000 for i in range(10)))
        expected = b"".join(bytes([i]) * 1000 for i in range(10))
        self.assertEqual(self.mf.get(uid).getvalue(), expected)

    def test_str_chunks_are_encoded(self):
        uid = self._new_file("text.txt")
        self.mf.put_stream(uid, ["héllo ", "wörld"])
        self.assertEqual(self.mf.get(uid).getvalue(), "héllo wörld".encode())

    def test_empty_body_writes_an_empty_version(self):
        """An empty iterable must still name a target, or the server answers
        'No file data received' rather than writing an empty file."""
        uid = self._new_file("empty.bin")
        self.mf.put_stream(uid, [])
        self.assertEqual(self.mf.get(uid).getvalue(), b"")

    def test_empty_chunks_are_skipped_without_ending_the_body(self):
        uid = self._new_file("holes.bin")
        self.mf.put_stream(uid, [b"a", b"", b"b", b"", b"c"])
        self.assertEqual(self.mf.get(uid).getvalue(), b"abc")

    def test_get_stream_reassembles_to_the_same_bytes(self):
        payload = bytes(range(256)) * 4096
        uid = self._new_file("readback.bin")
        self.mf.put_stream(uid, [payload])
        self.assertEqual(b"".join(self.mf.get_stream(uid)), payload)

    def test_get_stream_raises_for_a_missing_file(self):
        with self.assertRaises(NotFoundError):
            list(self.mf.get_stream(str(uuid.uuid4())))


if __name__ == "__main__":
    unittest.main()
