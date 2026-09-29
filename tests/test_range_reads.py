# Byte-range reads (storage_pipeline.md SR-10 – SR-14).
#
# These run WITHOUT a server, against a fake stub, and that is deliberate. The
# other suites in here gate on `_server_up()` and skip when there is no core —
# which is honest for an integration test and useless for a contract like this
# one, where the thing worth checking is what the client PUTS ON THE REQUEST and
# what it does with the reply. A skipped test proves nothing, and the whole
# point of adding a range option is that callers can rely on it.
#
# So: assert the wire fields directly, and assert the two properties a caller
# actually depends on — that an existing call is unchanged, and that the range
# metadata is read from the frame that carries it.
import unittest

from fileengine import ManagedFiles
from fileengine.client import RangeInfo, RangeResult
from fileengine.exceptions import InvalidRequestError


class _Frame:
    """One StreamFileDownload response."""

    def __init__(self, data=b"", success=True, total_size=0, range_start=0,
                 range_length=0, ranged=False, range_method="", error=""):
        self.data = data
        self.success = success
        self.error = error
        self.total_size = total_size
        self.range_start = range_start
        self.range_length = range_length
        self.ranged = ranged
        self.range_method = range_method


class _FakeStub:
    """Records the requests it is given and replays canned frames."""

    def __init__(self, frames=None):
        self.requests = []
        self.frames = frames if frames is not None else [_Frame(data=b"whole")]
        self.get_version_calls = 0

    def StreamFileDownload(self, request):
        self.requests.append(request)
        return iter(self.frames)

    def GetVersion(self, request):
        self.get_version_calls += 1
        self.requests.append(request)
        return _Frame(data=b"unary")

    def ListVersions(self, request):
        self.requests.append(request)
        return _Frame()


def _client(stub):
    """A ManagedFiles wired to a fake stub, with no channel and no server."""
    mf = object.__new__(ManagedFiles)
    mf.stub = stub
    mf.user = "tester@rationalboxes.com"
    mf.tenant = "default"
    mf.roles = ["users"]
    mf.claims = []
    mf.source_addr = ""
    return mf


class RangeRequestFields(unittest.TestCase):
    """What ends up on the wire."""

    def test_existing_call_still_asks_for_the_whole_file(self):
        # SR-21: proto3 defaults mean an untouched caller sends offset=0,
        # length=0, which the server reads as "everything". If this ever fails,
        # adding the option broke every existing reader.
        stub = _FakeStub()
        _client(stub).get("uid-1")
        self.assertEqual(stub.requests[0].offset, 0)
        self.assertEqual(stub.requests[0].length, 0)

    def test_existing_positional_arguments_keep_their_meaning(self):
        # The new parameters were APPENDED. A caller passing the old positional
        # arguments must still be passing them to the same parameters.
        stub = _FakeStub()
        mf = _client(stub)
        # NOTE the list(): get_stream is a generator, so a bare call never runs
        # its body and records no request at all. An earlier version of this
        # test omitted it and was asserting against the NEXT call's request.
        list(mf.get_stream("uid-1", "20260929_120000.000", "someone@example.com",
                           "other-tenant", ["admins"], []))
        req = stub.requests[0]
        self.assertEqual(req.version_timestamp, "20260929_120000.000")
        self.assertEqual(req.auth.user, "someone@example.com")
        self.assertEqual(req.auth.tenant, "other-tenant")
        self.assertEqual(req.offset, 0)
        self.assertEqual(req.length, 0)

    def test_get_sends_the_range(self):
        stub = _FakeStub()
        _client(stub).get("uid-1", offset=5, length=10)
        self.assertEqual(stub.requests[0].offset, 5)
        self.assertEqual(stub.requests[0].length, 10)

    def test_get_stream_sends_the_range(self):
        stub = _FakeStub()
        list(_client(stub).get_stream("uid-1", offset=7, length=3))
        self.assertEqual(stub.requests[0].offset, 7)
        self.assertEqual(stub.requests[0].length, 3)

    def test_length_zero_means_to_the_end(self):
        stub = _FakeStub()
        _client(stub).get("uid-1", offset=64)
        self.assertEqual(stub.requests[0].offset, 64)
        self.assertEqual(stub.requests[0].length, 0)


class RangeValidation(unittest.TestCase):
    """Bad input is refused here, not after a round-trip."""

    def test_negative_offset_is_refused_without_an_rpc(self):
        stub = _FakeStub()
        with self.assertRaises(InvalidRequestError):
            _client(stub).get("uid-1", offset=-1)
        self.assertEqual(stub.requests, [], "no RPC should have been sent")

    def test_negative_length_is_refused_without_an_rpc(self):
        stub = _FakeStub()
        with self.assertRaises(InvalidRequestError):
            list(_client(stub).get_stream("uid-1", length=-5))
        self.assertEqual(stub.requests, [], "no RPC should have been sent")

    def test_a_range_past_the_end_is_left_to_the_server(self):
        # Deliberately NOT refused client-side: whether that is empty or an
        # error depends on the version's size, which this side does not know.
        stub = _FakeStub(frames=[_Frame(data=b"", total_size=10, ranged=True)])
        r = _client(stub).get_range("uid-1", offset=1_000_000, length=10)
        self.assertEqual(r.data, b"")
        self.assertEqual(stub.requests[0].offset, 1_000_000)


class RangeMetadata(unittest.TestCase):
    """SR-12: the metadata rides the first frame and is zero on the rest."""

    def _frames(self):
        return [
            _Frame(data=b"abc", total_size=100, range_start=5,
                   range_length=9, ranged=True, range_method="seek"),
            _Frame(data=b"def"),   # zeros everywhere — must not overwrite
            _Frame(data=b"ghi"),
        ]

    def test_get_range_reads_the_first_frame_and_keeps_it(self):
        r = _client(_FakeStub(self._frames())).get_range("uid-1", 5, 9)
        self.assertIsInstance(r, RangeResult)
        self.assertEqual(r.data, b"abcdefghi")
        self.assertEqual(r.info.total_size, 100)
        self.assertEqual(r.info.range_start, 5)
        self.assertEqual(r.info.range_length, 9)
        self.assertTrue(r.info.ranged)
        self.assertEqual(r.info.range_method, "seek")

    def test_later_frames_do_not_clobber_the_metadata(self):
        # The failure this guards against is reading the metadata off every
        # frame: the last one is all zeros, so total_size would come back 0 and
        # a door would answer Content-Range with a length of nothing.
        r = _client(_FakeStub(self._frames())).get_range("uid-1", 5, 9)
        self.assertNotEqual(r.info.total_size, 0)

    def test_range_stream_yields_stable_info_with_each_chunk(self):
        pairs = list(_client(_FakeStub(self._frames())).get_range_stream("uid-1", 5, 9))
        self.assertEqual([c for _, c in pairs], [b"abc", b"def", b"ghi"])
        self.assertTrue(all(i.total_size == 100 and i.range_method == "seek"
                            for i, _ in pairs))

    def test_whole_file_read_reports_not_ranged(self):
        # ranged=False for a whole-file read, including offset=0/length=0.
        stub = _FakeStub([_Frame(data=b"all", total_size=3, range_length=3)])
        r = _client(stub).get_range("uid-1")
        self.assertFalse(r.info.ranged)
        self.assertEqual(r.info.total_size, 3)

    def test_an_old_server_reports_no_method_rather_than_lying(self):
        # A core that predates this leaves range_method empty. It must come back
        # empty rather than defaulting to "seek", because a caller that trusts a
        # fabricated "seek" will build a scrubbing UI on an O(offset) read.
        stub = _FakeStub([_Frame(data=b"x")])
        self.assertEqual(_client(stub).get_range("uid-1").info.range_method, "")


class RangeOnAnOlderVersion(unittest.TestCase):
    """back= and offset= compose, and do not fall into the unary RPC."""

    def test_ranged_read_of_a_previous_revision_streams(self):
        stub = _FakeStub([_Frame(data=b"old-slice", total_size=50,
                                 range_start=2, range_length=9, ranged=True)])
        mf = _client(stub)
        mf.revisions = lambda *a, **k: [
            type("R", (), {"version": "20260929_120000.000"})(),
            type("R", (), {"version": "20260928_120000.000"})(),
        ]
        buf = mf.get("uid-1", back=1, offset=2, length=9)
        self.assertEqual(buf.read(), b"old-slice")
        # GetVersion is unary and carries no range fields; using it here would
        # silently return the WHOLE version instead of the slice asked for.
        self.assertEqual(stub.get_version_calls, 0)
        self.assertEqual(stub.requests[0].version_timestamp, "20260928_120000.000")
        self.assertEqual(stub.requests[0].offset, 2)

    def test_unranged_read_of_a_previous_revision_still_uses_getversion(self):
        # Unchanged behaviour for everyone not asking for a range.
        stub = _FakeStub()
        mf = _client(stub)
        mf.revisions = lambda *a, **k: [
            type("R", (), {"version": "20260929_120000.000"})(),
            type("R", (), {"version": "20260928_120000.000"})(),
        ]
        mf.get("uid-1", back=1)
        self.assertEqual(stub.get_version_calls, 1)


class RangeInfoDefaults(unittest.TestCase):

    def test_defaults_describe_an_unranged_read(self):
        i = RangeInfo()
        self.assertEqual((i.total_size, i.range_start, i.range_length), (0, 0, 0))
        self.assertFalse(i.ranged)
        self.assertEqual(i.range_method, "")


if __name__ == "__main__":
    unittest.main()
