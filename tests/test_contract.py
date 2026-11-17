import copy
import sqlite3
from capture.source.codec import canonical_bytes, decode_canonical, CanonicalEncodingError
from capture.source.envelope import EnvelopeError
from capture.source.session import PollResult, SessionConflictError, SourceSession
from capture.source.replay import ReplaySource
from capture.rows.schema import RowValidationError
from capture.intake.collector import open as open_collector
from capture.intake.recovery import RecoveryConsistencyError
from tests.support import CaptureCase, Tape, session


class TestContract(CaptureCase):
    def test_codec_rejects_unstable_values(self):
        self.assertEqual(canonical_bytes({"b": 2, "a": 1}), canonical_bytes({"a": 1, "b": 2}))
        for value in (float("nan"), float("inf"), {1: "x"}, (1, 2), object()):
            with self.subTest(value=repr(value)), self.assertRaises(CanonicalEncodingError):
                canonical_bytes(value)
        with self.assertRaises(CanonicalEncodingError):
            decode_canonical('{"x":1,"x":2}')
        self.assertEqual(decode_canonical(canonical_bytes({"text": "中文", "null": None})),
                         {"text": "中文", "null": None})

    def test_envelope_errors_are_atomic(self):
        tape = Tape()
        tape.add("begin")
        tape.row("insert", key=1, value=1)
        collector = self.connect(tape)
        before = self.state(collector)
        invalid = [
            {"source_position": True}, {"kind": "unknown"}, {"top_id": ""},
            {"event_id": ""}, {"payload": []}]
        for changes in invalid:
            bad = tape.segment()
            bad["events"][0].update(changes)
            with self.subTest(changes=changes), self.assertRaises(EnvelopeError):
                collector.feed(bad)
            self.assertEqual(self.state(collector), before)
        bad = tape.segment()
        bad["events"][1]["session_id"] = "wrong"
        from capture.transactions.identity import EventConflictError
        with self.assertRaises(EventConflictError):
            collector.feed(bad)
        self.assertEqual(self.state(collector), before)

    def test_recovery_rejects_missing_live_reference(self):
        tape = Tape()
        tape.add("begin")
        tape.row("insert", key=1, value=1)
        collector = self.connect(tape)
        collector.feed(tape.segment())
        collector.close()
        connection = sqlite3.connect(self.path)
        connection.execute("DELETE FROM journal WHERE position=2")
        connection.commit()
        connection.close()
        source = ReplaySource(session(), tape.events)
        with self.assertRaises(RecoveryConsistencyError):
            open_collector(self.path, source)
        self.assertEqual(source.ack_calls, [])
        self.assertTrue(source.closed)

    def test_recovery_rejects_tampered_lineage(self):
        tape = Tape()
        tape.add("begin")
        tape.add("branch", branch="child", parent="root")
        tape.add("seal", branch="child")
        collector = self.connect(tape)
        collector.feed(tape.segment())
        collector.close()
        connection = sqlite3.connect(self.path)
        connection.execute("UPDATE branches SET state='active' WHERE branch_id='child'")
        connection.commit()
        connection.close()
        source = ReplaySource(session(), tape.events)
        with self.assertRaises(RecoveryConsistencyError):
            open_collector(self.path, source)
        self.assertEqual(source.ack_calls, [])

    def test_source_poll_and_schema_errors(self):
        for state, segment in (("unknown", None), ("segment", None), ("end", {"events": []})):
            with self.subTest(state=state), self.assertRaises(ValueError):
                PollResult(state, segment)
        with self.assertRaises(SessionConflictError):
            open_collector(self.path, ReplaySource(SourceSession("source", 2, session().tables)))
        definitions = copy.deepcopy(session().tables)
        definitions["accounts"]["primary_key"] = ["value", "value"]
        with self.assertRaises(RowValidationError):
            open_collector(self.path, ReplaySource(session(tables=definitions)))
