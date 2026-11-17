import copy
import sqlite3
from capture.archive.publication import PublicationHooks
from capture.archive.query import Archive
from capture.archive.database import StorageVersionError
from capture.source.session import SessionConflictError
from capture.source.replay import ReplaySource
from capture.transactions.identity import EventConflictError
from capture.source.segments import SegmentPositionError
from capture.intake.collector import open as open_collector, CollectorClosedError
from capture.intake.retention import PayloadStore, RetentionManager
from tests.support import CaptureCase, Tape, rollback_tape, reference_accounts, session


class TestReplay(CaptureCase):
    def test_duplicate_and_conflict(self):
        tape = Tape()
        tape.add("begin")
        tape.row("insert", key=1, value=10)
        tape.add("commit")
        collector = self.connect(tape)
        collector.feed(tape.segment(0, 2))
        before = self.state(collector)
        reordered = copy.deepcopy(tape.events[:2])
        reordered = [dict(reversed(list(event.items()))) for event in reordered]
        collector.feed({"events": reordered})
        self.assertEqual(self.state(collector), before)
        bad = tape.segment(1, 3)
        bad["events"][0]["payload"]["row"]["value"] = 20
        with self.assertRaises(EventConflictError):
            collector.feed(bad)
        self.assertEqual(self.state(collector), before)
        collector.feed(tape.segment(1))
        after = self.state(collector)
        self.assertEqual(PayloadStore(collector.database.connection).retained_ids(), ())
        result = collector.feed(tape.segment())
        self.assertEqual(result.completed_top_ids, ())
        self.assertEqual(self.state(collector), after)
        conflict = tape.segment()
        conflict["events"][1]["payload"]["row"]["value"] = 99
        with self.assertRaises(EventConflictError):
            collector.feed(conflict)
        self.assertEqual(self.state(collector), after)

    def test_conflict_after_legal_new_prefix(self):
        tape = Tape()
        tape.add("begin", top="a")
        tape.row("insert", key=1, value=1, top="a")
        tape.add("commit", top="a")
        tape.add("begin", top="b")
        tape.row("insert", key=2, value=2, top="b")
        collector = self.connect(tape)
        collector.feed(tape.segment(0, 3))
        before = self.state(collector)
        conflict = tape.segment(3)
        conflict["events"][-1]["event_id"] = "e2"
        with self.assertRaises(EventConflictError):
            collector.feed(conflict)
        self.assertEqual(self.state(collector), before)

    def test_overlap_suffix_and_position_gap(self):
        tape = Tape()
        tape.add("begin")
        tape.row("insert", key=1, value=10)
        tape.add("commit")
        collector = self.connect(tape)
        collector.feed(tape.segment(0, 1))
        before = self.state(collector)
        with self.assertRaises(SegmentPositionError):
            collector.feed(tape.segment(2))
        self.assertEqual(self.state(collector), before)
        gap = tape.segment()
        gap["events"][1]["source_position"] = 3
        gap["events"][2]["source_position"] = 4
        with self.assertRaises(SegmentPositionError):
            collector.feed(gap)
        self.assertEqual(self.state(collector), before)
        collector.feed(tape.segment(0, 2))
        collector = self.reopen(collector, tape)
        collector.feed(tape.segment(1))
        self.assertEqual(collector.archive.read("accounts"), [{"id": 1, "value": 10}])
        self.assertEqual(self.receipts(collector), [("t", 1)])
        before = self.state(collector)
        wrong_position = tape.segment()
        wrong_position["events"][0]["event_id"] = "other"
        with self.assertRaises(EventConflictError):
            collector.feed(wrong_position)
        self.assertEqual(self.state(collector), before)


class TestRecovery(CaptureCase):
    def test_publication_failure(self):
        tape = Tape()
        tape.add("begin", top="seed")
        tape.row("insert", key=1, value=1, top="seed")
        tape.add("commit", top="seed")
        tape.add("begin", top="a")
        tape.row("update", key=1, value=10, top="a")
        tape.row("insert", key=2, value=20, top="a")
        tape.add("commit", top="a")
        tape.add("begin", top="b")
        tape.row("insert", key=3, value=30, top="b")
        tape.add("commit", top="b")
        collector = self.connect(tape)
        collector.feed(tape.segment(0, 3))
        for stage in ("after_rows", "before_receipt", "before_commit"):
            with self.subTest(stage=stage):
                before = self.state(collector)
                ack_calls = list(collector.source.ack_calls)
                failure = sqlite3.OperationalError("injected " + stage)
                with Archive(self.path) as reader:
                    def fail_at(actual):
                        self.assertEqual(reader.read("accounts"), [{"id": 1, "value": 1}])
                        if actual == stage:
                            raise failure
                    collector.hooks = PublicationHooks(fail_at)
                    with self.assertRaises(sqlite3.OperationalError) as caught:
                        collector.feed(tape.segment(3))
                    self.assertIs(caught.exception, failure)
                self.assertEqual(self.state(collector), before)
                self.assertEqual(collector.source.ack_calls, ack_calls)
                collector = self.reopen(collector, tape)
                self.assertEqual(self.state(collector), before)
        collector.feed(tape.segment(3))
        self.assertEqual(collector.archive.read("accounts"), reference_accounts(tape.events))
        self.assertEqual(self.receipts(collector), [("seed", 1), ("a", 2), ("b", 3)])
        after = self.state(collector)
        collector.feed(tape.segment(3))
        self.assertEqual(self.state(collector), after)

    def test_reopen_every_boundary(self):
        tape = rollback_tape("move")
        tape.add("begin", top="other")
        tape.row("insert", key=10, value=10, top="other")
        tape.add("commit", top="other")
        collector = self.connect(tape)
        for index in range(len(tape.events)):
            collector.feed(tape.segment(index, index + 1))
            self.assertEqual(collector.archive.read("accounts"),
                             reference_accounts(tape.events[:index + 1]))
            before = self.state(collector)
            collector = self.reopen(collector, tape)
            self.assertEqual(self.state(collector), before)
            collector.feed(tape.segment(max(0, index - 2), index + 1))
            self.assertEqual(self.state(collector), before)
        self.assertEqual(self.receipts(collector), [("seed", 1), ("t", 2), ("other", 3)])

    def test_lost_ack_and_retention(self):
        tape = Tape()
        tape.add("begin", top="long")
        tape.row("insert", key=1, value=10, top="long")
        tape.add("begin", top="short")
        tape.row("insert", key=2, value=20, top="short")
        tape.add("commit", top="short")
        tape.row("update", key=1, value=11, top="long")
        tape.add("commit", top="long")
        collector = self.connect(tape, lose_ack_once=(5,), truncate_on_ack=True)
        result = collector.feed(tape.segment(0, 5))
        self.assertEqual(result.accepted_position, 5)
        checkpoint = collector.checkpoint()
        self.assertEqual((checkpoint.accepted_position, checkpoint.confirmed_position,
                          checkpoint.pending_position), (5, 0, 5))
        self.assertIsInstance(collector.acknowledgements.last_error, OSError)
        identifiers = PayloadStore(collector.database.connection).retained_ids()
        self.assertEqual(len(identifiers), 1)
        self.assertEqual(len(PayloadStore(collector.database.connection).load(identifiers[0])), 5)
        self.assertEqual(collector.archive.read("accounts"), [{"id": 2, "value": 20}])
        collector.close()
        source = ReplaySource(session(), tape.events, truncate_on_ack=True)
        source.acknowledge(5)
        collector = open_collector(self.path, source)
        self.collectors.append(collector)
        collector.feed(tape.segment(0, 5))
        self.assertEqual(collector.checkpoint().confirmed_position, 5)
        self.assertEqual(PayloadStore(collector.database.connection).retained_ids(), identifiers)
        with collector.database.write_unit() as connection:
            self.assertEqual(RetentionManager(connection).reclaim(), ())
        collector = self.reopen(collector, tape)
        collector.feed(tape.segment(5))
        self.assertEqual(collector.archive.read("accounts"), reference_accounts(tape.events))
        self.assertEqual(self.receipts(collector), [("short", 1), ("long", 2)])
        self.assertEqual(PayloadStore(collector.database.connection).retained_ids(), ())
        with collector.database.write_unit() as connection:
            self.assertEqual(RetentionManager(connection).reclaim(), ())
        before = self.state(collector)
        collector.feed(tape.segment())
        self.assertEqual(self.state(collector), before)

    def test_session_guard_and_close_resume(self):
        tape = Tape()
        tape.add("begin")
        tape.row("insert", key=1, value=10)
        tape.add("commit")
        collector = self.connect(tape)
        collector.feed(tape.segment(0, 2))
        before = self.state(collector)
        collector.close()
        collector.close()
        with self.assertRaises(CollectorClosedError):
            collector.feed(tape.segment(2))
        for conflicting in (session("different"), session(tables={
                "x": {"columns": {"id": {"type": "int", "nullable": False}},
                      "primary_key": ["id"]}})):
            with self.assertRaises(SessionConflictError):
                open_collector(self.path, ReplaySource(conflicting))
        connection = sqlite3.connect(self.path)
        connection.execute("PRAGMA user_version=99")
        connection.close()
        with self.assertRaises(StorageVersionError):
            open_collector(self.path, ReplaySource(session(), tape.events))
        connection = sqlite3.connect(self.path)
        self.assertEqual(connection.execute("PRAGMA user_version").fetchone()[0], 99)
        connection.execute("PRAGMA user_version=1")
        connection.close()
        collector = self.connect(tape)
        self.assertEqual(self.state(collector), before)
        collector.feed(tape.segment(2))
        self.assertEqual(collector.archive.read("accounts"), [{"id": 1, "value": 10}])

    def test_idle_end_preserves_unfinished(self):
        tape = Tape()
        tape.add("begin")
        tape.row("insert", key=1, value=1)
        collector = self.connect(tape, idle_polls=(1,), batch_size=2)
        self.assertIsNone(collector.poll_once())
        self.assertEqual(collector.last_poll_state, "idle")
        self.assertEqual(collector.checkpoint().accepted_position, 0)
        collector.poll_once()
        before = self.state(collector)
        self.assertIsNone(collector.poll_once())
        self.assertEqual(collector.last_poll_state, "end")
        self.assertEqual(self.state(collector), before)
        self.assertEqual(collector.archive.read("accounts"), [])
        self.assertEqual(collector.checkpoint().unfinished_top_ids, ("t",))
