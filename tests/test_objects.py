"""Business regression for late-arriving large fields (OBJECTS.md).

Covers overwritten/deleted references not blocking publication, key occupancy
against the logical base, object declarations and conflicts, zero-part
objects, cross-part UTF-8, whole-segment rollback, interleaved commits,
reopen at every acceptance boundary, lost acknowledgement replies and
replay after reclamation. An independent small-domain reference engine
computes the expected rows, receipts and waiting tops for every boundary.
"""
import base64
import copy
import hashlib
import random
import sqlite3
import unittest
from pathlib import Path

from capture.archive.publication import PublicationHooks
from capture.archive.query import Archive
from capture.intake.collector import open as open_collector
from capture.intake.recovery import RecoveryConsistencyError
from capture.rows.schema import RowValidationError
from capture.source.envelope import EnvelopeError
from capture.source.replay import ReplaySource
from capture.source.session import SourceSession
from capture.transactions.completion import TransactionStateError
from capture.transactions.identity import EventConflictError
from capture.transactions.lineage import TransactionRelationError
from tests.support import CaptureCase

TABLES = {
    "documents": {"columns": {
        "id": {"type": "int", "nullable": False},
        "text": {"type": "str", "nullable": False}},
        "primary_key": ["id"]},
    "notes": {"columns": {
        "id": {"type": "int", "nullable": False},
        "body": {"type": "str", "nullable": True}},
        "primary_key": ["id"]},
}

EMPTY_SHA256 = hashlib.sha256(b"").hexdigest()


def session(name="objects"):
    return SourceSession(name, 1, copy.deepcopy(TABLES))


class ObjectTape:
    def __init__(self, session_id="objects"):
        self.session_id = session_id
        self.events = []

    def add(self, kind, top="t", branch="root", payload=None, parent=None):
        position = len(self.events) + 1
        event = {"session_id": self.session_id, "source_position": position,
                 "event_id": "e" + str(position), "top_id": top,
                 "branch_id": branch, "kind": kind,
                 "payload": {} if payload is None else payload}
        if kind == "branch":
            event["parent_id"] = parent
        self.events.append(event)
        return event

    def row(self, op, key, text=None, new_key=None, top="t", branch="root"):
        payload = {"table": "documents", "op": op}
        if op != "insert":
            payload["key"] = [key]
        if op != "delete":
            payload["row"] = {"id": key if new_key is None else new_key, "text": text}
        return self.add("row", top, branch, payload)

    def part(self, object_id, index, data, top="t"):
        return self.add("object_part", top=top, payload={
            "object_id": object_id, "index": index,
            "data": base64.b64encode(data).decode("ascii")})

    def seal(self, object_id, pieces, top="t"):
        body = b"".join(pieces)
        return self.add("object_seal", top=top, payload={
            "object_id": object_id, "parts": len(pieces), "size": len(body),
            "sha256": hashlib.sha256(body).hexdigest()})

    def segment(self, start=0, stop=None):
        return {"events": copy.deepcopy(self.events[start:stop])}


class ReferenceEngine:
    """Independent small-domain model of the public rules.

    Single table documents(id, text). Computes published rows, receipts and
    waiting tops from the event list alone; used as the oracle for the real
    collector at every acceptance boundary.
    """

    def __init__(self):
        self.tops = {}
        self.objects = {}
        self.pending = []
        self.archive = {}
        self.receipts = []

    @staticmethod
    def _blank():
        return {"seal": None, "parts": {}, "content": None}

    def _object(self, top, object_id):
        return self.objects.setdefault((top, object_id), self._blank())

    def _live(self, top, branch):
        branches = self.tops[top]["branches"]
        while branch is not None:
            parent, state = branches[branch]
            if state == "aborted":
                return False
            branch = parent
        return True

    def _surviving(self, top):
        return [(branch, payload) for branch, payload in self.tops[top]["rows"]
                if self._live(top, branch)]

    def _apply(self, top, rows, resolve):
        written = {}
        for _, payload in self._surviving(top):
            op = payload["op"]
            if op == "delete":
                rows.pop(payload["key"][0], None)
                written.pop(payload["key"][0], None)
                continue
            row = dict(payload["row"])
            if resolve:
                for name, value in row.items():
                    if isinstance(value, dict):
                        row[name] = self.objects[(top, value["$object"])]["content"]
            if op == "update":
                old = payload["key"][0]
                assert old in rows, "reference update of a missing key"
                if old != row["id"]:
                    assert row["id"] not in rows, "reference move onto an occupied key"
                    del rows[old]
                    written.pop(old, None)
            else:
                assert row["id"] not in rows, "reference insert of an occupied key"
            rows[row["id"]] = row
            written[row["id"]] = row
        return written

    def logical_rows(self):
        rows = dict(self.archive)
        for top in self.pending:
            self._apply(top, rows, resolve=False)
        return rows

    def _complete(self, top, object_id):
        obj = self._object(top, object_id)
        seal = obj["seal"]
        if seal is not None and len(obj["parts"]) == seal[0]:
            body = b"".join(obj["parts"][index] for index in range(seal[0]))
            assert len(body) == seal[1] and hashlib.sha256(body).hexdigest() == seal[2]
            obj["content"] = body.decode("utf-8")

    def _try_publish(self):
        published = []
        while self.pending:
            top = self.pending[0]
            written = self._apply(top, dict(self.archive), resolve=False)
            references = {value["$object"] for row in written.values()
                          for value in row.values() if isinstance(value, dict)}
            if any(self.objects.get((top, object_id), self._blank())["content"] is None
                   for object_id in references):
                break
            rows = dict(self.archive)
            self._apply(top, rows, resolve=True)
            self.archive = rows
            self.pending.pop(0)
            self.receipts.append(top)
            published.append(top)
        return published

    def accept(self, event):
        kind, top = event["kind"], event["top_id"]
        if kind == "begin":
            self.tops[top] = {"state": "active", "rows": [],
                              "branches": {event["branch_id"]: [None, "active"]}}
        elif kind == "branch":
            self.tops[top]["branches"][event["branch_id"]] = [event["parent_id"], "active"]
        elif kind == "seal":
            self.tops[top]["branches"][event["branch_id"]][1] = "sealed"
        elif kind == "rollback":
            doomed = {event["branch_id"]}
            branches = self.tops[top]["branches"]
            while True:
                expanded = doomed | {b for b, (p, _) in branches.items() if p in doomed}
                if expanded == doomed:
                    break
                doomed = expanded
            for branch in doomed:
                branches[branch][1] = "aborted"
        elif kind == "row":
            self.tops[top]["rows"].append((event["branch_id"], event["payload"]))
        elif kind == "abort":
            self.tops[top]["state"] = "aborted"
        elif kind == "commit":
            self.tops[top]["state"] = "committed"
            self.pending.append(top)
        elif kind == "object_part":
            payload = event["payload"]
            obj = self._object(top, payload["object_id"])
            obj["parts"][payload["index"]] = base64.b64decode(payload["data"])
            self._complete(top, payload["object_id"])
        elif kind == "object_seal":
            payload = event["payload"]
            self._object(top, payload["object_id"])["seal"] = (
                payload["parts"], payload["size"], payload["sha256"])
            self._complete(top, payload["object_id"])
        else:
            raise AssertionError("unknown event kind")
        return self._try_publish()

    def expected_rows(self):
        return [self.archive[key] for key in sorted(self.archive)]

    def expected_unfinished(self):
        active = [top for top, value in self.tops.items() if value["state"] == "active"]
        return tuple(sorted(active + self.pending))

    def expected_receipts(self):
        return [(top, index + 1) for index, top in enumerate(self.receipts)]


class ObjectCase(CaptureCase):
    def connect(self, tape, **source_options):
        source = ReplaySource(session(), tape.events, **source_options)
        collector = open_collector(self.path, source)
        self.collectors.append(collector)
        return collector

    def object_state(self, collector):
        connection = collector.database.connection
        return (tuple(connection.execute("SELECT * FROM objects ORDER BY top_id,object_id")),
                tuple(connection.execute(
                    "SELECT * FROM object_parts ORDER BY top_id,object_id,idx")),
                tuple(connection.execute("SELECT * FROM pending ORDER BY commit_position")),
                tuple(connection.execute("SELECT * FROM pending_refs ORDER BY 1,2")))

    def full_state(self, collector):
        connection = collector.database.connection
        return (collector.archive.read("documents"), collector.checkpoint(),
                tuple(connection.execute("SELECT * FROM receipts ORDER BY publication")),
                tuple(connection.execute("SELECT * FROM identities ORDER BY position")),
                tuple(connection.execute("SELECT * FROM tops ORDER BY top_id")),
                tuple(connection.execute("SELECT * FROM branches ORDER BY top_id,branch_id")),
                tuple(connection.execute("SELECT * FROM journal ORDER BY position")),
                tuple(connection.execute("SELECT * FROM payloads ORDER BY payload_id")),
                tuple(connection.execute("SELECT * FROM progress")),
                ) + self.object_state(collector)

    def check_against(self, collector, engine, position):
        self.assertEqual(collector.archive.read("documents"), engine.expected_rows())
        self.assertEqual(self.receipts(collector), engine.expected_receipts())
        checkpoint = collector.checkpoint()
        self.assertEqual(checkpoint.unfinished_top_ids, engine.expected_unfinished())
        self.assertEqual(checkpoint.accepted_position, position)
        self.assertIsNone(checkpoint.pending_position)

    def drive(self, tape, engine=None, replay_each=True):
        """Feed one event per feed, reopen and replay at every boundary."""
        engine = ReferenceEngine() if engine is None else engine
        collector = self.connect(tape)
        for index, event in enumerate(tape.events, 1):
            published = engine.accept(event)
            result = collector.feed({"events": [copy.deepcopy(event)]})
            self.assertEqual(list(result.completed_top_ids), published)
            self.assertEqual(result.accepted_position, index)
            self.check_against(collector, engine, index)
            before = self.full_state(collector)
            collector = self.reopen(collector, tape)
            self.assertEqual(self.full_state(collector), before)
            self.check_against(collector, engine, index)
            if replay_each:
                again = collector.feed({"events": [copy.deepcopy(event)]})
                self.assertEqual(again.completed_top_ids, ())
                self.assertEqual(self.full_state(collector), before)
                self.check_against(collector, engine, index)
        return collector


class TestScenarios(ObjectCase):
    def test_withdrawn_reference_waits_for_last_part(self):
        tape = ObjectTape()
        tape.add("begin")
        tape.row("insert", 1, {"$object": "X"})
        tape.add("branch", branch="child", parent="root")
        tape.row("update", 1, {"$object": "Y"}, branch="child")
        tape.add("rollback", branch="child")
        body = "中文".encode("utf-8")
        tape.seal("X", [body[:2], body[2:]])
        tape.part("X", 0, body[:2])
        tape.add("commit")
        tape.part("X", 1, body[2:])
        collector = self.connect(tape)
        collector.feed(tape.segment(0, 8))
        self.assertEqual(collector.archive.read("documents"), [])
        self.assertEqual(self.receipts(collector), [])
        self.assertEqual(collector.checkpoint().unfinished_top_ids, ("t",))
        result = collector.feed(tape.segment(8))
        self.assertEqual(result.completed_top_ids, ("t",))
        self.assertEqual(collector.archive.read("documents"),
                         [{"id": 1, "text": "中文"}])
        self.assertEqual(self.receipts(collector), [("t", 1)])
        # The receipt keeps the original commit event identity, not the
        # identity of the part event that released the publication.
        receipt = collector.database.connection.execute(
            "SELECT event_id,digest FROM receipts WHERE top_id='t'").fetchone()
        commit_event = tape.events[7]
        self.assertEqual(receipt[0], commit_event["event_id"])
        from capture.source.codec import content_digest
        self.assertEqual(receipt[1], content_digest(commit_event))
        # Y never received any content and never blocked anything.
        self.assertEqual(collector.checkpoint().unfinished_top_ids, ())

    def test_same_segment_commit_and_completing_part(self):
        tape = ObjectTape()
        tape.add("begin")
        tape.row("insert", 1, {"$object": "X"})
        tape.add("commit")
        tape.seal("X", ["即".encode("utf-8")])
        tape.part("X", 0, "即".encode("utf-8"))
        collector = self.connect(tape)
        result = collector.feed(tape.segment())
        self.assertEqual(result.completed_top_ids, ("t",))
        self.assertEqual(collector.archive.read("documents"), [{"id": 1, "text": "即"}])

    def test_reference_to_object_without_any_event(self):
        tape = ObjectTape()
        tape.add("begin")
        tape.row("insert", 1, {"$object": "Q"})
        tape.add("commit")
        collector = self.connect(tape)
        collector.feed(tape.segment())
        self.assertEqual(collector.checkpoint().unfinished_top_ids, ("t",))
        tape.seal("Q", ["到".encode("utf-8")])
        tape.part("Q", 0, "到".encode("utf-8"))
        result = collector.feed(tape.segment(3))
        self.assertEqual(result.completed_top_ids, ("t",))
        self.assertEqual(collector.archive.read("documents"), [{"id": 1, "text": "到"}])

    def test_same_object_id_independent_across_tops(self):
        tape = ObjectTape()
        tape.add("begin", top="a")
        tape.row("insert", 1, {"$object": "X"}, top="a")
        tape.add("commit", top="a")
        tape.add("begin", top="b")
        tape.row("insert", 2, {"$object": "X"}, top="b")
        tape.add("commit", top="b")
        tape.seal("X", ["甲".encode("utf-8")], top="b")
        tape.part("X", 0, "甲".encode("utf-8"), top="b")
        tape.seal("X", ["乙".encode("utf-8")], top="a")
        tape.part("X", 0, "乙".encode("utf-8"), top="a")
        collector = self.connect(tape)
        collector.feed(tape.segment(0, 6))
        self.assertEqual(collector.checkpoint().unfinished_top_ids, ("a", "b"))
        result = collector.feed(tape.segment(6, 8))
        self.assertEqual(result.completed_top_ids, ())
        self.assertEqual(collector.archive.read("documents"), [])
        result = collector.feed(tape.segment(8))
        self.assertEqual(result.completed_top_ids, ("a", "b"))
        self.assertEqual(collector.archive.read("documents"),
                         [{"id": 1, "text": "乙"}, {"id": 2, "text": "甲"}])

    def test_ordered_release_in_commit_order(self):
        tape = ObjectTape()
        tape.add("begin")
        tape.row("insert", 1, {"$object": "X"})
        tape.seal("X", ["中".encode("utf-8")])
        tape.add("commit")
        tape.add("begin", top="next")
        tape.row("update", 1, "后继", new_key=2, top="next")
        tape.add("commit", top="next")
        tape.part("X", 0, "中".encode("utf-8"))
        collector = self.connect(tape)
        collector.feed(tape.segment(0, 4))
        collector.feed(tape.segment(4, 7))
        self.assertEqual(collector.archive.read("documents"), [])
        self.assertEqual(collector.checkpoint().unfinished_top_ids, ("next", "t"))
        result = collector.feed(tape.segment(7))
        self.assertEqual(result.completed_top_ids, ("t", "next"))
        self.assertEqual(collector.archive.read("documents"),
                         [{"id": 2, "text": "后继"}])
        self.assertEqual(self.receipts(collector), [("t", 1), ("next", 2)])

    def test_overwritten_deleted_and_withdrawn_references_not_waited(self):
        tape = ObjectTape()
        tape.add("begin")
        tape.row("insert", 1, {"$object": "X"})      # overwritten below
        tape.row("update", 1, "plain")
        tape.row("insert", 2, {"$object": "Y"})      # deleted below
        tape.row("delete", 2)
        tape.add("branch", branch="child", parent="root")
        tape.row("insert", 3, {"$object": "Z"}, branch="child")
        tape.add("rollback", branch="child")          # withdrawn reference
        tape.add("commit")
        collector = self.connect(tape)
        result = collector.feed(tape.segment())
        self.assertEqual(result.completed_top_ids, ("t",))
        self.assertEqual(collector.archive.read("documents"), [{"id": 1, "text": "plain"}])
        self.assertEqual(self.receipts(collector), [("t", 1)])
        self.assertEqual(self.object_state(collector), ((), (), (), ()))

    def test_only_final_written_reference_is_waited(self):
        tape = ObjectTape()
        tape.add("begin")
        tape.row("insert", 1, {"$object": "X"})
        tape.row("delete", 1)
        tape.row("insert", 1, {"$object": "Y"})
        tape.add("commit")
        tape.seal("Y", ["好".encode("utf-8")])
        tape.part("Y", 0, "好".encode("utf-8"))
        collector = self.connect(tape)
        collector.feed(tape.segment(0, 5))
        self.assertEqual(collector.checkpoint().unfinished_top_ids, ("t",))
        result = collector.feed(tape.segment(5))
        self.assertEqual(result.completed_top_ids, ("t",))
        self.assertEqual(collector.archive.read("documents"), [{"id": 1, "text": "好"}])

    def test_zero_part_object(self):
        tape = ObjectTape()
        tape.add("begin")
        tape.seal("empty", [])
        tape.row("insert", 1, {"$object": "empty"})
        tape.add("commit")
        collector = self.connect(tape)
        result = collector.feed(tape.segment())
        self.assertEqual(result.completed_top_ids, ("t",))
        self.assertEqual(collector.archive.read("documents"), [{"id": 1, "text": ""}])
        # The empty object is reclaimable and recovery accepts the scrubbed state.
        connection = collector.database.connection
        self.assertEqual(connection.execute(
            "SELECT content IS NULL,reclaimed FROM objects").fetchall(), [(1, 1)])
        before = self.full_state(collector)
        collector = self.reopen(collector, tape)
        self.assertEqual(self.full_state(collector), before)
        replay = collector.feed(tape.segment())
        self.assertEqual(replay.completed_top_ids, ())
        self.assertEqual(self.full_state(collector), before)

    def test_nullable_column_accepts_reference_and_null(self):
        tape = ObjectTape()
        tape.add("begin")
        tape.add("row", payload={"table": "notes", "op": "insert",
                                 "row": {"id": 1, "body": {"$object": "X"}}})
        tape.add("row", payload={"table": "notes", "op": "insert",
                                 "row": {"id": 2, "body": None}})
        tape.add("commit")
        tape.seal("X", ["中".encode("utf-8")])
        tape.part("X", 0, "中".encode("utf-8"))
        collector = self.connect(tape)
        collector.feed(tape.segment(0, 4))
        self.assertEqual(collector.checkpoint().unfinished_top_ids, ("t",))
        result = collector.feed(tape.segment(4))
        self.assertEqual(result.completed_top_ids, ("t",))
        self.assertEqual(collector.archive.read("notes"),
                         [{"id": 1, "body": "中"}, {"id": 2, "body": None}])

    def test_idle_and_end_never_publish_or_cancel_waiting(self):
        tape = ObjectTape()
        tape.add("begin")
        tape.row("insert", 1, {"$object": "X"})
        tape.add("commit")
        tape.seal("X", [b"a", b"b"])
        tape.part("X", 0, b"a")
        collector = self.connect(tape, idle_polls=(2,), batch_size=3)
        result = collector.poll_once()
        self.assertEqual(result.completed_top_ids, ())
        self.assertEqual(collector.last_poll_state, "segment")
        self.assertEqual(collector.checkpoint().unfinished_top_ids, ("t",))
        before = self.full_state(collector)
        self.assertIsNone(collector.poll_once())
        self.assertEqual(collector.last_poll_state, "idle")
        self.assertEqual(self.full_state(collector), before)
        collector.poll_once()
        self.assertEqual(collector.last_poll_state, "segment")
        self.assertEqual(collector.checkpoint().unfinished_top_ids, ("t",))
        self.assertEqual(collector.archive.read("documents"), [])
        before = self.full_state(collector)
        self.assertIsNone(collector.poll_once())
        self.assertEqual(collector.last_poll_state, "end")
        self.assertEqual(self.full_state(collector), before)
        tape.part("X", 1, b"b")
        result = collector.feed(tape.segment(5))
        self.assertEqual(result.completed_top_ids, ("t",))
        self.assertEqual(collector.archive.read("documents"), [{"id": 1, "text": "ab"}])

    def test_cross_part_utf8_and_empty_parts(self):
        tape = ObjectTape()
        tape.add("begin")
        body = "中文".encode("utf-8")
        pieces = [body[:1], b"", body[1:2], body[2:3], body[3:]]
        tape.part("X", 2, pieces[2])
        tape.seal("X", pieces)
        tape.part("X", 0, pieces[0])
        tape.part("X", 4, pieces[4])
        tape.part("X", 1, pieces[1])
        tape.part("X", 3, pieces[3])
        tape.row("insert", 1, {"$object": "X"})
        tape.add("commit")
        collector = self.connect(tape)
        result = collector.feed(tape.segment())
        self.assertEqual(result.completed_top_ids, ("t",))
        self.assertEqual(collector.archive.read("documents"), [{"id": 1, "text": "中文"}])


class TestKeyOccupancy(ObjectCase):
    def test_logical_base_counts_committed_predecessors(self):
        tape = ObjectTape()
        tape.add("begin", top="a")
        tape.row("insert", 1, {"$object": "X"}, top="a")
        tape.add("commit", top="a")
        tape.add("begin", top="b")
        tape.row("update", 1, "moved", new_key=2, top="b")
        tape.add("commit", top="b")
        tape.add("begin", top="c")
        tape.row("insert", 1, "again", top="c")
        tape.add("commit", top="c")
        tape.seal("X", ["值".encode("utf-8")], top="a")
        tape.part("X", 0, "值".encode("utf-8"), top="a")
        collector = self.connect(tape)
        for index in range(9):
            result = collector.feed(tape.segment(index, index + 1))
            self.assertEqual(result.completed_top_ids, ())
        self.assertEqual(collector.checkpoint().unfinished_top_ids, ("a", "b", "c"))
        result = collector.feed(tape.segment(9))
        self.assertEqual(result.completed_top_ids, ("a", "b", "c"))
        self.assertEqual(collector.archive.read("documents"),
                         [{"id": 1, "text": "again"}, {"id": 2, "text": "moved"}])
        self.assertEqual(self.receipts(collector), [("a", 1), ("b", 2), ("c", 3)])

    def test_key_errors_against_logical_base(self):
        tape = ObjectTape()
        tape.add("begin", top="a")
        tape.row("insert", 1, {"$object": "X"}, top="a")
        tape.add("commit", top="a")
        collector = self.connect(tape)
        collector.feed(tape.segment(0, 3))
        # Key 1 is logically occupied even though the archive is still empty.
        bad = ObjectTape()
        bad.events = copy.deepcopy(tape.events)
        bad.add("begin", top="b")
        bad.row("insert", 1, "clash", top="b")
        before = self.full_state(collector)
        with self.assertRaises(RowValidationError):
            collector.feed(bad.segment(3))
        self.assertEqual(self.full_state(collector), before)
        # Deleting a logically absent key fails too.
        bad2 = ObjectTape()
        bad2.events = copy.deepcopy(tape.events)
        bad2.add("begin", top="b")
        bad2.row("delete", 9, top="b")
        with self.assertRaises(RowValidationError):
            collector.feed(bad2.segment(3))
        self.assertEqual(self.full_state(collector), before)
        # An uncommitted top never provides keys to successors.
        tape.add("begin", top="b")
        tape.row("insert", 5, "held", top="b")
        collector.feed(tape.segment(3, 5))
        bad3 = ObjectTape()
        bad3.events = copy.deepcopy(tape.events)
        bad3.add("begin", top="c")
        bad3.row("update", 5, "take", top="c")
        with self.assertRaises(RowValidationError):
            collector.feed(bad3.segment(5))
        self.assertEqual(collector.checkpoint().unfinished_top_ids, ("a", "b"))

    def test_reference_cannot_enter_primary_key(self):
        tape = ObjectTape()
        tape.add("begin")
        collector = self.connect(tape)
        collector.feed(tape.segment(0, 1))
        before = self.full_state(collector)
        bad_rows = [
            {"id": {"$object": "X"}, "text": "v"},          # primary key reference
            {"id": 1, "text": {"$object": ""}},              # empty object id
            {"id": 1, "text": {"$object": "X", "other": 1}},  # extra member
            {"id": 1, "text": {"ref": "X"}},                  # other dictionary
            {"id": 1, "text": 3},                             # wrong type
        ]
        for row in bad_rows:
            with self.subTest(row=row):
                bad = ObjectTape()
                bad.events = copy.deepcopy(tape.events)
                bad.add("row", payload={"table": "documents", "op": "insert", "row": row})
                with self.assertRaises(RowValidationError):
                    collector.feed(bad.segment(1))
                self.assertEqual(self.full_state(collector), before)


class TestObjectDeclarations(ObjectCase):
    def test_part_and_seal_conflicts(self):
        tape = ObjectTape()
        tape.add("begin")
        tape.part("X", 0, b"ab")
        tape.part("X", 0, b"ab")          # identical bytes: accepted
        tape.seal("X", [b"ab", b"cd"])
        tape.seal("X", [b"ab", b"cd"])    # identical declaration: accepted
        collector = self.connect(tape)
        collector.feed(tape.segment(0, 5))
        before = self.full_state(collector)
        conflicts = []
        changed = ObjectTape()
        changed.events = copy.deepcopy(tape.events)
        changed.part("X", 0, b"zz")       # fragment bytes conflict
        conflicts.append(changed.events[-1])
        for field, value in (("parts", 3), ("size", 5),
                             ("sha256", hashlib.sha256(b"xx").hexdigest())):
            changed = ObjectTape()
            changed.events = copy.deepcopy(tape.events)
            seal = changed.seal("X", [b"ab", b"cd"])
            seal["payload"][field] = value
            conflicts.append(changed.events[-1])
        for event in conflicts:
            with self.subTest(event=event):
                with self.assertRaises(EnvelopeError):
                    collector.feed({"events": [event]})
                self.assertEqual(self.full_state(collector), before)

    def test_index_out_of_range_both_directions(self):
        tape = ObjectTape()
        tape.add("begin")
        tape.seal("X", [b"ab"])
        collector = self.connect(tape)
        collector.feed(tape.segment(0, 2))
        before = self.full_state(collector)
        late = ObjectTape()
        late.events = copy.deepcopy(tape.events)
        late.part("X", 1, b"cd")          # index beyond sealed count
        with self.assertRaises(EnvelopeError):
            collector.feed(late.segment(2, 3))
        self.assertEqual(self.full_state(collector), before)
        # Seal arriving after an out-of-range part is rejected as well.
        tape.add("begin", top="u")
        tape.part("Y", 1, b"cd", top="u")
        collector.feed(tape.segment(2, 4))
        before = self.full_state(collector)
        tape.seal("Y", [b"ab"], top="u")
        with self.assertRaises(EnvelopeError):
            collector.feed(tape.segment(4, 5))
        self.assertEqual(self.full_state(collector), before)

    def test_malformed_part_and_seal_fields(self):
        tape = ObjectTape()
        tape.add("begin")
        collector = self.connect(tape)
        collector.feed(tape.segment(0, 1))
        before = self.full_state(collector)
        bad_parts = [
            {"object_id": "", "index": 0, "data": "5Lit"},
            {"object_id": "X", "index": True, "data": "5Lit"},
            {"object_id": "X", "index": -1, "data": "5Lit"},
            {"object_id": "X", "index": "0", "data": "5Lit"},
            {"object_id": "X", "index": 0, "data": "5Lit\n"},
            {"object_id": "X", "index": 0, "data": "5Li t"},
            {"object_id": "X", "index": 0, "data": "5Li-"},   # URL alphabet
            {"object_id": "X", "index": 0, "data": "5Li"},    # missing padding
            {"object_id": "X", "index": 0, "data": "5Lit="},  # extra padding
            {"object_id": "X", "index": 0, "data": "5LB="},   # nonzero padding bits
            {"object_id": "X", "index": 0, "data": "5Lit", "extra": 1},
            {"object_id": "X", "index": 0},
            {"object_id": "X", "index": 0, "data": 5},
        ]
        bad_seals = [
            {"object_id": "X", "parts": True, "size": 6, "sha256": EMPTY_SHA256},
            {"object_id": "X", "parts": 1, "size": -1, "sha256": EMPTY_SHA256},
            {"object_id": "X", "parts": 1, "size": 6, "sha256": EMPTY_SHA256.upper()},
            {"object_id": "X", "parts": 1, "size": 6, "sha256": "0" * 63},
            {"object_id": "X", "parts": 1, "size": 6, "sha256": "g" * 64},
            {"object_id": "X", "parts": 1, "size": 6},
            {"object_id": "X", "parts": 1, "size": 6, "sha256": EMPTY_SHA256, "x": 1},
        ]
        for payload in bad_parts:
            with self.subTest(payload=payload):
                bad = ObjectTape()
                bad.events = copy.deepcopy(tape.events)
                bad.add("object_part", payload=payload)
                with self.assertRaises(EnvelopeError):
                    collector.feed(bad.segment(1))
                self.assertEqual(self.full_state(collector), before)
        for payload in bad_seals:
            with self.subTest(payload=payload):
                bad = ObjectTape()
                bad.events = copy.deepcopy(tape.events)
                bad.add("object_seal", payload=payload)
                with self.assertRaises(EnvelopeError):
                    collector.feed(bad.segment(1))
                self.assertEqual(self.full_state(collector), before)

    def test_completion_length_digest_and_encoding_errors(self):
        cases = []
        tape = ObjectTape()
        tape.add("begin", top="t0")
        tape.seal("X", [b"ab"], top="t0")
        seal = tape.events[-1]
        seal["payload"]["size"] = 3       # assembled length differs
        tape.part("X", 0, b"ab", top="t0")
        cases.append((tape, 3))
        tape = ObjectTape()
        tape.add("begin", top="t1")
        tape.seal("X", [b"ab"], top="t1")
        tape.events[-1]["payload"]["sha256"] = hashlib.sha256(b"zz").hexdigest()
        tape.part("X", 0, b"ab", top="t1")
        cases.append((tape, 3))
        tape = ObjectTape()
        tape.add("begin", top="t2")
        tape.seal("X", [b"\xff"], top="t2")
        tape.part("X", 0, b"\xff", top="t2")   # not strict UTF-8
        cases.append((tape, 3))
        tape = ObjectTape()
        tape.add("begin", top="t3")
        tape.seal("X", [b"\xc0\xaf"], top="t3")  # overlong encoding
        tape.part("X", 0, b"\xc0\xaf", top="t3")
        cases.append((tape, 3))
        collector = self.connect(ObjectTape())
        for tape, index in cases:
            with self.subTest(position=index, top=tape.events[0]["top_id"]):
                before = self.full_state(collector)
                with self.assertRaises(EnvelopeError):
                    collector.feed(tape.segment(0, index))
                self.assertEqual(self.full_state(collector), before)

    def test_object_event_addressing_and_lifetime(self):
        tape = ObjectTape()
        tape.add("begin")
        tape.add("branch", branch="child", parent="root")
        tape.add("begin", top="doomed")
        tape.add("abort", top="doomed")
        collector = self.connect(tape)
        collector.feed(tape.segment(0, 4))
        before = self.full_state(collector)
        # Object event before begin.
        early = ObjectTape()
        early.events = copy.deepcopy(tape.events)
        early.part("X", 0, b"ab", top="missing")
        with self.assertRaises(TransactionStateError):
            collector.feed(early.segment(4, 5))
        # Object event on a non-root branch.
        wrong_branch = ObjectTape()
        wrong_branch.events = copy.deepcopy(tape.events)
        wrong_branch.part("X", 0, b"ab")
        wrong_branch.events[-1]["branch_id"] = "child"
        with self.assertRaises(TransactionRelationError):
            collector.feed(wrong_branch.segment(4, 5))
        # Object event after abort.
        after_abort = ObjectTape()
        after_abort.events = copy.deepcopy(tape.events)
        after_abort.part("X", 0, b"ab", top="doomed")
        with self.assertRaises(TransactionStateError):
            collector.feed(after_abort.segment(4, 5))
        self.assertEqual(self.full_state(collector), before)
        # Object events remain accepted after commit while unpublished...
        tape.row("insert", 1, {"$object": "X"})
        tape.part("X", 0, b"ab")
        tape.add("commit")
        tape.part("X", 1, b"cd")
        collector.feed(tape.segment(4, 7))
        collector.feed(tape.segment(7, 8))
        self.assertEqual(collector.checkpoint().unfinished_top_ids, ("t",))
        # ...but not after publication.
        tape.seal("X", [b"ab", b"cd"])
        tape.part("X", 2, b"ef")
        result = collector.feed(tape.segment(8, 9))
        self.assertEqual(result.completed_top_ids, ("t",))
        before = self.full_state(collector)
        with self.assertRaises(TransactionStateError):
            collector.feed(tape.segment(9, 10))
        self.assertEqual(self.full_state(collector), before)


class TestSegmentRollback(ObjectCase):
    def waiting_tape(self):
        tape = ObjectTape()
        tape.add("begin")
        tape.row("insert", 1, {"$object": "X"})
        tape.add("commit")
        tape.seal("X", ["中".encode("utf-8")])
        return tape

    def test_invalid_event_rolls_back_triggered_publication(self):
        tape = self.waiting_tape()
        collector = self.connect(tape)
        collector.feed(tape.segment(0, 4))
        before = self.full_state(collector)
        bad = tape.segment(4, 4)
        completing = tape.part("X", 0, "中".encode("utf-8"))
        bad["events"] = [copy.deepcopy(completing),
                         dict(copy.deepcopy(completing), source_position=6,
                              event_id="e6", payload={"object_id": "X", "index": 0,
                                                      "data": "%%%="})]
        with self.assertRaises(EnvelopeError):
            collector.feed(bad)
        self.assertEqual(self.full_state(collector), before)
        self.assertEqual(collector.archive.read("documents"), [])
        self.assertEqual(collector.checkpoint().unfinished_top_ids, ("t",))
        result = collector.feed(tape.segment(4, 5))
        self.assertEqual(result.completed_top_ids, ("t",))
        self.assertEqual(collector.archive.read("documents"), [{"id": 1, "text": "中"}])

    def test_hook_failure_rolls_back_triggered_publication(self):
        tape = self.waiting_tape()
        collector = self.connect(tape)
        collector.feed(tape.segment(0, 4))
        tape.part("X", 0, "中".encode("utf-8"))
        for stage in ("after_rows", "before_receipt", "before_commit"):
            with self.subTest(stage=stage):
                before = self.full_state(collector)
                ack_calls = list(collector.source.ack_calls)
                failure = sqlite3.OperationalError("injected " + stage)

                def fail_at(actual):
                    if actual == stage:
                        raise failure
                collector.hooks = PublicationHooks(fail_at)
                with self.assertRaises(sqlite3.OperationalError) as caught:
                    collector.feed(tape.segment(4, 5))
                self.assertIs(caught.exception, failure)
                self.assertEqual(self.full_state(collector), before)
                self.assertEqual(collector.source.ack_calls, ack_calls)
                collector = self.reopen(collector, tape)
                self.assertEqual(self.full_state(collector), before)
                collector.hooks = PublicationHooks()
        result = collector.feed(tape.segment(4, 5))
        self.assertEqual(result.completed_top_ids, ("t",))
        self.assertEqual(self.receipts(collector), [("t", 1)])


class TestInterleavedAndRecovery(ObjectCase):
    def test_interleaved_commits_release_in_order(self):
        tape = ObjectTape()
        tape.add("begin", top="a")
        tape.add("begin", top="b")
        tape.row("insert", 1, {"$object": "X"}, top="a")
        tape.row("insert", 2, "ready", top="b")
        tape.add("commit", top="b")       # b commits first and is ready
        tape.add("commit", top="a")       # a waits for X
        tape.seal("X", ["完".encode("utf-8")], top="a")
        tape.part("X", 0, "完".encode("utf-8"), top="a")
        engine = ReferenceEngine()
        collector = self.drive(tape, engine)
        self.assertEqual(self.receipts(collector), [("b", 1), ("a", 2)])
        self.assertEqual(collector.archive.read("documents"),
                         [{"id": 1, "text": "完"}, {"id": 2, "text": "ready"}])

    def test_ready_successor_cannot_overtake_waiting_predecessor(self):
        tape = ObjectTape()
        tape.add("begin", top="a")
        tape.row("insert", 1, {"$object": "X"}, top="a")
        tape.add("commit", top="a")
        tape.add("begin", top="b")
        tape.row("insert", 2, "ready", top="b")
        tape.add("commit", top="b")
        tape.seal("X", ["先".encode("utf-8")], top="a")
        tape.part("X", 0, "先".encode("utf-8"), top="a")
        engine = ReferenceEngine()
        collector = self.drive(tape, engine)
        self.assertEqual(self.receipts(collector), [("a", 1), ("b", 2)])

    def test_lost_acknowledgement_keeps_waiting_state(self):
        tape = ObjectTape()
        tape.add("begin")
        tape.row("insert", 1, {"$object": "X"})
        tape.add("commit")
        tape.seal("X", ["中".encode("utf-8")])
        tape.part("X", 0, "中".encode("utf-8"))
        collector = self.connect(tape, lose_ack_once=(3,))
        result = collector.feed(tape.segment(0, 3))
        self.assertEqual(result.completed_top_ids, ())
        checkpoint = collector.checkpoint()
        self.assertEqual((checkpoint.accepted_position, checkpoint.confirmed_position,
                          checkpoint.pending_position), (3, 0, 3))
        self.assertIsInstance(collector.acknowledgements.last_error, OSError)
        self.assertEqual(checkpoint.unfinished_top_ids, ("t",))
        collector = self.reopen(collector, tape)
        checkpoint = collector.checkpoint()
        self.assertEqual(checkpoint.unfinished_top_ids, ("t",))
        self.assertEqual(checkpoint.pending_position, 3)
        self.assertEqual(collector.archive.read("documents"), [])
        result = collector.feed(tape.segment(3))
        self.assertEqual(result.completed_top_ids, ("t",))
        self.assertEqual(collector.checkpoint().confirmed_position, 5)
        self.assertEqual(self.receipts(collector), [("t", 1)])
        after = self.full_state(collector)
        replay = collector.feed(tape.segment(2, 3))
        self.assertEqual(replay.completed_top_ids, ())
        self.assertEqual(self.full_state(collector), after)

    def test_recovery_rejects_tampered_object_state(self):
        tape = ObjectTape()
        tape.add("begin")
        tape.row("insert", 1, {"$object": "X"})
        tape.add("commit")
        tape.seal("X", ["中".encode("utf-8")[:2], "中".encode("utf-8")[2:]])
        tape.part("X", 0, "中".encode("utf-8")[:2])
        collector = self.connect(tape)
        collector.feed(tape.segment())
        self.assertEqual(collector.checkpoint().unfinished_top_ids, ("t",))
        collector.close()
        self.collectors.remove(collector)
        tampering = [
            "DELETE FROM object_parts WHERE idx=0",
            "UPDATE object_parts SET data=NULL",
            "UPDATE identities SET canonical=NULL WHERE kind='object_part'",
            "UPDATE identities SET canonical=NULL WHERE kind='commit'",
            "DELETE FROM identities WHERE kind='object_part'",
            "UPDATE objects SET content=x'00',complete=1",
            "UPDATE objects SET reclaimed=1",
            "DELETE FROM pending",
            "DELETE FROM pending_refs",
        ]
        for index, statement in enumerate(tampering):
            with self.subTest(statement=statement):
                scratch = Path(self.directory.name) / ("copy" + str(index) + ".sqlite")
                connection = sqlite3.connect(self.path)
                backup = sqlite3.connect(scratch)
                connection.backup(backup)
                backup.close()
                connection.close()
                connection = sqlite3.connect(scratch)
                connection.execute(statement)
                connection.commit()
                connection.close()
                source = ReplaySource(session(), tape.events)
                with self.assertRaises(RecoveryConsistencyError):
                    open_collector(scratch, source)
                self.assertTrue(source.closed)


class TestReclamation(ObjectCase):
    def complete_tape(self):
        tape = ObjectTape()
        tape.add("begin")
        tape.row("insert", 1, {"$object": "X"})
        tape.add("commit")
        tape.seal("X", ["中".encode("utf-8")])
        tape.part("X", 0, "中".encode("utf-8"))
        return tape

    def test_reclaim_after_confirmation_and_publication(self):
        tape = self.complete_tape()
        collector = self.connect(tape)
        result = collector.feed(tape.segment())
        self.assertEqual(result.completed_top_ids, ("t",))
        self.assertEqual(collector.checkpoint().confirmed_position, 5)
        connection = collector.database.connection
        objects = connection.execute("SELECT content,reclaimed FROM objects").fetchall()
        self.assertEqual(objects, [(None, 1)])
        parts = connection.execute("SELECT data,digest FROM object_parts").fetchall()
        self.assertEqual([data for data, _ in parts], [None])
        self.assertEqual(len(parts[0][1]), 64)
        identities = connection.execute(
            "SELECT canonical,digest,kind FROM identities WHERE kind='object_part'").fetchall()
        self.assertEqual([(canonical, len(digest)) for canonical, digest, _ in identities],
                         [(None, 64)])
        self.assertEqual(connection.execute("SELECT COUNT(*) FROM payloads").fetchone()[0], 0)
        self.assertIsNone(collector.last_maintenance_error)
        # Published rows are archive results and survive reclamation.
        self.assertEqual(collector.archive.read("documents"), [{"id": 1, "text": "中"}])
        # Exact replay stays idempotent and never rebuilds reclaimed objects.
        before = self.full_state(collector)
        replay = collector.feed(tape.segment())
        self.assertEqual(replay.completed_top_ids, ())
        self.assertEqual(self.full_state(collector), before)
        collector = self.reopen(collector, tape)
        self.assertEqual(self.full_state(collector), before)
        replay = collector.feed(tape.segment(3))
        self.assertEqual(replay.completed_top_ids, ())
        self.assertEqual(self.full_state(collector), before)
        # Same identity with changed content is still a conflict.
        changed = tape.segment()
        changed["events"][4]["payload"]["data"] = base64.b64encode(b"xx").decode("ascii")
        with self.assertRaises(EventConflictError):
            collector.feed(changed)
        self.assertEqual(self.full_state(collector), before)

    def test_reclaim_waits_for_confirmation(self):
        tape = self.complete_tape()
        tape.add("begin", top="other")
        tape.add("commit", top="other")
        collector = self.connect(tape, lose_ack_once=(5,))
        collector.feed(tape.segment(0, 5))
        self.assertEqual(collector.checkpoint().confirmed_position, 0)
        connection = collector.database.connection
        self.assertEqual(connection.execute(
            "SELECT content IS NULL FROM objects").fetchone()[0], 0)
        self.assertEqual(connection.execute(
            "SELECT COUNT(*) FROM object_parts WHERE data IS NOT NULL").fetchone()[0], 1)
        collector.feed(tape.segment(5))
        self.assertEqual(collector.checkpoint().confirmed_position, 7)
        self.assertEqual(connection.execute(
            "SELECT content IS NULL,reclaimed FROM objects").fetchall(), [(1, 1)])
        self.assertEqual(connection.execute(
            "SELECT COUNT(*) FROM object_parts WHERE data IS NOT NULL").fetchone()[0], 0)

    def test_reclaim_keeps_content_needed_by_unpublished(self):
        tape = ObjectTape()
        tape.add("begin")
        tape.row("insert", 1, {"$object": "X"})
        tape.row("insert", 2, "plain")
        tape.seal("Z", ["多".encode("utf-8")])
        tape.part("Z", 0, "多".encode("utf-8"))
        tape.seal("X", ["余".encode("utf-8")[:2], "余".encode("utf-8")[2:]])
        tape.part("X", 0, "余".encode("utf-8")[:2])
        tape.add("commit")
        tape.part("X", 1, "余".encode("utf-8")[2:])
        collector = self.connect(tape)
        collector.feed(tape.segment(0, 8))
        self.assertEqual(collector.checkpoint().unfinished_top_ids, ("t",))
        self.assertEqual(collector.checkpoint().confirmed_position, 8)
        connection = collector.database.connection
        # Z is complete and unreferenced: reclaimed. X is needed: retained.
        states = dict(connection.execute(
            "SELECT object_id,reclaimed FROM objects WHERE top_id='t'").fetchall())
        self.assertEqual(states, {"X": 0, "Z": 1})
        self.assertEqual(connection.execute(
            "SELECT data IS NULL FROM object_parts WHERE object_id='X'").fetchall(),
            [(0,)])
        self.assertEqual(connection.execute(
            "SELECT data IS NULL FROM object_parts WHERE object_id='Z'").fetchall(),
            [(1,)])
        # The shared raw segment is still referenced by the waiting top, so it
        # is retained whole even though Z's own copies were scrubbed.
        from capture.intake.retention import PayloadStore
        self.assertEqual(len(PayloadStore(connection).retained_ids()), 1)
        before = self.full_state(collector)
        collector = self.reopen(collector, tape)
        self.assertEqual(self.full_state(collector), before)
        connection = collector.database.connection
        result = collector.feed(tape.segment(8))
        self.assertEqual(result.completed_top_ids, ("t",))
        self.assertEqual(collector.archive.read("documents"),
                         [{"id": 1, "text": "余"}, {"id": 2, "text": "plain"}])
        states = dict(connection.execute(
            "SELECT object_id,reclaimed FROM objects WHERE top_id='t'").fetchall())
        self.assertEqual(states, {"X": 1, "Z": 1})


class TestGeneratedScenarios(ObjectCase):
    """Deterministic generated interleavings checked against the reference
    engine at every acceptance boundary, with reopen and replay each step."""

    def generate(self, seed):
        rng = random.Random(seed)
        tape = ObjectTape()
        engine = ReferenceEngine()
        counter = {"key": 0, "object": 0}
        bodies = ["中文", "末尾", "", "aé中", "完整行的字段内容", "plain"]
        object_plans = {}

        def emit(kind, **kwargs):
            event = tape.add(kind, **kwargs)
            engine.accept(event)
            return event

        def object_event_allowed(top):
            return (engine.tops[top]["state"] == "active" or top in engine.pending)

        def new_object(top):
            counter["object"] += 1
            object_id = "O" + str(counter["object"])
            body = rng.choice(bodies).encode("utf-8")
            cuts = sorted(rng.randrange(0, len(body) + 1)
                          for _ in range(rng.randrange(0, 3)))
            pieces = [body[left:right] for left, right in
                      zip([0] + cuts, cuts + [len(body)])]
            object_plans.setdefault(top, {})[object_id] = {
                "pieces": pieces, "sealed": False, "delivered": set()}
            return object_id

        def maybe_seal(top, object_id):
            plan = object_plans[top][object_id]
            if plan["sealed"] or not object_event_allowed(top):
                return
            plan["sealed"] = True
            pieces = plan["pieces"]
            body = b"".join(pieces)
            emit("object_seal", top=top, payload={
                "object_id": object_id, "parts": len(pieces), "size": len(body),
                "sha256": hashlib.sha256(body).hexdigest()})

        def maybe_deliver(top, object_id, chance):
            plan = object_plans[top][object_id]
            for index in rng.sample(range(len(plan["pieces"])), len(plan["pieces"])):
                if (index not in plan["delivered"] and rng.random() < chance and
                        object_event_allowed(top)):
                    plan["delivered"].add(index)
                    emit("object_part", top=top, payload={
                        "object_id": object_id, "index": index,
                        "data": base64.b64encode(plan["pieces"][index]).decode("ascii")})

        for index in range(8):
            top = "g" + str(index)
            emit("begin", top=top)
            if rng.random() < 0.4:
                emit("branch", top=top, branch="child", parent="root")
            own_objects = []
            for _ in range(rng.randrange(1, 4)):
                current = engine.logical_rows()
                engine._apply(top, current, resolve=False)
                child_ok = ("child" in engine.tops[top]["branches"] and
                            engine._live(top, "child") and
                            engine.tops[top]["branches"]["child"][1] == "active")
                branch = "child" if child_ok and rng.random() < 0.3 else "root"
                action = rng.random()
                if action < 0.45 or not current:
                    counter["key"] += 1
                    key = counter["key"]
                    if rng.random() < 0.5:
                        object_id = new_object(top)
                        own_objects.append(object_id)
                        emit("row", top=top, branch=branch, payload={
                            "table": "documents", "op": "insert",
                            "row": {"id": key, "text": {"$object": object_id}}})
                    else:
                        emit("row", top=top, branch=branch, payload={
                            "table": "documents", "op": "insert",
                            "row": {"id": key, "text": rng.choice(bodies)}})
                elif action < 0.7:
                    key = rng.choice(sorted(current))
                    if rng.random() < 0.4:
                        object_id = new_object(top)
                        own_objects.append(object_id)
                        emit("row", top=top, branch=branch, payload={
                            "table": "documents", "op": "update", "key": [key],
                            "row": {"id": key, "text": {"$object": object_id}}})
                    else:
                        emit("row", top=top, branch=branch, payload={
                            "table": "documents", "op": "update", "key": [key],
                            "row": {"id": key, "text": rng.choice(bodies)}})
                elif action < 0.85:
                    key = rng.choice(sorted(current))
                    counter["key"] += 1
                    new_key = counter["key"]
                    emit("row", top=top, branch=branch, payload={
                        "table": "documents", "op": "update", "key": [key],
                        "row": {"id": new_key, "text": rng.choice(bodies)}})
                else:
                    key = rng.choice(sorted(current))
                    emit("row", top=top, branch=branch, payload={
                        "table": "documents", "op": "delete", "key": [key]})
            if "child" in engine.tops[top]["branches"] and rng.random() < 0.5:
                emit("rollback", top=top, branch="child")
            for object_id in own_objects:
                if rng.random() < 0.7:
                    maybe_seal(top, object_id)
                maybe_deliver(top, object_id, 0.5)
            if rng.random() < 0.15:
                emit("abort", top=top)
                continue
            emit("commit", top=top)
            for object_id in own_objects:
                maybe_seal(top, object_id)
                maybe_deliver(top, object_id, 0.6)
        # Drain: complete every object that can still accept events so that
        # every committed top publishes by the end of the tape.
        for top, plans in object_plans.items():
            for object_id, plan in plans.items():
                maybe_seal(top, object_id)
                maybe_deliver(top, object_id, 1.0)
        return tape

    def test_generated_interleaved_scenarios(self):
        for seed in (7, 19, 42, 113):
            with self.subTest(seed=seed):
                self.path = Path(self.directory.name) / ("archive-" + str(seed) + ".sqlite")
                tape = self.generate(seed)
                engine = ReferenceEngine()
                collector = self.drive(tape, engine)
                self.assertEqual(collector.checkpoint().unfinished_top_ids, ())
                self.assertEqual(engine.expected_unfinished(), ())
                self.assertEqual(collector.archive.read("documents"),
                                 engine.expected_rows())
