import copy
import tempfile
import unittest
from pathlib import Path
from capture.source.session import SourceSession
from capture.source.replay import ReplaySource
from capture.intake.collector import open as open_collector


TABLES = {
    "accounts": {"columns": {
        "id": {"type": "int", "nullable": False},
        "value": {"type": "int", "nullable": False}},
        "primary_key": ["id"]},
    "labels": {"columns": {
        "group": {"type": "str", "nullable": False},
        "rank": {"type": "int", "nullable": False},
        "text": {"type": "str", "nullable": True}},
        "primary_key": ["group", "rank"]}
}


def session(name="source", tables=None):
    return SourceSession(name, 1, copy.deepcopy(TABLES if tables is None else tables))


class Tape:
    def __init__(self, session_id="source"):
        self.events = []
        self.session_id = session_id

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

    def row(self, op, key=None, value=None, new_key=None, top="t", branch="root"):
        payload = {"table": "accounts", "op": op}
        if op != "insert":
            payload["key"] = [key]
        if op != "delete":
            payload["row"] = {"id": key if new_key is None else new_key, "value": value}
        return self.add("row", top, branch, payload)

    def segment(self, start=0, stop=None):
        return {"events": copy.deepcopy(self.events[start:stop])}


class CaptureCase(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(prefix="capture-test-")
        self.path = Path(self.directory.name) / "archive.sqlite"
        self.collectors = []

    def tearDown(self):
        for collector in self.collectors:
            collector.close()
        self.directory.cleanup()

    def connect(self, tape, **source_options):
        source = ReplaySource(session(), tape.events, **source_options)
        collector = open_collector(self.path, source)
        self.collectors.append(collector)
        return collector

    def reopen(self, collector, tape, **options):
        collector.close()
        return self.connect(tape, **options)

    def state(self, collector):
        connection = collector.database.connection
        return (collector.archive.read("accounts"), collector.checkpoint(),
                tuple(connection.execute("SELECT * FROM receipts ORDER BY publication")),
                tuple(connection.execute("SELECT * FROM identities ORDER BY position")),
                tuple(connection.execute("SELECT * FROM tops ORDER BY top_id")),
                tuple(connection.execute("SELECT * FROM branches ORDER BY top_id,branch_id")),
                tuple(connection.execute("SELECT * FROM journal ORDER BY position")),
                tuple(connection.execute("SELECT * FROM payloads ORDER BY payload_id")),
                tuple(connection.execute("SELECT * FROM progress")))

    def receipts(self, collector):
        return collector.database.connection.execute(
            "SELECT top_id,publication FROM receipts ORDER BY publication").fetchall()


def reference_accounts(events):
    published, transactions = {}, {}
    for event in events:
        top, branch, kind = event["top_id"], event["branch_id"], event["kind"]
        if kind == "begin":
            transactions[top] = {"parents": {branch: None}, "dead": set(), "rows": []}
            continue
        transaction = transactions[top]
        if kind == "branch":
            transaction["parents"][branch] = event["parent_id"]
        elif kind == "rollback":
            transaction["dead"].add(branch)
        elif kind == "row":
            transaction["rows"].append((branch, event["payload"]))
        elif kind == "commit":
            work = copy.deepcopy(published)
            for owner, operation in transaction["rows"]:
                ancestry = []
                while owner is not None:
                    ancestry.append(owner)
                    owner = transaction["parents"][owner]
                if any(item in transaction["dead"] for item in ancestry):
                    continue
                op = operation["op"]
                if op == "insert":
                    row = operation["row"]
                    if row["id"] in work:
                        raise AssertionError("invalid reference insert")
                    work[row["id"]] = copy.deepcopy(row)
                elif op == "delete":
                    del work[operation["key"][0]]
                else:
                    old = operation["key"][0]
                    if old not in work:
                        raise AssertionError("invalid reference update")
                    row = operation["row"]
                    if row["id"] != old and row["id"] in work:
                        raise AssertionError("invalid reference move")
                    del work[old]
                    work[row["id"]] = copy.deepcopy(row)
            published = work
    return [published[key] for key in sorted(published)]


def rollback_tape(mode="update"):
    tape = Tape()
    tape.add("begin", top="seed")
    tape.row("insert", key=1, value=0, top="seed")
    tape.add("commit", top="seed")
    tape.add("begin")
    tape.row("update", key=1, value=10)
    tape.add("branch", branch="child", parent="root")
    if mode == "delete":
        tape.row("delete", key=1, branch="child")
    elif mode == "move":
        tape.row("update", key=1, new_key=2, value=20, branch="child")
    else:
        tape.row("update", key=1, value=20, branch="child")
    tape.add("branch", branch="grandchild", parent="child")
    tape.row("insert", key=3, value=30, branch="grandchild")
    tape.add("seal", branch="grandchild")
    tape.add("seal", branch="child")
    tape.add("branch", branch="sibling", parent="root")
    tape.row("insert", key=4, value=40, branch="sibling")
    tape.add("rollback", branch="child")
    tape.add("commit")
    return tape
