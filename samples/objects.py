"""Finite public inputs and observations; no V2 implementation."""
import argparse
import base64
import copy
import hashlib
import json
import tempfile
from pathlib import Path

from capture.source.codec import canonical_bytes
from capture.source.session import SourceSession

TABLES = {
    "documents": {
        "columns": {
            "id": {"type": "int", "nullable": False},
            "text": {"type": "str", "nullable": False},
        },
        "primary_key": ["id"],
    }
}


def session():
    return SourceSession("objects-demo", 1, copy.deepcopy(TABLES))


class Tape:
    def __init__(self):
        self.events = []

    def add(self, kind, payload=None, *, top="t", branch="root", parent=None):
        position = len(self.events) + 1
        event = {
            "session_id": "objects-demo",
            "source_position": position,
            "event_id": "obj-e" + str(position),
            "top_id": top,
            "branch_id": branch,
            "kind": kind,
            "payload": {} if payload is None else copy.deepcopy(payload),
        }
        if kind == "branch":
            event["parent_id"] = parent
        self.events.append(event)
        return event

    def part(self, object_id, index, data, *, top="t"):
        return self.add("object_part", {
            "object_id": object_id,
            "index": index,
            "data": base64.b64encode(data).decode("ascii"),
        }, top=top)

    def seal(self, object_id, pieces, *, top="t"):
        body = b"".join(pieces)
        return self.add("object_seal", {
            "object_id": object_id,
            "parts": len(pieces),
            "size": len(body),
            "sha256": hashlib.sha256(body).hexdigest(),
        }, top=top)

    def row(self, op, key, text=None, *, new_key=None, top="t", branch="root"):
        payload = {"table": "documents", "op": op}
        if op != "insert":
            payload["key"] = [key]
        if op != "delete":
            payload["row"] = {
                "id": key if new_key is None else new_key,
                "text": copy.deepcopy(text),
            }
        return self.add("row", payload, top=top, branch=branch)


def make_case(name):
    if name not in ("withdrawn", "ordered"):
        raise ValueError("unknown public case")
    tape = Tape()
    tape.add("begin")
    tape.row("insert", 1, {"$object": "X"})
    tape.add("branch", branch="child", parent="root")
    tape.row("update", 1, {"$object": "Y"}, branch="child")
    tape.add("rollback", branch="child")
    body = "中文".encode("utf-8")
    pieces = [body[:2], body[2:]]
    tape.seal("X", pieces)
    tape.part("X", 0, pieces[0])
    tape.add("commit")
    observations = [
        {"through": 8, "rows": [], "unfinished": ["t"], "receipts": []}
    ]
    if name == "ordered":
        tape.add("begin", top="next")
        tape.row("update", 1, "后继", new_key=2, top="next")
        tape.add("commit", top="next")
        observations.append({
            "through": 11, "rows": [], "unfinished": ["next", "t"], "receipts": []
        })
    tape.part("X", 1, pieces[1])
    completions = ["t"] if name == "withdrawn" else ["t", "next"]
    final_rows = [{"id": 1, "text": "中文"}] if name == "withdrawn" else [
        {"id": 2, "text": "后继"}
    ]
    observations.append({
        "through": len(tape.events),
        "rows": final_rows,
        "unfinished": [],
        "receipts": [[top, index + 1] for index, top in enumerate(completions)],
    })
    return {
        "events": tape.events,
        "observations": observations,
        "last_completed": completions,
        "lost_ack_position": 8,
    }


def require(condition, message):
    if not condition:
        raise AssertionError(message)


def selfcheck():
    from capture.rows.schema import SchemaCatalog
    SchemaCatalog(TABLES)
    for name in ("withdrawn", "ordered"):
        case = make_case(name)
        events = case["events"]
        canonical_bytes(events)
        require([e["source_position"] for e in events] ==
                list(range(1, len(events) + 1)), "fixture positions")
        require(len({e["event_id"] for e in events}) == len(events), "fixture ids")
        parts = [e["payload"] for e in events if e["kind"] == "object_part"]
        ordered = sorted(parts, key=lambda p: p["index"])
        body = b"".join(base64.b64decode(p["data"], validate=True) for p in ordered)
        seal = next(e["payload"] for e in events if e["kind"] == "object_seal")
        require(seal["parts"] == len(parts) and seal["size"] == len(body), "fixture seal")
        require(seal["sha256"] == hashlib.sha256(body).hexdigest(), "fixture digest")
        require(body.decode("utf-8") == "中文", "fixture text")
        require(all(p["object_id"] == "X" for p in parts), "Y must remain absent")
        require(case["observations"][-1]["through"] == len(events), "fixture endpoint")
    print("OBJECT_FIXTURES_OK (materials only)")


def observe(collector, reader):
    from capture.archive.receipts import ReceiptStore
    cp = collector.checkpoint()
    return {
        "through": cp.accepted_position,
        "rows": reader.read("documents"),
        "unfinished": list(cp.unfinished_top_ids),
        "receipts": [[r.top_id, r.publication]
                     for r in ReceiptStore(collector.database.connection).all()],
    }


def run_case(name, display=False):
    from capture.archive.query import Archive
    from capture.intake.collector import open as open_collector
    from capture.source.replay import ReplaySource
    case = make_case(name)
    expected = {item["through"]: item for item in case["observations"]}
    events = case["events"]
    with tempfile.TemporaryDirectory(prefix="object-capture-") as directory:
        path = Path(directory) / "archive.sqlite"
        collector = None
        try:
            for index, event in enumerate(events, 1):
                source = ReplaySource(
                    session(), events,
                    lose_ack_once=(case["lost_ack_position"],) if index == 8 else (),
                    truncate_on_ack=True,
                )
                collector = open_collector(path, source)
                result = collector.feed({"events": [copy.deepcopy(event)]})
                wanted = case["last_completed"] if index == len(events) else []
                require(list(result.completed_top_ids) == wanted,
                        name + ": unexpected completion at " + str(index))
                require(result.accepted_position == index, name + ": accepted prefix")
                if index == 8:
                    require(collector.checkpoint().confirmed_position == 7,
                            name + ": lost confirmation became confirmed")
                    require(isinstance(collector.acknowledgements.last_error, OSError),
                            name + ": original acknowledgement error missing")
                with Archive(path) as reader:
                    actual = observe(collector, reader)
                if index in expected:
                    require(actual == expected[index], name + ": observation " + repr(actual))
                    if display:
                        print(name, json.dumps(actual, ensure_ascii=False, sort_keys=True))
                # Reopen using a fresh source; no in-memory object state can survive.
                collector.close()
                collector = open_collector(path, ReplaySource(session(), events))
                with Archive(path) as reader:
                    require(observe(collector, reader) == actual, name + ": reopen changed state")
                replay = collector.feed({"events": [copy.deepcopy(event)]})
                require(replay.completed_top_ids == (), name + ": replay republished")
                with Archive(path) as reader:
                    require(observe(collector, reader) == actual, name + ": replay changed state")
                collector.close()
                collector = None
            if display:
                print(name, "completed from real archive")
        finally:
            if collector is not None:
                collector.close()


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("selfcheck", "verify", "demo"))
    args = parser.parse_args(argv)
    if args.action == "selfcheck":
        selfcheck()
    else:
        for name in ("withdrawn", "ordered"):
            run_case(name, display=args.action == "demo")
        if args.action == "verify":
            print("OBJECT_SCENARIOS_OK")


if __name__ == "__main__":
    main()
