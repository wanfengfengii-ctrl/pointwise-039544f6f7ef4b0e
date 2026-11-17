import argparse
from dataclasses import dataclass
from pathlib import Path
import tempfile
from capture.source.session import validate_session
from capture.source.segments import parse_segment, split_accepted_prefix
from capture.source.replay import ReplaySource, load_replay_events
from capture.transactions.assembly import TransactionAssembly
from capture.transactions.identity import EventConflictError
from capture.rows.schema import SchemaCatalog
from capture.rows.operations import parse_row_operation
from capture.rows.workspace import RowWorkspace
from capture.archive.database import ArchiveDatabase
from capture.archive.publication import Publisher, PublicationHooks
from capture.archive.query import Archive
from capture.intake.checkpoints import CheckpointStore
from capture.intake.acknowledgements import AcknowledgementCoordinator
from capture.intake.retention import PayloadStore, RetentionManager
from capture.intake.recovery import restore_intake


class CollectorClosedError(RuntimeError):
    pass


@dataclass(frozen=True)
class FeedResult:
    accepted_position: int
    completed_top_ids: tuple


class Collector:
    def __init__(self, database, source, session, hooks=None):
        self.database = database
        self.source = source
        self.session = session
        self.catalog = SchemaCatalog(session.tables)
        self.hooks = hooks if hooks is not None else PublicationHooks()
        self.acknowledgements = AcknowledgementCoordinator(database, source)
        self.archive = Archive(database)
        self.closed = False
        self.last_poll_state = None
        self._feeding = False
        self.last_maintenance_error = None

    def _require_open(self):
        if self.closed:
            raise CollectorClosedError("collector is closed")

    def feed(self, segment):
        self._require_open()
        if self._feeding:
            raise RuntimeError("feed cannot be reentered")
        self._feeding = True
        try:
            parsed = parse_segment(segment)
            for event in parsed.events:
                if event.session_id != self.session.session_id:
                    raise EventConflictError("event belongs to another source session")
                if event.kind == "row":
                    parse_row_operation(event, self.catalog)
            completed = []
            with self.database.write_unit() as connection:
                assembly = TransactionAssembly(connection)
                accepted = CheckpointStore(connection).read().accepted_position
                prefix, suffix = split_accepted_prefix(parsed, accepted)
                for event in prefix:
                    if assembly.identity.classify(event) != "duplicate":
                        raise EventConflictError("accepted prefix identity is absent")
                for event in suffix:
                    if assembly.identity.classify(event) != "new":
                        raise EventConflictError("new suffix reuses old identity")
                if suffix:
                    payload_id = PayloadStore(connection).stage(parsed)
                    workspace = RowWorkspace(self.catalog)
                    publisher = Publisher(connection, self.catalog, self.session.session_id, self.hooks)
                    for index, event in enumerate(parsed.events):
                        if event.source_position <= accepted:
                            continue
                        decision = assembly.accept(event, payload_id, index)
                        if decision.action == "commit":
                            rows = workspace.materialize(decision.candidate, connection)
                            publisher.publish(decision.candidate, rows)
                            assembly.journal.finish(event.top_id, "committed", event.event_id)
                            completed.append(event.top_id)
                        elif event.kind in ("row", "rollback"):
                            workspace.materialize(assembly.preview(event.top_id), connection)
                    CheckpointStore(connection).advance_accepted(parsed.last_position)
                    self.hooks.hit("before_commit")
            self.acknowledgements.flush_pending()
            self._reclaim()
            return FeedResult(self.checkpoint().accepted_position, tuple(completed))
        finally:
            self._feeding = False

    def checkpoint(self):
        self._require_open()
        with self.database.read_snapshot() as connection:
            return CheckpointStore(connection).read()

    def poll_once(self):
        self._require_open()
        self.acknowledgements.flush_pending()
        position = self.checkpoint().accepted_position
        result = self.source.poll(position)
        if result.state not in ("segment", "idle", "end"):
            raise ValueError("unknown source poll result")
        self.last_poll_state = result.state
        if result.state == "segment":
            if result.segment is None:
                raise ValueError("source omitted segment")
            return self.feed(result.segment)
        if result.segment is not None:
            raise ValueError("non-segment poll contains data")
        self._reclaim()
        return None

    def _reclaim(self):
        try:
            with self.database.write_unit() as connection:
                RetentionManager(connection).reclaim()
            self.last_maintenance_error = None
        except Exception as exc:
            self.last_maintenance_error = exc

    def close(self):
        if self.closed:
            return
        if self._feeding:
            raise RuntimeError("cannot close during feed")
        self.closed = True
        try:
            self.source.close()
        finally:
            self.database.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()


def open(db_path, source, *, hooks=None):
    database = None
    try:
        session = validate_session(source.describe())
        SchemaCatalog(session.tables)
        database = ArchiveDatabase(db_path, session)
        restore_intake(database, session)
        return Collector(database, source, session, hooks)
    except BaseException:
        try:
            source.close()
        finally:
            if database is not None:
                database.close()
        raise


def main(argv=None):
    parser = argparse.ArgumentParser(description="Offline transaction capture")
    parser.add_argument("action", choices=("demo", "verify"))
    args = parser.parse_args(argv)
    if args.action == "verify":
        import unittest
        suite = unittest.defaultTestLoader.discover("tests")
        return 0 if unittest.TextTestRunner(verbosity=2).run(suite).wasSuccessful() else 1
    from capture.source.session import SourceSession
    from capture.source.codec import canonical_bytes
    events = load_replay_events(Path(__file__).resolve().parents[2] / "samples/events.json")
    tables = {"accounts": {"columns": {
        "id": {"type": "int", "nullable": False},
        "value": {"type": "int", "nullable": False}},
        "primary_key": ["id"]}}
    session = SourceSession("demo", 1, tables)
    with tempfile.TemporaryDirectory(prefix="capture-demo-") as directory:
        path = Path(directory) / "archive.sqlite"
        source = ReplaySource(session, events, batch_size=1, lose_ack_once=(5,))
        collector = open(path, source)
        for _ in range(5):
            collector.poll_once()
        print("before commit:", collector.archive.read("accounts"))
        collector.close()
        with open(path, ReplaySource(session, events, batch_size=2)) as collector:
            while True:
                collector.poll_once()
                if collector.last_poll_state == "end":
                    break
            print("after reopen:", collector.archive.read("accounts"))
            print("checkpoint:", collector.checkpoint())
            print("receipts:", collector.database.connection.execute(
                "SELECT top_id,publication FROM receipts ORDER BY publication").fetchall())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
