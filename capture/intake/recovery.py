from dataclasses import dataclass
from capture.source.codec import canonical_bytes, content_digest, decode_canonical
from capture.source.envelope import parse_envelope
from capture.transactions.identity import EventIdentityIndex
from capture.transactions.lineage import Lineage
from capture.transactions.journal import TransactionJournal
from capture.archive.receipts import ReceiptStore
from capture.rows.schema import SchemaCatalog
from capture.rows.keys import key_from_row, encode_key
from capture.rows.operations import parse_row_operation
from capture.intake.checkpoints import CheckpointStore


class RecoveryConsistencyError(ValueError):
    pass


@dataclass(frozen=True)
class RecoveryState:
    checkpoint: object
    retained_payload_ids: tuple


def _require(condition, message):
    if not condition:
        raise RecoveryConsistencyError(message)


def validate_recovery_state(database, session):
    connection = database.connection
    catalog = SchemaCatalog(session.tables)
    checkpoint = CheckpointStore(connection).read()
    _require(0 <= checkpoint.confirmed_position <= checkpoint.accepted_position,
             "confirmation outside durable prefix")
    ledger = {}
    expected_tops, expected_branches = {}, {}
    rows = connection.execute(
        "SELECT event_id,position,canonical FROM identities ORDER BY position").fetchall()
    _require(len(rows) == checkpoint.accepted_position, "identity prefix length mismatch")
    for expected_position, (identifier, position, encoded) in enumerate(rows, 1):
        event = parse_envelope(decode_canonical(encoded))
        _require(position == expected_position == event.source_position and
                 identifier == event.event_id and event.session_id == session.session_id and
                 bytes(encoded) == event.encoded, "identity prefix mismatch")
        ledger[position] = event
        top = event.top_id
        branches = expected_branches.setdefault(top, {})
        if event.kind == "begin":
            _require(top not in expected_tops, "top identity reused")
            expected_tops[top] = [event.branch_id, "active", None]
            branches[event.branch_id] = [None, "active"]
            continue
        _require(top in expected_tops and expected_tops[top][1] == "active",
                 "event outside active top")
        if event.kind == "branch":
            _require(event.branch_id not in branches and event.parent_id in branches and
                     branches[event.parent_id][1] == "active", "invalid branch establishment")
            branches[event.branch_id] = [event.parent_id, "active"]
        else:
            _require(event.branch_id in branches, "unknown event branch")
            if event.kind == "row":
                _require(branches[event.branch_id][1] == "active", "row on closed branch")
                parse_row_operation(event, catalog)
            elif event.kind == "seal":
                _require(branches[event.branch_id][0] is not None and
                         branches[event.branch_id][1] == "active", "invalid seal")
                branches[event.branch_id][1] = "sealed"
            elif event.kind == "rollback":
                _require(branches[event.branch_id][0] is not None and
                         branches[event.branch_id][1] != "aborted", "invalid rollback")
                doomed = {event.branch_id}
                while True:
                    expanded = doomed | {b for b, (p, _) in branches.items() if p in doomed}
                    if expanded == doomed:
                        break
                    doomed = expanded
                for branch in doomed:
                    branches[branch][1] = "aborted"
            else:
                _require(event.branch_id == expected_tops[top][0], "completion not on root")
                state = "committed" if event.kind == "commit" else "aborted"
                expected_tops[top][1:] = [state, event.event_id]
                if state == "aborted":
                    for branch in branches.values():
                        branch[1] = "aborted"
    actual_tops = {top: [root, state, completion] for top, root, state, completion in
                   connection.execute("SELECT top_id,root_id,state,completion_id FROM tops")}
    _require(actual_tops == expected_tops, "top state differs from accepted prefix")
    actual_branches = {}
    for top, branch, parent, state in connection.execute(
            "SELECT top_id,branch_id,parent_id,state FROM branches"):
        actual_branches.setdefault(top, {})[branch] = [parent, state]
    _require(actual_branches == expected_branches, "lineage differs from accepted prefix")
    lineage = Lineage(connection)
    for top, branches in actual_branches.items():
        for branch in branches:
            lineage.is_live(top, branch)
    payloads = {}
    for identifier, last, body in connection.execute(
            "SELECT payload_id,last_position,body FROM payloads"):
        raw = decode_canonical(body)
        _require(type(raw) is list and bool(raw), "empty retained payload")
        parsed = tuple(parse_envelope(item) for item in raw)
        _require(last == parsed[-1].source_position <= checkpoint.accepted_position,
                 "payload exceeds durable prefix")
        for left, right in zip(parsed, parsed[1:]):
            _require(right.source_position == left.source_position + 1, "payload position gap")
        for event in parsed:
            _require(event.source_position in ledger and
                     ledger[event.source_position].encoded == event.encoded, "payload identity mismatch")
        payloads[identifier] = parsed
    referenced_positions = set()
    for position, top, branch, payload_id, offset in connection.execute(
            "SELECT position,top_id,branch_id,payload_id,event_index FROM journal"):
        _require(payload_id in payloads and type(offset) is int and
                 0 <= offset < len(payloads[payload_id]), "missing payload or invalid offset")
        event = payloads[payload_id][offset]
        _require(event.source_position == position and event.top_id == top and
                 event.branch_id == branch and actual_tops[top][1] == "active",
                 "journal identity mismatch")
        referenced_positions.add(position)
    expected_positions = {p for p, e in ledger.items() if actual_tops[e.top_id][1] == "active"}
    _require(referenced_positions == expected_positions, "unfinished journal prefix incomplete")
    journal = TransactionJournal(connection)
    _require(journal.unfinished_top_ids() == checkpoint.unfinished_top_ids,
             "unfinished top mismatch")
    identities = EventIdentityIndex(connection)
    for top in journal.unfinished_top_ids():
        for event in journal.ordered_events(top):
            _require(identities.classify(event) == "duplicate", "journal identity absent")
    receipts = ReceiptStore(connection).all()
    committed = {top for top, (_, state, _) in actual_tops.items() if state == "committed"}
    _require({r.top_id for r in receipts} == committed, "receipt coverage mismatch")
    completions = {e.event_id: e for e in ledger.values() if e.kind == "commit"}
    _require(tuple(r.event_id for r in receipts) == tuple(completions),
             "receipt order differs from commit order")
    for sequence, receipt in enumerate(receipts, 1):
        event = completions.get(receipt.event_id)
        _require(event is not None and event.top_id == receipt.top_id and
                 receipt.session_id == session.session_id and receipt.publication == sequence and
                 receipt.digest == content_digest(event.as_dict()), "receipt completion mismatch")
    publication = connection.execute(
        "SELECT publication FROM progress WHERE singleton=1").fetchone()[0]
    _require(publication == len(receipts), "publication sequence mismatch")
    for table, key, encoded in connection.execute(
            "SELECT table_name,key_data,row_data FROM archive_rows"):
        row = catalog.validate_row(table, decode_canonical(encoded))
        schema = catalog.require_table(table)
        _require(encode_key(key_from_row(schema, row)) == key, "archive key mismatch")
    return RecoveryState(checkpoint, tuple(sorted(payloads)))


def restore_intake(database, session):
    try:
        with database.read_snapshot():
            return validate_recovery_state(database, session)
    except RecoveryConsistencyError:
        raise
    except (ValueError, KeyError, IndexError, TypeError) as exc:
        raise RecoveryConsistencyError(str(exc)) from exc
