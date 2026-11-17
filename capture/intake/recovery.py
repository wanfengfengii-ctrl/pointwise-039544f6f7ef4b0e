from dataclasses import dataclass
import hashlib
from capture.source.codec import content_digest, decode_canonical
from capture.source.envelope import parse_envelope
from capture.transactions.identity import EventIdentityIndex
from capture.transactions.lineage import Lineage
from capture.transactions.journal import TransactionJournal
from capture.transactions.objects import parse_object_event
from capture.archive.receipts import ReceiptStore
from capture.rows.schema import SchemaCatalog
from capture.rows.keys import key_from_row, encode_key
from capture.rows.operations import parse_row_operation
from capture.rows.workspace import candidate_object_references
from capture.transactions.completion import build_commit_candidate
from capture.intake.checkpoints import CheckpointStore


class RecoveryConsistencyError(ValueError):
    pass


@dataclass(frozen=True)
class RecoveryState:
    checkpoint: object
    retained_payload_ids: tuple


@dataclass(frozen=True)
class _Accepted:
    """One accepted identity fact; event is None once content was scrubbed."""
    event_id: str
    position: int
    session_id: str
    top_id: str
    branch_id: str
    kind: str
    object_id: str | None
    object_index: int | None
    digest: str
    event: object


def _require(condition, message):
    if not condition:
        raise RecoveryConsistencyError(message)


def _load_ledger(connection, session):
    rows = connection.execute(
        "SELECT event_id,position,canonical,digest,session_id,top_id,branch_id,kind,"
        "object_id,object_index FROM identities ORDER BY position").fetchall()
    ledger = {}
    for identifier, position, encoded, digest, session_id, top, branch, kind, \
            object_id, object_index in rows:
        event = None
        if encoded is not None:
            event = parse_envelope(decode_canonical(encoded))
            _require(event.event_id == identifier and event.source_position == position and
                     event.session_id == session_id and event.top_id == top and
                     event.branch_id == branch and event.kind == kind and
                     bytes(encoded) == event.encoded, "identity metadata mismatch")
            _require(digest == content_digest(event.as_dict()), "identity digest mismatch")
            if kind == "object_part":
                _, parsed_id, parsed_index, _ = parse_object_event(event)
                _require(parsed_id == object_id and parsed_index == object_index,
                         "identity object metadata mismatch")
        else:
            _require(kind == "object_part" and object_id is not None and
                     type(object_index) is int, "scrubbed identity is not an object part")
        _require(session_id == session.session_id, "identity session mismatch")
        ledger[position] = _Accepted(identifier, position, session_id, top, branch, kind,
                                     object_id, object_index, digest, event)
    return ledger


def _replay_prefix(ledger, catalog):
    expected_tops, expected_branches = {}, {}
    expected_seals, expected_parts = {}, {}
    for position in sorted(ledger):
        accepted = ledger[position]
        event, top, kind = accepted.event, accepted.top_id, accepted.kind
        branches = expected_branches.setdefault(top, {})
        if kind == "begin":
            _require(top not in expected_tops, "top identity reused")
            expected_tops[top] = [accepted.branch_id, "active", None]
            branches[accepted.branch_id] = [None, "active"]
            continue
        _require(top in expected_tops, "event before begin")
        if kind in ("object_part", "object_seal"):
            _require(expected_tops[top][1] in ("active", "committed"),
                     "object event outside top lifetime")
            _require(accepted.branch_id == expected_tops[top][0],
                     "object event not on root branch")
            if kind == "object_seal":
                _, object_id, parts, size, sha256 = parse_object_event(event)
                declaration = (parts, size, sha256)
                prior = expected_seals.setdefault((top, object_id), declaration)
                _require(prior == declaration, "seal declaration changed")
            elif event is not None:
                _, object_id, index, data = parse_object_event(event)
                parts = expected_parts.setdefault((top, object_id), {})
                if index in parts and parts[index] is not None:
                    _require(parts[index] == data, "part bytes changed")
                else:
                    parts[index] = data
            else:
                expected_parts.setdefault((accepted.top_id, accepted.object_id), {}) \
                    .setdefault(accepted.object_index, None)
            continue
        _require(expected_tops[top][1] == "active", "event outside active top")
        if kind == "branch":
            _require(event.branch_id not in branches and event.parent_id in branches and
                     branches[event.parent_id][1] == "active", "invalid branch establishment")
            branches[event.branch_id] = [event.parent_id, "active"]
        else:
            _require(event.branch_id in branches, "unknown event branch")
            if kind == "row":
                _require(branches[event.branch_id][1] == "active", "row on closed branch")
                parse_row_operation(event, catalog)
            elif kind == "seal":
                _require(branches[event.branch_id][0] is not None and
                         branches[event.branch_id][1] == "active", "invalid seal")
                branches[event.branch_id][1] = "sealed"
            elif kind == "rollback":
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
                state = "committed" if kind == "commit" else "aborted"
                expected_tops[top][1:] = [state, event.event_id]
                if state == "aborted":
                    for branch in branches.values():
                        branch[1] = "aborted"
    return expected_tops, expected_branches, expected_seals, expected_parts


def _validate_objects(connection, ledger, expected_seals, expected_parts):
    actual_objects = {}
    for top, object_id, parts, size, sha256, content, complete, reclaimed in \
            connection.execute("SELECT top_id,object_id,parts,size,sha256,content,complete,"
                               "reclaimed FROM objects"):
        actual_objects[(top, object_id)] = (parts, size, sha256, content, complete, reclaimed)
    _require(set(actual_objects) == set(expected_seals) | set(expected_parts),
             "object set differs from accepted prefix")
    actual_parts = {}
    for top, object_id, index, data, digest in connection.execute(
            "SELECT top_id,object_id,idx,data,digest FROM object_parts"):
        actual_parts.setdefault((top, object_id), {})[index] = (data, digest)
    for key, (parts, size, sha256, content, complete, reclaimed) in actual_objects.items():
        _require(complete in (0, 1) and reclaimed in (0, 1), "object flags are not boolean")
        seal = expected_seals.get(key)
        declared = None if parts is None else (parts, size, sha256)
        _require(declared == seal, "object seal differs from accepted prefix")
        expected_indexes = expected_parts.get(key, {})
        stored = actual_parts.get(key, {})
        _require(set(stored) == set(expected_indexes),
                 "object parts differ from accepted prefix")
        for index, (data, digest) in stored.items():
            _require(type(digest) is str and len(digest) == 64, "part digest missing")
            known = expected_indexes[index]
            if data is None:
                _require(known is None,
                         "part content missing while identity ledger retains it")
            else:
                _require(known is not None and bytes(data) == known,
                         "part bytes differ from accepted prefix")
                _require(hashlib.sha256(bytes(data)).hexdigest() == digest,
                         "part digest does not match content")
        if reclaimed:
            _require(content is None, "reclaimed object retains content")
            _require(all(data is None for data, _ in stored.values()),
                     "reclaimed object retains part content")
            _require(all(known is None for known in expected_indexes.values()),
                     "reclaimed object retains identity content copies")
        present = [index for index, (data, _) in stored.items() if data is not None]
        if complete:
            _require(seal is not None, "complete object without seal")
            if content is None:
                _require(seal[0] == 0 or len(present) != seal[0],
                         "complete object content reassemblable")
            else:
                body = bytes(content)
                _require(len(body) == seal[1] and
                         hashlib.sha256(body).hexdigest() == seal[2],
                         "object content differs from seal")
                body.decode("utf-8")
                if len(present) == seal[0]:
                    assembled = b"".join(bytes(stored[index][0]) for index in range(seal[0]))
                    _require(body == assembled, "object content differs from parts")
        else:
            _require(content is None, "incomplete object retains content")
            _require(seal is None or len(present) != seal[0],
                     "complete object not marked complete")


def validate_recovery_state(database, session):
    connection = database.connection
    catalog = SchemaCatalog(session.tables)
    checkpoint = CheckpointStore(connection).read()
    _require(0 <= checkpoint.confirmed_position <= checkpoint.accepted_position,
             "confirmation outside durable prefix")
    ledger = _load_ledger(connection, session)
    _require(len(ledger) == checkpoint.accepted_position, "identity prefix length mismatch")
    _require(sorted(ledger) == list(range(1, len(ledger) + 1)), "identity prefix positions")
    expected_tops, expected_branches, expected_seals, expected_parts = \
        _replay_prefix(ledger, catalog)
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
    _validate_objects(connection, ledger, expected_seals, expected_parts)
    pending_rows = {top: (position, digest) for top, position, digest in connection.execute(
        "SELECT top_id,commit_position,completion_digest FROM pending")}
    committed = {top for top, (_, state, _) in actual_tops.items() if state == "committed"}
    receipts = ReceiptStore(connection).all()
    receipted = {receipt.top_id for receipt in receipts}
    _require(set(pending_rows) == committed - receipted, "pending set differs from commits")
    completions = {}
    for accepted in ledger.values():
        if accepted.kind == "commit":
            completions[accepted.top_id] = accepted
    for top, (position, digest) in pending_rows.items():
        accepted = completions.get(top)
        _require(accepted is not None and accepted.position == position and
                 digest == content_digest(accepted.event.as_dict()),
                 "pending commit identity mismatch")
    commit_order = [accepted.top_id for accepted in ledger.values() if accepted.kind == "commit"]
    _require([top for top in commit_order if top not in pending_rows] ==
             [receipt.top_id for receipt in receipts],
             "publication order differs from commit order")
    journal = TransactionJournal(connection)
    pending_refs = frozenset(connection.execute("SELECT top_id,object_id FROM pending_refs"))
    expected_refs = set()
    for top in pending_rows:
        candidate = build_commit_candidate(top, "", "", journal, lineage)
        expected_refs |= {(top, object_id)
                          for object_id in candidate_object_references(candidate, catalog)}
    _require(pending_refs == expected_refs, "pending object references differ")
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
            accepted = ledger.get(event.source_position)
            _require(accepted is not None and accepted.event_id == event.event_id,
                     "payload identity mismatch")
            if accepted.event is not None:
                _require(accepted.event.encoded == event.encoded, "payload identity mismatch")
            else:
                _require(accepted.digest == content_digest(event.as_dict()),
                         "payload content differs from scrubbed identity")
        payloads[identifier] = parsed
    referenced_positions = set()
    waiting = set(journal.unfinished_top_ids())
    for position, top, branch, payload_id, offset in connection.execute(
            "SELECT position,top_id,branch_id,payload_id,event_index FROM journal"):
        _require(payload_id in payloads and type(offset) is int and
                 0 <= offset < len(payloads[payload_id]), "missing payload or invalid offset")
        event = payloads[payload_id][offset]
        _require(event.source_position == position and event.top_id == top and
                 event.branch_id == branch and top in waiting,
                 "journal identity mismatch")
        referenced_positions.add(position)
    expected_positions = {position for position, accepted in ledger.items()
                          if accepted.top_id in waiting}
    _require(referenced_positions == expected_positions, "unfinished journal prefix incomplete")
    _require(journal.unfinished_top_ids() == checkpoint.unfinished_top_ids,
             "unfinished top mismatch")
    identities = EventIdentityIndex(connection)
    for top in journal.unfinished_top_ids():
        for event in journal.ordered_events(top):
            _require(identities.classify(event) == "duplicate", "journal identity absent")
    for sequence, receipt in enumerate(receipts, 1):
        accepted = completions.get(receipt.top_id)
        _require(accepted is not None and accepted.event is not None and
                 receipt.event_id == accepted.event_id and
                 receipt.session_id == session.session_id and
                 receipt.publication == sequence and
                 receipt.digest == content_digest(accepted.event.as_dict()),
                 "receipt completion mismatch")
    publication = connection.execute(
        "SELECT publication FROM progress WHERE singleton=1").fetchone()[0]
    _require(publication == len(receipts), "publication sequence mismatch")
    for table, key, encoded in connection.execute(
            "SELECT table_name,key_data,row_data FROM archive_rows"):
        row = catalog.validate_row(table, decode_canonical(encoded))
        _require(not any(type(value) is dict for value in row.values()),
                 "published row retains an object reference")
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
