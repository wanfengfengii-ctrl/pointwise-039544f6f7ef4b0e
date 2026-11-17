from capture.transactions.journal import TransactionJournal
from capture.transactions.lineage import Lineage
from capture.transactions.completion import build_commit_candidate
from capture.transactions.objects import ObjectStore
from capture.transactions.pending import PendingStore
from capture.rows.operations import parse_row_operation, apply_row_operation
from capture.rows.workspace import RowWorkspace, apply_candidate, candidate_object_references
from capture.rows.candidate import diff_rows, validate_row_candidate
from capture.archive.publication import Publisher


def logical_base_rows(connection, catalog):
    """Physical archive plus the logical results of committed-unpublished tops.

    Successor row/rollback previews and commit revalidation interpret against
    this base even while the predecessors still lack object strings. Key
    occupancy is content independent, so unresolved references in the overlay
    are harmless. The overlay is never visible through Archive.read and does
    not imply the predecessors have receipts.
    """
    workspace = RowWorkspace(catalog)
    rows = workspace.base_rows(connection)
    journal = TransactionJournal(connection)
    lineage = Lineage(connection)
    for (top_id,) in connection.execute(
            "SELECT top_id FROM pending ORDER BY commit_position"):
        for event in journal.ordered_events(top_id):
            if event.kind == "row" and lineage.is_live(top_id, event.branch_id):
                apply_row_operation(parse_row_operation(event, catalog), catalog, rows)
    return rows


def publish_ready(connection, catalog, session_id, hooks):
    """Publish the longest ready prefix of waiting commits, in commit order.

    A top publishes only when every object its final written rows reference
    has validated content; a later commit never overtakes an incomplete
    predecessor. Each publication interprets surviving operations against the
    real predecessor archive base and keeps the original commit identity.
    """
    completed = []
    workspace = RowWorkspace(catalog)
    publisher = Publisher(connection, catalog, session_id, hooks)
    journal = TransactionJournal(connection)
    lineage = Lineage(connection)
    objects = ObjectStore(connection)
    pending = PendingStore(connection)
    while True:
        row = pending.earliest()
        if row is None:
            return completed
        top_id, completion_digest, completion_id = row
        candidate = build_commit_candidate(top_id, completion_id, completion_digest,
                                           journal, lineage)
        references = candidate_object_references(candidate, catalog)
        if objects.missing(top_id, references):
            return completed
        before = workspace.base_rows(connection)
        after, written = apply_candidate(candidate, catalog, before)
        for written_row in written.values():
            objects.resolve(top_id, written_row)
        result = diff_rows(before, after)
        validate_row_candidate(result, catalog)
        publisher.publish(candidate, result)
        journal.release(top_id)
        pending.remove(top_id)
        completed.append(top_id)
