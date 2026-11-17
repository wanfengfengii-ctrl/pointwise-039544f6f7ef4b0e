from capture.transactions.identity import EventIdentityIndex, EventConflictError
from capture.transactions.lineage import Lineage, TransactionRelationError
from capture.transactions.journal import TransactionJournal
from capture.transactions.completion import (
    CompletionDecision, TransactionStateError, decide_completion, build_commit_candidate)


class TransactionAssembly:
    def __init__(self, connection):
        self.connection = connection
        self.identity = EventIdentityIndex(connection)
        self.lineage = Lineage(connection)
        self.journal = TransactionJournal(connection)

    def accept(self, event, payload_id, event_index):
        if self.identity.classify(event) != "new":
            raise EventConflictError("accept requires a new event")
        top = self.connection.execute(
            "SELECT root_id,state FROM tops WHERE top_id=?", (event.top_id,)).fetchone()
        if event.kind == "begin":
            if top is not None:
                raise TransactionStateError("top identity cannot be reused")
            self.connection.execute("INSERT INTO tops VALUES(?,?,'active',NULL)",
                                    (event.top_id, event.branch_id))
            self.connection.execute("INSERT INTO branches VALUES(?,?,NULL,'active')",
                                    (event.top_id, event.branch_id))
        else:
            if top is None or top[1] != "active":
                raise TransactionStateError("new event after top terminal or before begin")
            if event.kind == "branch":
                self.lineage.add_branch(event.top_id, event.branch_id, event.parent_id)
            elif event.kind == "row":
                self.lineage.require_writable(event.top_id, event.branch_id)
            elif event.kind == "seal":
                self.lineage.seal_branch(event.top_id, event.branch_id)
            elif event.kind == "rollback":
                self.lineage.abort_subtree(event.top_id, event.branch_id)
            elif event.branch_id != top[0]:
                raise TransactionRelationError("completion requires the root")
        self.identity.record(event)
        self.journal.append(event, payload_id, event_index)
        if event.kind in ("commit", "abort"):
            decision = decide_completion(event, self.connection, self.journal, self.lineage)
            if decision.action == "abort":
                self.connection.execute(
                    "UPDATE branches SET state='aborted' WHERE top_id=?", (event.top_id,))
                self.journal.finish(event.top_id, "aborted", event.event_id)
            return decision
        return CompletionDecision("continue")

    def preview(self, top_id):
        return build_commit_candidate(top_id, "", "", self.journal, self.lineage)

    def unfinished_top_ids(self):
        return self.journal.unfinished_top_ids()
