from capture.transactions.identity import EventIdentityIndex, EventConflictError
from capture.transactions.lineage import Lineage, TransactionRelationError
from capture.transactions.journal import TransactionJournal
from capture.transactions.completion import (
    CompletionDecision, TransactionStateError, decide_completion, build_commit_candidate)
from capture.transactions.objects import ObjectStore
from capture.transactions.pending import PendingStore


class TransactionAssembly:
    def __init__(self, connection):
        self.connection = connection
        self.identity = EventIdentityIndex(connection)
        self.lineage = Lineage(connection)
        self.journal = TransactionJournal(connection)
        self.objects = ObjectStore(connection)

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
        elif event.kind in ("object_part", "object_seal"):
            if top is None or top[1] == "aborted" or (
                    top[1] == "committed" and
                    not PendingStore(self.connection).contains(event.top_id)):
                raise TransactionStateError("object event outside top lifetime")
            if event.branch_id != top[0]:
                raise TransactionRelationError("object events address the root branch")
            self.objects.accept(event)
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

    def conclude_commit(self, event, candidate, references):
        """Freeze a committed top and park it in commit order until its
        referenced objects are complete and predecessors have published."""
        self.journal.conclude(event.top_id, "committed", event.event_id)
        PendingStore(self.connection).add(event.top_id, event.source_position,
                                          candidate.completion_digest, references)

    def preview(self, top_id):
        return build_commit_candidate(top_id, "", "", self.journal, self.lineage)

    def unfinished_top_ids(self):
        return self.journal.unfinished_top_ids()
