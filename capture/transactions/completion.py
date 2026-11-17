from dataclasses import dataclass
from capture.source.codec import content_digest


class TransactionStateError(ValueError):
    pass


@dataclass(frozen=True)
class CommitCandidate:
    top_id: str
    completion_id: str
    completion_digest: str
    modifications: tuple


@dataclass(frozen=True)
class CompletionDecision:
    action: str
    candidate: CommitCandidate | None = None


def build_commit_candidate(top_id, completion_id, completion_digest, journal, lineage):
    events = journal.ordered_events(top_id)
    modifications = tuple(e for e in events if e.kind == "row" and
                          lineage.is_live(top_id, e.branch_id))
    return CommitCandidate(top_id, completion_id, completion_digest, modifications)


def decide_completion(event, connection, journal, lineage):
    row = connection.execute("SELECT root_id,state FROM tops WHERE top_id=?",
                             (event.top_id,)).fetchone()
    if row is None or row[1] != "active":
        raise TransactionStateError("top transaction is not active")
    if event.kind in ("commit", "abort"):
        if event.branch_id != row[0]:
            raise TransactionStateError("top completion must address root branch")
        if event.kind == "abort":
            return CompletionDecision("abort")
        candidate = build_commit_candidate(event.top_id, event.event_id,
                                           content_digest(event.as_dict()), journal, lineage)
        return CompletionDecision("commit", candidate)
    return CompletionDecision("continue")
