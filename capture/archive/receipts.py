from dataclasses import dataclass
from capture.transactions.identity import EventConflictError


@dataclass(frozen=True)
class CommitReceipt:
    top_id: str
    session_id: str
    event_id: str
    digest: str
    publication: int


class ReceiptStore:
    def __init__(self, connection):
        self.connection = connection

    def lookup(self, top_id):
        row = self.connection.execute(
            "SELECT top_id,session_id,event_id,digest,publication FROM receipts WHERE top_id=?",
            (top_id,)).fetchone()
        return None if row is None else CommitReceipt(*row)

    def verify_completion(self, receipt, session_id, candidate):
        if (receipt.session_id != session_id or
                receipt.event_id != candidate.completion_id or
                receipt.digest != candidate.completion_digest):
            raise EventConflictError("commit completion conflicts with receipt")
        return receipt

    def record(self, receipt):
        old = self.lookup(receipt.top_id)
        if old is not None:
            if old != receipt:
                raise EventConflictError("receipt is immutable")
            return old
        self.connection.execute("INSERT INTO receipts VALUES(?,?,?,?,?)",
                                (receipt.top_id, receipt.session_id, receipt.event_id,
                                 receipt.digest, receipt.publication))
        return receipt

    def all(self):
        return tuple(CommitReceipt(*row) for row in self.connection.execute(
            "SELECT top_id,session_id,event_id,digest,publication FROM receipts ORDER BY publication"))
