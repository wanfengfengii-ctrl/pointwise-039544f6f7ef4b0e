from dataclasses import dataclass
from capture.archive.receipts import ReceiptStore, CommitReceipt
from capture.rows.candidate import validate_row_candidate
from capture.rows.keys import encode_key


class PublicationHooks:
    def __init__(self, callback=None):
        self.callback = callback

    def hit(self, stage):
        if self.callback is not None:
            self.callback(stage)


@dataclass(frozen=True)
class PublicationResult:
    top_id: str
    publication: int
    created: bool


class Publisher:
    def __init__(self, connection, catalog, session_id, hooks=None):
        self.connection = connection
        self.catalog = catalog
        self.session_id = session_id
        self.hooks = hooks if hooks is not None else PublicationHooks()
        self.receipts = ReceiptStore(connection)

    def publish(self, transaction, rows):
        if not self.connection.in_transaction:
            raise RuntimeError("publication requires caller write unit")
        prior = self.receipts.lookup(transaction.top_id)
        if prior is not None:
            self.receipts.verify_completion(prior, self.session_id, transaction)
            return PublicationResult(prior.top_id, prior.publication, False)
        validate_row_candidate(rows, self.catalog)
        for table, key in rows.deletes:
            self.connection.execute(
                "DELETE FROM archive_rows WHERE table_name=? AND key_data=?",
                (table, encode_key(key)))
        for table, key, encoded in rows.writes:
            self.connection.execute(
                "INSERT INTO archive_rows VALUES(?,?,?) "
                "ON CONFLICT(table_name,key_data) DO UPDATE SET row_data=excluded.row_data",
                (table, encode_key(key), encoded))
        self.hooks.hit("after_rows")
        sequence = self.connection.execute(
            "SELECT publication FROM progress WHERE singleton=1").fetchone()[0] + 1
        self.hooks.hit("before_receipt")
        self.receipts.record(CommitReceipt(transaction.top_id, self.session_id,
                                          transaction.completion_id,
                                          transaction.completion_digest, sequence))
        self.connection.execute("UPDATE progress SET publication=? WHERE singleton=1", (sequence,))
        return PublicationResult(transaction.top_id, sequence, True)
