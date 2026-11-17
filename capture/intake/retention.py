from capture.source.codec import canonical_bytes, decode_canonical
from capture.transactions.journal import TransactionJournal
from capture.intake.checkpoints import CheckpointStore


class PayloadStore:
    def __init__(self, connection):
        self.connection = connection

    def stage(self, segment):
        body = canonical_bytes([event.as_dict() for event in segment.events])
        cursor = self.connection.execute(
            "INSERT INTO payloads(last_position,body) VALUES(?,?)",
            (segment.last_position, body))
        return cursor.lastrowid

    def load(self, payload_id):
        row = self.connection.execute("SELECT body FROM payloads WHERE payload_id=?",
                                      (payload_id,)).fetchone()
        if row is None:
            raise ValueError("payload is missing")
        return decode_canonical(row[0])

    def retained_ids(self):
        return tuple(row[0] for row in self.connection.execute(
            "SELECT payload_id FROM payloads ORDER BY payload_id"))


class RetentionManager:
    def __init__(self, connection):
        self.connection = connection

    def reclaim(self):
        confirmed = CheckpointStore(self.connection).read().confirmed_position
        references = TransactionJournal(self.connection).payload_references()
        eligible = tuple(row[0] for row in self.connection.execute(
            "SELECT payload_id FROM payloads WHERE last_position<=? ORDER BY payload_id",
            (confirmed,)) if row[0] not in references)
        self.connection.executemany("DELETE FROM payloads WHERE payload_id=?",
                                    [(identifier,) for identifier in eligible])
        return eligible
