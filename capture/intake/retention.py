from capture.source.codec import canonical_bytes, decode_canonical
from capture.transactions.journal import TransactionJournal
from capture.transactions.pending import PendingStore
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
    """Reclaims intake staging copies once they are confirmed and unneeded.

    Raw segment payloads follow the V1 rule: confirmed and unreferenced by
    every unfinished top. Object content (object store bytes, part bytes and
    the identity ledger's canonical copies of object_part events) is scrubbed
    once the owning top can no longer reference it and the source has
    confirmed the part positions. Digests and metadata remain as permanent
    dedup and recovery evidence.
    """

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
        self._reclaim_objects(confirmed)
        return eligible

    def _reclaim_objects(self, confirmed):
        needed = PendingStore(self.connection).needed()
        states = {top: state for top, state in self.connection.execute(
            "SELECT top_id,state FROM tops")}
        rows = self.connection.execute(
            "SELECT top_id,object_id FROM objects WHERE reclaimed=0").fetchall()
        for top_id, object_id in rows:
            if states.get(top_id) == "active" or (top_id, object_id) in needed:
                continue
            last_part = self.connection.execute(
                "SELECT MAX(position) FROM identities "
                "WHERE top_id=? AND kind='object_part' AND object_id=?",
                (top_id, object_id)).fetchone()[0]
            if last_part is not None and last_part > confirmed:
                continue
            self.connection.execute(
                "UPDATE objects SET content=NULL,reclaimed=1 "
                "WHERE top_id=? AND object_id=?", (top_id, object_id))
            self.connection.execute(
                "UPDATE object_parts SET data=NULL WHERE top_id=? AND object_id=?",
                (top_id, object_id))
            self.connection.execute(
                "UPDATE identities SET canonical=NULL "
                "WHERE top_id=? AND kind='object_part' AND object_id=?",
                (top_id, object_id))
