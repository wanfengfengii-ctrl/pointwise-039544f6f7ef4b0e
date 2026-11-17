from capture.source.codec import decode_canonical
from capture.source.envelope import parse_envelope


class TransactionJournal:
    def __init__(self, connection):
        self.connection = connection

    def append(self, event, payload_id, event_index):
        self.connection.execute("INSERT INTO journal VALUES(?,?,?,?,?)",
                                (event.source_position, event.top_id, event.branch_id,
                                 payload_id, event_index))

    def ordered_events(self, top_id):
        rows = self.connection.execute(
            "SELECT j.position,j.event_index,p.body FROM journal j "
            "LEFT JOIN payloads p ON p.payload_id=j.payload_id "
            "WHERE j.top_id=? ORDER BY j.position", (top_id,)).fetchall()
        events = []
        for position, index, body in rows:
            if body is None:
                raise ValueError("journal payload missing")
            raw = decode_canonical(body)
            if not 0 <= index < len(raw):
                raise ValueError("journal payload offset invalid")
            event = parse_envelope(raw[index])
            if event.source_position != position or event.top_id != top_id:
                raise ValueError("journal payload identity mismatch")
            events.append(event)
        return tuple(events)

    def unfinished_top_ids(self):
        return tuple(row[0] for row in self.connection.execute(
            "SELECT top_id FROM tops WHERE state='active' "
            "UNION SELECT top_id FROM pending ORDER BY 1"))

    def payload_references(self, payload_id=None):
        if payload_id is None:
            return frozenset(row[0] for row in self.connection.execute(
                "SELECT DISTINCT payload_id FROM journal"))
        return tuple(row[0] for row in self.connection.execute(
            "SELECT DISTINCT top_id FROM journal WHERE payload_id=? ORDER BY top_id",
            (payload_id,)))

    def conclude(self, top_id, state, completion_id):
        if state not in ("committed", "aborted"):
            raise ValueError("invalid terminal state")
        changed = self.connection.execute(
            "UPDATE tops SET state=?,completion_id=? WHERE top_id=? AND state='active'",
            (state, completion_id, top_id)).rowcount
        if changed != 1:
            raise ValueError("transaction is not active")

    def release(self, top_id):
        self.connection.execute("DELETE FROM journal WHERE top_id=?", (top_id,))

    def finish(self, top_id, state, completion_id):
        self.conclude(top_id, state, completion_id)
        self.release(top_id)
