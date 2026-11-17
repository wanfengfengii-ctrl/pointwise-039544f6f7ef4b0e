from capture.source.envelope import parse_envelope


class EventConflictError(ValueError):
    pass


class EventIdentityIndex:
    def __init__(self, connection):
        self.connection = connection

    def classify(self, event):
        by_id = self.connection.execute(
            "SELECT position,canonical FROM identities WHERE event_id=?",
            (event.event_id,)).fetchone()
        by_position = self.connection.execute(
            "SELECT event_id FROM identities WHERE position=?",
            (event.source_position,)).fetchone()
        if by_id is None and by_position is None:
            return "new"
        if (by_id is not None and by_position == (event.event_id,) and
                by_id[0] == event.source_position and bytes(by_id[1]) == event.encoded):
            return "duplicate"
        raise EventConflictError("immutable event identity or position conflict")

    def record(self, event):
        if self.classify(event) != "new":
            raise EventConflictError("cannot record duplicate event")
        self.connection.execute("INSERT INTO identities VALUES(?,?,?)",
                                (event.event_id, event.source_position, event.encoded))

    def event_at(self, position):
        row = self.connection.execute(
            "SELECT canonical FROM identities WHERE position=?", (position,)).fetchone()
        if row is None:
            raise EventConflictError("missing accepted identity")
        from capture.source.codec import decode_canonical
        return parse_envelope(decode_canonical(row[0]))
