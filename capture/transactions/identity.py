from capture.source.codec import content_digest, decode_canonical
from capture.source.envelope import parse_envelope


class EventConflictError(ValueError):
    pass


class EventIdentityIndex:
    """Permanent dedup ledger for accepted events.

    Each row keeps the event identity, position, metadata and a SHA-256 digest
    of the canonical encoding forever. The canonical bytes themselves are
    retained except for object_part events, whose content copies are scrubbed
    by reclamation; digest comparison then still classifies exact replays as
    duplicates and changed content as conflicts.
    """

    def __init__(self, connection):
        self.connection = connection

    def classify(self, event):
        by_id = self.connection.execute(
            "SELECT position,canonical,digest FROM identities WHERE event_id=?",
            (event.event_id,)).fetchone()
        by_position = self.connection.execute(
            "SELECT event_id FROM identities WHERE position=?",
            (event.source_position,)).fetchone()
        if by_id is None and by_position is None:
            return "new"
        if by_id is not None and by_position == (event.event_id,) and \
                by_id[0] == event.source_position:
            if by_id[1] is not None:
                if bytes(by_id[1]) == event.encoded:
                    return "duplicate"
            elif by_id[2] == content_digest(event.as_dict()):
                return "duplicate"
        raise EventConflictError("immutable event identity or position conflict")

    def record(self, event):
        if self.classify(event) != "new":
            raise EventConflictError("cannot record duplicate event")
        object_id, object_index = None, None
        if event.kind == "object_part":
            from capture.transactions.objects import parse_object_part_payload
            object_id, object_index, _ = parse_object_part_payload(event.payload)
        self.connection.execute(
            "INSERT INTO identities VALUES(?,?,?,?,?,?,?,?,?,?)",
            (event.event_id, event.source_position, event.encoded,
             content_digest(event.as_dict()), event.session_id, event.top_id,
             event.branch_id, event.kind, object_id, object_index))

    def event_at(self, position):
        row = self.connection.execute(
            "SELECT canonical FROM identities WHERE position=?", (position,)).fetchone()
        if row is None or row[0] is None:
            raise EventConflictError("missing accepted identity")
        return parse_envelope(decode_canonical(row[0]))
