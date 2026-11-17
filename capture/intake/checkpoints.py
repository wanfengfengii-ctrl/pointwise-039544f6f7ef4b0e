from dataclasses import dataclass


@dataclass(frozen=True)
class IntakeCheckpoint:
    accepted_position: int
    confirmed_position: int
    pending_position: int | None
    unfinished_top_ids: tuple


class CheckpointStore:
    def __init__(self, connection):
        self.connection = connection

    def read(self):
        accepted, confirmed = self.connection.execute(
            "SELECT accepted,confirmed FROM progress WHERE singleton=1").fetchone()
        unfinished = tuple(row[0] for row in self.connection.execute(
            "SELECT top_id FROM tops WHERE state='active' ORDER BY top_id"))
        pending = accepted if accepted > confirmed else None
        return IntakeCheckpoint(accepted, confirmed, pending, unfinished)

    def advance_accepted(self, position):
        current = self.read().accepted_position
        if type(position) is not int or position < current:
            raise ValueError("accepted position cannot decrease")
        self.connection.execute("UPDATE progress SET accepted=? WHERE singleton=1", (position,))

    def mark_confirmed(self, position):
        checkpoint = self.read()
        if (type(position) is not int or
                not checkpoint.confirmed_position <= position <= checkpoint.accepted_position):
            raise ValueError("confirmation outside durable prefix")
        self.connection.execute("UPDATE progress SET confirmed=? WHERE singleton=1", (position,))
