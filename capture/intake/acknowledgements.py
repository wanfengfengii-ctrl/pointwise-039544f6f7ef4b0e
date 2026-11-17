from capture.intake.checkpoints import CheckpointStore


class AcknowledgementCoordinator:
    def __init__(self, database, source):
        self.database = database
        self.source = source
        self.last_error = None

    def flush_pending(self):
        if self.database.connection.in_transaction:
            raise RuntimeError("source acknowledgement inside local transaction")
        checkpoint = CheckpointStore(self.database.connection).read()
        target = checkpoint.pending_position
        if target is None:
            return True
        try:
            self.source.acknowledge(target)
        except Exception as exc:
            self.last_error = exc
            return False
        try:
            with self.database.write_unit() as connection:
                CheckpointStore(connection).mark_confirmed(target)
        except Exception as exc:
            self.last_error = exc
            return False
        self.last_error = None
        return True
