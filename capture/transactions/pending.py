class PendingStore:
    """Durable wait state: committed tops not yet published, in commit order.

    A commit is accepted durably even when its referenced objects are still
    missing. pending_refs records the object ids each waiting top's final
    written rows actually reference, so reclamation can tell which content is
    still needed and recovery can cross-check the wait set.
    """

    def __init__(self, connection):
        self.connection = connection

    def add(self, top_id, commit_position, completion_digest, references):
        self.connection.execute(
            "INSERT INTO pending VALUES(?,?,?)",
            (top_id, commit_position, completion_digest))
        self.connection.executemany(
            "INSERT INTO pending_refs VALUES(?,?)",
            [(top_id, object_id) for object_id in sorted(references)])

    def earliest(self):
        return self.connection.execute(
            "SELECT p.top_id,p.completion_digest,t.completion_id FROM pending p "
            "JOIN tops t ON t.top_id=p.top_id ORDER BY p.commit_position LIMIT 1").fetchone()

    def remove(self, top_id):
        self.connection.execute("DELETE FROM pending WHERE top_id=?", (top_id,))
        self.connection.execute("DELETE FROM pending_refs WHERE top_id=?", (top_id,))

    def ordered(self):
        return tuple(row[0] for row in self.connection.execute(
            "SELECT top_id FROM pending ORDER BY commit_position"))

    def contains(self, top_id):
        return self.connection.execute(
            "SELECT 1 FROM pending WHERE top_id=?", (top_id,)).fetchone() is not None

    def needed(self):
        return frozenset(self.connection.execute("SELECT top_id,object_id FROM pending_refs"))
