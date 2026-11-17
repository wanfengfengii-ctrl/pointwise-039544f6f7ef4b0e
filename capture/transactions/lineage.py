class TransactionRelationError(ValueError):
    pass


class Lineage:
    def __init__(self, connection):
        self.connection = connection

    def branches(self, top_id):
        return {branch: (parent, state) for branch, parent, state in
                self.connection.execute(
                    "SELECT branch_id,parent_id,state FROM branches WHERE top_id=?", (top_id,))}

    def require_branch(self, top_id, branch_id):
        branches = self.branches(top_id)
        if branch_id not in branches:
            raise TransactionRelationError("branch does not belong to this top")
        return branches[branch_id]

    def require_writable(self, top_id, branch_id):
        parent, state = self.require_branch(top_id, branch_id)
        if state != "active" or not self.is_live(top_id, branch_id):
            raise TransactionRelationError("branch is not writable")

    def add_branch(self, top_id, branch_id, parent_id):
        branches = self.branches(top_id)
        if branch_id in branches or branch_id == parent_id:
            raise TransactionRelationError("branch reuse or cycle")
        self.require_writable(top_id, parent_id)
        cursor, visited = parent_id, {branch_id}
        while cursor is not None:
            if cursor in visited or cursor not in branches:
                raise TransactionRelationError("invalid ancestry")
            visited.add(cursor)
            cursor = branches[cursor][0]
        self.connection.execute("INSERT INTO branches VALUES(?,?,?,'active')",
                                (top_id, branch_id, parent_id))

    def seal_branch(self, top_id, branch_id):
        parent, state = self.require_branch(top_id, branch_id)
        if parent is None or state != "active" or not self.is_live(top_id, branch_id):
            raise TransactionRelationError("only an active child can be sealed")
        self.connection.execute(
            "UPDATE branches SET state='sealed' WHERE top_id=? AND branch_id=?",
            (top_id, branch_id))

    def abort_subtree(self, top_id, branch_id):
        branches = self.branches(top_id)
        parent, state = self.require_branch(top_id, branch_id)
        if parent is None or state == "aborted":
            raise TransactionRelationError("invalid child rollback")
        doomed = {branch_id}
        while True:
            expanded = doomed | {b for b, (p, _) in branches.items() if p in doomed}
            if expanded == doomed:
                break
            doomed = expanded
        self.connection.executemany(
            "UPDATE branches SET state='aborted' WHERE top_id=? AND branch_id=?",
            [(top_id, branch) for branch in sorted(doomed)])
        return frozenset(doomed)

    def is_live(self, top_id, branch_id):
        branches = self.branches(top_id)
        visited = set()
        cursor = branch_id
        while cursor is not None:
            if cursor not in branches or cursor in visited:
                raise TransactionRelationError("missing or cyclic lineage")
            visited.add(cursor)
            cursor, state = branches[cursor]
            if state == "aborted":
                return False
        return True
