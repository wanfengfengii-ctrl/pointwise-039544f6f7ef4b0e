import random
from capture.transactions.lineage import TransactionRelationError
from capture.transactions.completion import TransactionStateError
from tests.support import CaptureCase, Tape, rollback_tape, reference_accounts


class TestRollback(CaptureCase):
    def test_parent_child_sibling(self):
        tape = rollback_tape()
        collector = self.connect(tape)
        collector.feed(tape.segment(0, 3))
        collector.feed(tape.segment(3, -1))
        self.assertEqual(collector.archive.read("accounts"), [{"id": 1, "value": 0}])
        collector = self.reopen(collector, tape)
        collector.feed(tape.segment(-2))
        self.assertEqual(collector.archive.read("accounts"),
                         [{"id": 1, "value": 10}, {"id": 4, "value": 40}])
        self.assertEqual(self.receipts(collector), [("seed", 1), ("t", 2)])

    def test_delete_restore(self):
        tape = rollback_tape("delete")
        collector = self.connect(tape)
        for index in range(len(tape.events)):
            collector.feed(tape.segment(index, index + 1))
        self.assertEqual(collector.archive.read("accounts"), reference_accounts(tape.events))
        self.assertEqual(collector.archive.read("accounts", [[1]]), [{"id": 1, "value": 10}])

    def test_key_change_and_top_abort(self):
        tape = rollback_tape("move")
        collector = self.connect(tape)
        collector.feed(tape.segment())
        self.assertEqual(collector.archive.read("accounts"), reference_accounts(tape.events))
        self.assertEqual(collector.archive.read("accounts", [[2], [3]]), [])
        tape.add("begin", top="aborted")
        tape.row("update", key=1, new_key=2, value=99, top="aborted")
        tape.row("delete", key=4, top="aborted")
        tape.add("abort", top="aborted")
        old_rows = collector.archive.read("accounts")
        result = collector.feed(tape.segment(15))
        self.assertEqual(result.completed_top_ids, ())
        self.assertEqual(collector.archive.read("accounts"), old_rows)
        self.assertEqual(self.receipts(collector), [("seed", 1), ("t", 2)])
        self.assertEqual(collector.checkpoint().unfinished_top_ids, ())

    def test_parent_key_survives_child_move(self):
        tape = Tape()
        tape.add("begin")
        tape.row("insert", key=1, value=1)
        tape.row("update", key=1, new_key=2, value=10)
        tape.add("branch", branch="child", parent="root")
        tape.row("delete", key=2, branch="child")
        tape.row("insert", key=1, value=20, branch="child")
        tape.add("rollback", branch="child")
        tape.add("commit")
        collector = self.connect(tape)
        collector.feed(tape.segment())
        self.assertEqual(collector.archive.read("accounts"), [{"id": 2, "value": 10}])

    def test_invalid_lineage(self):
        tape = Tape()
        tape.add("begin")
        tape.add("branch", branch="child", parent="root")
        tape.row("insert", key=1, value=10)
        tape.add("branch", branch="root", parent="child")
        tape.add("commit")
        collector = self.connect(tape)
        collector.feed(tape.segment(0, 2))
        before = self.state(collector)
        with self.assertRaises(TransactionRelationError):
            collector.feed(tape.segment(2, 4))
        self.assertEqual(self.state(collector), before)
        fixed = tape.segment(2, 4)
        fixed["events"][-1]["branch_id"] = "sibling"
        fixed["events"][-1]["parent_id"] = "root"
        collector.feed(fixed)
        collector.feed(tape.segment(4))
        self.assertEqual(collector.archive.read("accounts"), [{"id": 1, "value": 10}])

    def test_cross_top_and_terminal_references(self):
        tape = Tape()
        tape.add("begin", top="a")
        tape.add("branch", top="a", branch="only-a", parent="root")
        tape.add("begin", top="b")
        tape.add("branch", top="b", branch="child", parent="only-a")
        collector = self.connect(tape)
        collector.feed(tape.segment(0, 3))
        before = self.state(collector)
        with self.assertRaises(TransactionRelationError):
            collector.feed(tape.segment(3))
        self.assertEqual(self.state(collector), before)
        tape.events.pop()
        tape.add("commit", top="b")
        collector.feed(tape.segment(3))
        tape.row("insert", key=1, value=1, top="b")
        before = self.state(collector)
        with self.assertRaises(TransactionStateError):
            collector.feed(tape.segment(4))
        self.assertEqual(self.state(collector), before)

    def test_sealed_branch_rejects_write(self):
        tape = Tape()
        tape.add("begin")
        tape.add("branch", branch="child", parent="root")
        tape.add("seal", branch="child")
        tape.row("insert", key=1, value=1, branch="child")
        collector = self.connect(tape)
        collector.feed(tape.segment(0, 3))
        before = self.state(collector)
        with self.assertRaises(TransactionRelationError):
            collector.feed(tape.segment(3))
        self.assertEqual(self.state(collector), before)

    def test_independent_ordered_reference(self):
        rng = random.Random(1941)
        tape = Tape()
        expected = []
        collector = None
        for index in range(18):
            top = "t" + str(index)
            key = index * 10
            tape.add("begin", top=top)
            tape.row("insert", key=key, value=rng.randrange(100), top=top)
            tape.row("update", key=key, new_key=key + 1, value=10, top=top)
            tape.add("branch", top=top, branch="child", parent="root")
            if index % 2:
                tape.row("delete", key=key + 1, top=top, branch="child")
                tape.row("insert", key=key, value=99, top=top, branch="child")
            else:
                tape.row("update", key=key + 1, value=20, top=top, branch="child")
            tape.add("branch", top=top, branch="grand", parent="child")
            tape.row("insert", key=key + 2, value=30, top=top, branch="grand")
            tape.add("seal", top=top, branch="grand")
            tape.add("branch", top=top, branch="sibling", parent="root")
            tape.row("insert", key=key + 3, value=40, top=top, branch="sibling")
            tape.add("rollback", top=top, branch="child")
            tape.add("abort" if index % 5 == 0 else "commit", top=top)
        collector = self.connect(tape)
        for index in range(0, len(tape.events), 7):
            collector.feed(tape.segment(index, index + 7))
            expected = reference_accounts(tape.events[:index + 7])
            self.assertEqual(collector.archive.read("accounts"), expected)
        self.assertEqual(collector.archive.read("accounts"), reference_accounts(tape.events))
