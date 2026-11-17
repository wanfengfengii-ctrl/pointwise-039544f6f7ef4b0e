import copy
from capture.archive.query import Archive
from capture.rows.schema import RowValidationError, ArchiveParameterError
from tests.support import CaptureCase, Tape, reference_accounts


class TestNormalCommit(CaptureCase):
    def test_insert_update_delete(self):
        tape = Tape()
        tape.add("begin")
        tape.row("insert", key=1, value=1)
        tape.row("insert", key=2, value=2)
        tape.row("update", key=1, value=10)
        tape.row("delete", key=2)
        tape.add("commit")
        collector = self.connect(tape)
        collector.feed(tape.segment(0, -1))
        self.assertEqual(collector.archive.read("accounts"), [])
        self.assertEqual(self.receipts(collector), [])
        result = collector.feed(tape.segment(-1))
        self.assertEqual(result.completed_top_ids, ("t",))
        self.assertEqual(result.accepted_position, 6)
        self.assertEqual(collector.archive.read("accounts"), [{"id": 1, "value": 10}])
        self.assertEqual(self.receipts(collector), [("t", 1)])

    def test_interleaved_transactions(self):
        tape = Tape()
        tape.add("begin", top="a")
        tape.add("begin", top="b")
        tape.add("branch", top="a", branch="local", parent="root")
        tape.add("branch", top="b", branch="local", parent="root")
        tape.row("insert", key=1, value=11, top="a", branch="local")
        tape.row("insert", key=2, value=22, top="b", branch="local")
        tape.add("commit", top="a")
        tape.row("update", key=2, value=23, top="b", branch="local")
        tape.add("commit", top="b")
        collector = self.connect(tape)
        with Archive(self.path) as reader:
            collector.feed(tape.segment(0, 6))
            self.assertEqual(reader.read("accounts"), [])
            collector.feed(tape.segment(6, 7))
            self.assertEqual(reader.read("accounts"), [{"id": 1, "value": 11}])
            self.assertEqual(collector.checkpoint().unfinished_top_ids, ("b",))
            collector = self.reopen(collector, tape)
            collector.feed(tape.segment(7))
            self.assertEqual(reader.read("accounts"), reference_accounts(tape.events))
        self.assertEqual(self.receipts(collector), [("a", 1), ("b", 2)])

    def test_key_move(self):
        tape = Tape()
        tape.add("begin", top="seed")
        tape.row("insert", key=1, value=1, top="seed")
        tape.add("commit", top="seed")
        tape.add("begin")
        tape.row("update", key=1, new_key=2, value=20)
        tape.row("update", key=2, value=21)
        tape.add("commit")
        collector = self.connect(tape)
        collector.feed(tape.segment())
        self.assertEqual(collector.archive.read("accounts", [[1]]), [])
        self.assertEqual(collector.archive.read("accounts", [[2]]), [{"id": 2, "value": 21}])
        self.assertEqual(collector.archive.read("accounts"), reference_accounts(tape.events))

    def test_key_conflict_atomicity(self):
        tape = Tape()
        tape.add("begin", top="seed")
        tape.row("insert", key=1, value=1, top="seed")
        tape.row("insert", key=2, value=2, top="seed")
        tape.add("commit", top="seed")
        tape.add("begin")
        tape.row("update", key=1, new_key=2, value=7)
        collector = self.connect(tape)
        collector.feed(tape.segment(0, 4))
        before = self.state(collector)
        with self.assertRaises(RowValidationError):
            collector.feed(tape.segment(4))
        self.assertEqual(self.state(collector), before)

    def test_schema_contract_and_segment_batch(self):
        tape = Tape()
        tape.add("begin")
        tape.row("insert", key=1, value=1)
        tape.add("commit")
        tape.add("begin", top="bad")
        tape.row("insert", key=2, value=2, top="bad")
        collector = self.connect(tape)
        before = self.state(collector)
        bad_values = [
            {"id": 2}, {"id": 2, "value": "2"}, {"id": True, "value": 2},
            {"id": 2, "value": None}, {"id": 2, "value": 2, "extra": 9}]
        for row in bad_values:
            with self.subTest(row=row):
                bad = tape.segment()
                bad["events"][-1]["payload"]["row"] = row
                with self.assertRaises(RowValidationError):
                    collector.feed(bad)
                self.assertEqual(self.state(collector), before)
        bad = tape.segment()
        bad["events"][-1]["payload"]["table"] = "missing"
        with self.assertRaises(RowValidationError):
            collector.feed(bad)
        self.assertEqual(self.state(collector), before)
        collector.feed(tape.segment())
        self.assertEqual(collector.archive.read("accounts"), [{"id": 1, "value": 1}])

    def test_key_order_and_read_errors(self):
        tape = Tape()
        tape.add("begin")
        for key in (10, 2, -1):
            tape.row("insert", key=key, value=key)
        for group, rank in (("z", 2), ("a", 10), ("a", 2)):
            tape.add("row", payload={"table": "labels", "op": "insert",
                     "row": {"group": group, "rank": rank, "text": None}})
        tape.add("commit")
        collector = self.connect(tape)
        collector.feed(tape.segment())
        self.assertEqual([r["id"] for r in collector.archive.read("accounts")], [-1, 2, 10])
        self.assertEqual([r["id"] for r in collector.archive.read("accounts", [[10], [2], [2]])], [2, 10])
        self.assertEqual([(r["group"], r["rank"]) for r in collector.archive.read("labels")],
                         [("a", 2), ("a", 10), ("z", 2)])
        before = self.state(collector)
        for table, keys in (("missing", None), ("accounts", [[True]]),
                            ("accounts", [["2"]]), ("accounts", [[1, 2]]),
                            ("accounts", [1]), ("accounts", "bad"),
                            ("labels", [[1, 2]])):
            with self.subTest(table=table, keys=keys), self.assertRaises(ArchiveParameterError):
                collector.archive.read(table, keys)
        self.assertEqual(self.state(collector), before)

    def test_missing_row_rejects_entire_segment(self):
        tape = Tape()
        tape.add("begin", top="good")
        tape.row("insert", key=1, value=1, top="good")
        tape.add("commit", top="good")
        tape.add("begin")
        tape.row("delete", key=99)
        collector = self.connect(tape)
        before = self.state(collector)
        with self.assertRaises(RowValidationError):
            collector.feed(tape.segment())
        self.assertEqual(self.state(collector), before)
