from capture.rows.keys import key_from_row
from capture.rows.operations import (
    parse_row_operation, apply_row_operation, row_object_references)
from capture.rows.candidate import diff_rows, validate_row_candidate
from capture.source.codec import decode_canonical


class RowWorkspace:
    def __init__(self, catalog):
        self.catalog = catalog

    def base_rows(self, connection):
        rows = {name: {} for name in self.catalog.tables}
        for table, encoded in connection.execute("SELECT table_name,row_data FROM archive_rows"):
            row = decode_canonical(encoded)
            schema = self.catalog.require_table(table)
            self.catalog.validate_row(table, row)
            rows[table][key_from_row(schema, row)] = row
        return rows

    def materialize(self, candidate, connection, base=None):
        before = self.base_rows(connection) if base is None else base
        after, _ = apply_candidate(candidate, self.catalog, before)
        result = diff_rows(before, after)
        validate_row_candidate(result, self.catalog)
        return result


def apply_candidate(candidate, catalog, base):
    """Interpret surviving operations over base rows.

    Returns (after_rows, written) where written maps each finally written
    (table, key) to its final row image. Key occupancy is content independent,
    so object references in written rows do not affect validity.
    """
    after = {table: dict(rows) for table, rows in base.items()}
    written = {}
    for event in candidate.modifications:
        operation = parse_row_operation(event, catalog)
        apply_row_operation(operation, catalog, after)
        if operation.op == "delete":
            written.pop((operation.table, operation.old_key), None)
            continue
        schema = catalog.require_table(operation.table)
        key = key_from_row(schema, operation.row)
        if operation.op == "update" and operation.old_key != key:
            written.pop((operation.table, operation.old_key), None)
        written[(operation.table, key)] = after[operation.table][key]
    return after, written


def candidate_object_references(candidate, catalog):
    """Object ids referenced by the candidate's final written rows.

    Withdrawn, overwritten and deleted references never appear here, so they
    cannot block publication. The result is independent of the base rows.
    """
    references = set()
    written = {}
    for event in candidate.modifications:
        operation = parse_row_operation(event, catalog)
        if operation.op == "delete":
            written.pop((operation.table, operation.old_key), None)
            continue
        row = operation.row
        schema = catalog.require_table(operation.table)
        key = key_from_row(schema, row)
        if operation.op == "update" and operation.old_key != key:
            written.pop((operation.table, operation.old_key), None)
        written[(operation.table, key)] = row
    for row in written.values():
        references |= row_object_references(row)
    return frozenset(references)
