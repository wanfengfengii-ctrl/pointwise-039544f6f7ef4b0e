from capture.rows.keys import key_from_row
from capture.rows.operations import parse_row_operation, apply_row_operation
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

    def materialize(self, candidate, connection):
        before = self.base_rows(connection)
        after = {table: dict(rows) for table, rows in before.items()}
        for event in candidate.modifications:
            operation = parse_row_operation(event, self.catalog)
            apply_row_operation(operation, self.catalog, after)
        result = diff_rows(before, after)
        validate_row_candidate(result, self.catalog)
        return result
