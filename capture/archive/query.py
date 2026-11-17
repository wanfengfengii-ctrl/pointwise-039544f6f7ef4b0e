from capture.archive.database import ArchiveDatabase
from capture.rows.schema import SchemaCatalog, ArchiveParameterError
from capture.rows.keys import normalize_key, key_from_row, key_sort_value, encode_key
from capture.source.codec import decode_canonical


class Archive:
    def __init__(self, database):
        self.owns_database = not isinstance(database, ArchiveDatabase)
        self.database = ArchiveDatabase(database) if self.owns_database else database
        self.catalog = SchemaCatalog(self.database.session_data["tables"])

    def read(self, table, keys=None):
        schema = self.catalog.require_table(table, ArchiveParameterError)
        selected = None
        if keys is not None:
            if not isinstance(keys, (list, tuple)):
                raise ArchiveParameterError("keys must be a list of key tuples")
            selected = {encode_key(normalize_key(schema, key, ArchiveParameterError)) for key in keys}
        with self.database.read_snapshot() as connection:
            result = []
            for key, encoded in connection.execute(
                    "SELECT key_data,row_data FROM archive_rows WHERE table_name=?", (table,)):
                if selected is None or key in selected:
                    row = decode_canonical(encoded)
                    self.catalog.validate_row(table, row)
                    result.append(row)
            result.sort(key=lambda row: key_sort_value(schema, key_from_row(schema, row)))
            return result

    def close(self):
        if self.owns_database:
            self.database.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()
