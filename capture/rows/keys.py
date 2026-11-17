from capture.source.codec import canonical_bytes
from capture.rows.schema import RowValidationError


def normalize_key(schema, key, error=RowValidationError):
    if not isinstance(key, (list, tuple)) or len(key) != len(schema.primary_key):
        raise error("key must have declared arity")
    normalized = []
    for name, value in zip(schema.primary_key, key):
        kind = schema.column(name)[1]
        if type(value) is not (int if kind == "int" else str):
            raise error("wrong key type: " + name)
        normalized.append(value)
    return tuple(normalized)


def key_from_row(schema, row):
    return normalize_key(schema, [row[name] for name in schema.primary_key])


def encode_key(key):
    return canonical_bytes(list(key)).decode("utf-8")


def key_sort_value(schema, key):
    return normalize_key(schema, key)


class KeyOccupancy:
    def __init__(self, rows):
        self.rows = rows

    def insert(self, key, row):
        if key in self.rows:
            raise RowValidationError("insert key already exists")
        self.rows[key] = row

    def move(self, old, new, row):
        if old not in self.rows:
            raise RowValidationError("update requires an existing row")
        if new != old and new in self.rows:
            raise RowValidationError("target key already exists")
        del self.rows[old]
        self.rows[new] = row

    def delete(self, key):
        if key not in self.rows:
            raise RowValidationError("delete requires an existing row")
        del self.rows[key]
