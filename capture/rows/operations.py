from dataclasses import dataclass
from capture.rows.schema import RowValidationError
from capture.rows.keys import normalize_key, key_from_row, KeyOccupancy
from capture.source.codec import canonical_bytes, decode_canonical


@dataclass(frozen=True)
class RowOperation:
    table: str
    op: str
    old_key: tuple | None
    row_bytes: bytes | None

    @property
    def row(self):
        return None if self.row_bytes is None else decode_canonical(self.row_bytes)


def parse_row_operation(event, catalog):
    payload = event.payload
    op = payload.get("op")
    required = {"table", "op"}
    if op == "insert":
        required |= {"row"}
    elif op == "update":
        required |= {"key", "row"}
    elif op == "delete":
        required |= {"key"}
    else:
        raise RowValidationError("unknown row operation")
    if set(payload) != required:
        raise RowValidationError("unexpected or missing row operation fields")
    schema = catalog.require_table(payload["table"])
    old = normalize_key(schema, payload["key"]) if "key" in payload else None
    row = catalog.validate_row(schema.name, payload["row"]) if "row" in payload else None
    if row is not None:
        key_from_row(schema, row)
    return RowOperation(schema.name, op, old,
                        canonical_bytes(row) if row is not None else None)


def apply_row_operation(operation, catalog, rows):
    schema = catalog.require_table(operation.table)
    occupancy = KeyOccupancy(rows[operation.table])
    if operation.op == "delete":
        occupancy.delete(operation.old_key)
    else:
        row = operation.row
        key = key_from_row(schema, row)
        if operation.op == "insert":
            occupancy.insert(key, row)
        else:
            occupancy.move(operation.old_key, key, row)
