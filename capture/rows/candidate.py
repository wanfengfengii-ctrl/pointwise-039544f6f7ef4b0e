from dataclasses import dataclass
from capture.rows.keys import key_from_row, normalize_key
from capture.rows.schema import RowValidationError
from capture.source.codec import canonical_bytes, decode_canonical


@dataclass(frozen=True)
class RowCandidate:
    deletes: tuple
    writes: tuple


def diff_rows(before, after):
    deletes, writes = [], []
    for table in sorted(before):
        for key in sorted(before[table].keys() - after[table].keys()):
            deletes.append((table, key))
        for key in sorted(after[table]):
            row = after[table][key]
            if key not in before[table] or canonical_bytes(row) != canonical_bytes(before[table][key]):
                writes.append((table, key, canonical_bytes(row)))
    return RowCandidate(tuple(deletes), tuple(writes))


def validate_row_candidate(candidate, catalog):
    seen = set()
    for table, key in candidate.deletes:
        schema = catalog.require_table(table)
        key = normalize_key(schema, key)
        if (table, key) in seen:
            raise RowValidationError("duplicate candidate key")
        seen.add((table, key))
    for table, key, encoded in candidate.writes:
        schema = catalog.require_table(table)
        key = normalize_key(schema, key)
        row = catalog.validate_row(table, decode_canonical(encoded))
        if key_from_row(schema, row) != key or (table, key) in seen:
            raise RowValidationError("conflicting candidate key")
        seen.add((table, key))
