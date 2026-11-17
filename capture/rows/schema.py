from dataclasses import dataclass
from math import isfinite


class RowValidationError(ValueError):
    pass


class ArchiveParameterError(ValueError):
    pass


_TYPES = {"int": int, "str": str, "float": float, "bool": bool}


@dataclass(frozen=True)
class TableSchema:
    name: str
    columns: tuple
    primary_key: tuple

    def column(self, name):
        for column in self.columns:
            if column[0] == name:
                return column
        raise RowValidationError("unknown column: " + str(name))


class SchemaCatalog:
    def __init__(self, definitions):
        if type(definitions) is not dict or not definitions:
            raise RowValidationError("nonempty table definitions required")
        self.tables = {}
        for name, spec in definitions.items():
            if type(name) is not str or not name or type(spec) is not dict:
                raise RowValidationError("invalid table definition")
            if set(spec) != {"columns", "primary_key"}:
                raise RowValidationError("columns and primary_key required")
            columns, primary = spec["columns"], spec["primary_key"]
            if type(columns) is not dict or not columns:
                raise RowValidationError("nonempty columns required")
            parsed = []
            for column, definition in columns.items():
                if type(column) is not str or not column or type(definition) is not dict:
                    raise RowValidationError("invalid column")
                if set(definition) != {"type", "nullable"}:
                    raise RowValidationError("type and nullable required")
                if (type(definition["type"]) is not str or definition["type"] not in _TYPES or
                        type(definition["nullable"]) is not bool):
                    raise RowValidationError("invalid column type or nullability")
                parsed.append((column, definition["type"], definition["nullable"]))
            if (type(primary) is not list or not primary or
                    any(type(c) is not str or c not in columns for c in primary) or
                    len(set(primary)) != len(primary)):
                raise RowValidationError("invalid primary key")
            for column in primary:
                if columns[column]["type"] not in ("int", "str") or columns[column]["nullable"]:
                    raise RowValidationError("primary key must be nonnull int or str")
            self.tables[name] = TableSchema(name, tuple(parsed), tuple(primary))

    def require_table(self, name, error=RowValidationError):
        if type(name) is not str or name not in self.tables:
            raise error("unknown table: " + str(name))
        return self.tables[name]

    def validate_row(self, table, row):
        schema = self.require_table(table)
        if type(row) is not dict or set(row) != {c[0] for c in schema.columns}:
            raise RowValidationError("full row must contain exactly the declared columns")
        for name, kind, nullable in schema.columns:
            value = row[name]
            if value is None and nullable:
                continue
            if type(value) is not _TYPES[kind]:
                raise RowValidationError("wrong value type: " + name)
            if kind == "float" and not isfinite(value):
                raise RowValidationError("nonfinite float: " + name)
        return dict(row)
