"""Normalize platform-specific data types so they can be compared.

Teradata VARCHAR(50), Snowflake TEXT with length 50 and Databricks STRING all
describe the same thing. This module reduces each to a NormType(family, length,
precision, scale). Getting this right is what keeps false positives down.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

from .models import Column

# Teradata reports two-letter codes in dbc.ColumnsV.ColumnType.
TERADATA_CODES: dict[str, str] = {
    "CV": "VARCHAR",
    "CF": "CHAR",
    "I1": "BYTEINT",
    "I2": "SMALLINT",
    "I": "INTEGER",
    "I8": "BIGINT",
    "D": "DECIMAL",
    "N": "NUMBER",
    "F": "FLOAT",
    "DA": "DATE",
    "TS": "TIMESTAMP",
    "SZ": "TIMESTAMP",
    "AT": "TIME",
    "TZ": "TIME",
    "CO": "CLOB",
    "BO": "BLOB",
    "BF": "BYTE",
    "BV": "VARBYTE",
}

# Integer names carry a fixed precision (decimal digits) so INT vs BIGINT can be compared.
INTEGER_PRECISION: dict[str, int] = {
    "BYTEINT": 3,
    "TINYINT": 3,
    "SMALLINT": 5,
    "INT": 10,
    "INTEGER": 10,
    "BIGINT": 19,
    "LONG": 19,
}

FAMILIES: dict[str, str] = {
    **{n: "STRING" for n in ("VARCHAR", "CHAR", "CHARACTER", "STRING", "TEXT", "NVARCHAR", "NCHAR")},
    **{n: "INT" for n in INTEGER_PRECISION},
    **{n: "DECIMAL" for n in ("DECIMAL", "NUMERIC", "NUMBER", "DEC")},
    **{n: "FLOAT" for n in ("FLOAT", "FLOAT4", "FLOAT8", "DOUBLE", "REAL", "DOUBLE PRECISION")},
    "DATE": "DATE",
    **{n: "TIMESTAMP" for n in ("TIMESTAMP", "TIMESTAMP_NTZ", "TIMESTAMP_LTZ", "TIMESTAMP_TZ", "DATETIME")},
    "TIME": "TIME",
    **{n: "BOOLEAN" for n in ("BOOLEAN", "BOOL")},
    **{n: "BINARY" for n in ("BINARY", "VARBINARY", "BYTE", "VARBYTE", "BLOB")},
    "CLOB": "CLOB",
    **{n: "SEMI" for n in ("VARIANT", "OBJECT", "ARRAY", "STRUCT", "MAP")},
}

# Families that hold the same data even though they are named differently.
COMPATIBLE = [{"STRING", "CLOB"}, {"INT", "DECIMAL"}]

_TYPE_RE = re.compile(r"^\s*([A-Z_][A-Z0-9_ ]*?)\s*(?:\(\s*(\d+)\s*(?:,\s*(\d+)\s*)?\))?(?:\s|<|$)")


@dataclass(frozen=True)
class NormType:
    family: str
    length: int | None = None
    precision: int | None = None
    scale: int | None = None

    def __str__(self) -> str:
        if self.family == "DECIMAL" and self.precision is not None:
            return f"DECIMAL({self.precision},{self.scale or 0})"
        if self.family == "INT" and self.precision is not None:
            return f"INT({self.precision})"
        if self.family in ("STRING", "BINARY") and self.length is not None:
            return f"{self.family}({self.length})"
        return self.family


def _parse(raw: str) -> tuple[str, int | None, int | None]:
    text = raw.strip().upper()
    match = _TYPE_RE.match(text + " ")
    if not match:
        return text, None, None
    base = match.group(1).strip()
    p1 = int(match.group(2)) if match.group(2) else None
    p2 = int(match.group(3)) if match.group(3) else None
    if base not in FAMILIES and " " in base:
        base = base.split()[0]
    return base, p1, p2


def _first(*values: int | None) -> int | None:
    for v in values:
        if v is not None and v >= 0:
            return v
    return None


def normalize(col: Column) -> NormType:
    raw = (col.raw_type or "").strip().upper()
    if raw in TERADATA_CODES:
        raw = TERADATA_CODES[raw]
    base, p1, p2 = _parse(raw)
    family = FAMILIES.get(base, "UNKNOWN")

    if family == "STRING":
        return NormType("STRING", length=_first(col.length, p1))
    if family == "BINARY":
        return NormType("BINARY", length=_first(col.length, p1))
    if family == "INT":
        # Use the name, not reported precision: some catalogs report bits, not digits.
        return NormType("INT", precision=INTEGER_PRECISION[base])
    if family == "DECIMAL":
        precision = _first(col.precision, p1)
        scale = _first(col.scale, p2)
        if scale in (0, None) and precision is not None:
            return NormType("INT", precision=precision)
        return NormType("DECIMAL", precision=precision, scale=scale)
    if family == "UNKNOWN":
        return NormType(f"UNKNOWN:{base}")
    return NormType(family)


def compatible(a: NormType, b: NormType) -> bool:
    if a.family == b.family:
        return True
    return any({a.family, b.family} <= group for group in COMPATIBLE)
