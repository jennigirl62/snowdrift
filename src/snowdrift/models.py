"""Core data structures shared by every part of the agent."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from enum import StrEnum


class Severity(StrEnum):
    INFO = "info"
    WARNING = "warning"
    BREAKING = "breaking"

    @property
    def rank(self) -> int:
        return {"info": 0, "warning": 1, "breaking": 2}[self.value]


def max_severity(*levels: Severity) -> Severity:
    return max(levels, key=lambda s: s.rank)


class Comparison(StrEnum):
    CROSS_PLATFORM = "cross_platform"  # reference platform vs a target platform
    LAYER = "layer"  # final table vs its work/staging table on one platform
    DAY_OVER_DAY = "day_over_day"  # yesterday's snapshot vs today's, same platform
    CONFIG = "config"  # a platform vs an expected setting (e.g. collation)


class DriftType(StrEnum):
    TABLE_MISSING = "table_missing"
    COLUMN_MISSING = "column_missing"  # in side A, not in side B
    COLUMN_EXTRA = "column_extra"  # in side B, not in side A
    COLUMN_ADDED = "column_added"  # day over day
    COLUMN_DROPPED = "column_dropped"  # day over day
    TYPE_CHANGED = "type_changed"
    LENGTH_SHRUNK = "length_shrunk"
    LENGTH_GREW = "length_grew"
    PRECISION_SHRUNK = "precision_shrunk"
    PRECISION_GREW = "precision_grew"
    NULLABILITY_CHANGED = "nullability_changed"
    COLLATION_MISMATCH = "collation_mismatch"
    ORDER_CHANGED = "order_changed"
    POSSIBLE_RENAME = "possible_rename"


@dataclass(frozen=True)
class Column:
    """One column as reported by a platform's metadata catalog."""

    platform: str
    schema: str
    table: str
    name: str
    ordinal: int
    raw_type: str
    nullable: bool = True
    length: int | None = None
    precision: int | None = None
    scale: int | None = None
    collation: str | None = None
    layer: str = "final"  # "final" or "work"


@dataclass
class Impact:
    """Something downstream that a drift event touches."""

    check: str
    object_type: str
    object_name: str
    breaks: bool
    note: str


@dataclass
class DriftEvent:
    comparison: Comparison
    drift_type: DriftType
    table: str
    column: str | None
    side_a: str
    side_b: str
    value_a: str | None
    value_b: str | None
    severity: Severity
    detail: str = ""
    col_a: Column | None = field(default=None, repr=False, compare=False)
    col_b: Column | None = field(default=None, repr=False, compare=False)
    schema_b: str | None = None
    impacts: list[Impact] = field(default_factory=list)
    summary: str | None = None
    fix: dict[str, str] = field(default_factory=dict)

    @property
    def drift_id(self) -> str:
        """Stable id: the same drift on the next run gets the same id, so it isn't re-alerted."""
        raw = "|".join(
            [
                self.comparison.value,
                self.drift_type.value,
                self.table.upper(),
                (self.column or "").upper(),
                self.side_a,
                self.side_b,
                self.value_a or "",
                self.value_b or "",
            ]
        )
        return hashlib.sha1(raw.encode()).hexdigest()[:12]

    @property
    def platform_b(self) -> str:
        return self.side_b.split(":")[0]

    def label(self) -> str:
        where = f"{self.table}.{self.column}" if self.column else self.table
        return f"{self.drift_type.value} on {where} ({self.side_a} vs {self.side_b})"
