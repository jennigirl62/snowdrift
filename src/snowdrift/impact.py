"""Impact checks: what does a drift event break downstream?

Each check is driven by configuration, so you describe your own platform
(merge keys, hand-written SQL, views, full-refresh tables) without code changes.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field

from .models import Comparison, DriftEvent, DriftType, Impact, Severity

COLUMN_SET_CHANGES = {
    DriftType.COLUMN_MISSING,
    DriftType.COLUMN_EXTRA,
    DriftType.COLUMN_ADDED,
    DriftType.COLUMN_DROPPED,
    DriftType.POSSIBLE_RENAME,
}
# Changes that break SQL which names the column (as opposed to changes that only need a look).
STRUCTURAL = COLUMN_SET_CHANGES | {DriftType.TYPE_CHANGED}
# Changes to a key column that can change which rows a merge matches.
KEY_SENSITIVE = STRUCTURAL | {
    DriftType.LENGTH_SHRUNK,
    DriftType.PRECISION_SHRUNK,
    DriftType.NULLABILITY_CHANGED,
    DriftType.COLLATION_MISMATCH,
}


@dataclass
class ImpactSettings:
    key_columns: dict[str, list[str]] = field(default_factory=dict)  # TABLE -> merge/upsert key columns
    full_refresh: set[str] = field(default_factory=set)  # tables reloaded in full each run
    stored_sql: dict[str, str] = field(default_factory=dict)  # name -> hand-written SQL text (merges, procs)
    views: dict[str, str] = field(default_factory=dict)  # view name -> definition


def _mentions(text: str, word: str) -> bool:
    return re.search(rf"(?<![A-Za-z0-9_$]){re.escape(word)}(?![A-Za-z0-9_$])", text, re.IGNORECASE) is not None


def _columns_of(event: DriftEvent) -> list[str]:
    names = [event.column] if event.column else []
    if event.drift_type is DriftType.POSSIBLE_RENAME and event.value_b:
        names.append(event.value_b)
    return [n for n in names if n]


def key_columns(event: DriftEvent, s: ImpactSettings) -> list[Impact]:
    if event.drift_type not in KEY_SENSITIVE:
        return []
    keys = {k.upper() for k in s.key_columns.get(event.table.upper(), [])}
    hit = [c for c in _columns_of(event) if c.upper() in keys]
    if not hit:
        return []
    return [
        Impact("key_columns", "KEY", event.table, True, f"{', '.join(hit)} is part of the merge/upsert key; key logic changes.")
    ]


def _sql_objects(
    event: DriftEvent, objects: dict[str, str], check: str, object_type: str, table_level_breaks: bool
) -> list[Impact]:
    """table_level_breaks: True when SQL that only names the table still breaks on a column-set change
    (e.g. a MERGE with an explicit column list). False for views, which keep working without a new column."""
    out = []
    cols = _columns_of(event)
    for name, text in sorted(objects.items()):
        if not _mentions(text, event.table):
            continue
        if any(_mentions(text, c) for c in cols):
            breaks = event.drift_type in STRUCTURAL
            note = "References the changed column directly." if breaks else "References the column; review."
            out.append(Impact(check, object_type, name, breaks, note))
        elif table_level_breaks and event.drift_type in COLUMN_SET_CHANGES - {DriftType.COLUMN_EXTRA}:
            out.append(
                Impact(
                    check,
                    object_type,
                    name,
                    True,
                    "Uses this table; if it lists columns explicitly, it won't pick up the change.",
                )
            )
        else:
            out.append(Impact(check, object_type, name, False, "Uses this table; review."))
    return out


def stored_sql(event: DriftEvent, s: ImpactSettings) -> list[Impact]:
    return _sql_objects(event, s.stored_sql, "stored_sql", "SQL", table_level_breaks=True)


def views(event: DriftEvent, s: ImpactSettings) -> list[Impact]:
    return _sql_objects(event, s.views, "views", "VIEW", table_level_breaks=False)


def full_refresh(event: DriftEvent, s: ImpactSettings) -> list[Impact]:
    if event.table.upper() not in {t.upper() for t in s.full_refresh}:
        return []
    return [
        Impact(
            "full_refresh",
            "LOAD",
            event.table,
            False,
            "Full-refresh table: data reloads each run, but the table definition still needs the fix.",
        )
    ]


CHECKS = [key_columns, stored_sql, views, full_refresh]


def assess(event: DriftEvent, settings: ImpactSettings, checks=CHECKS) -> DriftEvent:
    """Attach impacts and raise severity to breaking when anything downstream breaks.

    Day-over-day events keep their severity: they are context, and the matching
    cross-platform event carries the alarm, so one root cause isn't reported as breaking twice.
    """
    for check in checks:
        event.impacts.extend(check(event, settings))
    if event.comparison is not Comparison.DAY_OVER_DAY and any(i.breaks for i in event.impacts):
        event.severity = Severity.BREAKING
    return event
