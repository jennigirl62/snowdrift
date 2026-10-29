"""Deterministic drift rules. No AI here: detection has to be exact and repeatable."""

from __future__ import annotations

import difflib
from collections.abc import Iterable

from .models import Column, Comparison, DriftEvent, DriftType, Severity
from .types import NormType, compatible, normalize

B, W, INF = Severity.BREAKING, Severity.WARNING, Severity.INFO

# Default severity for each drift type, by kind of comparison.
SEVERITY: dict[Comparison, dict[DriftType, Severity]] = {
    Comparison.CROSS_PLATFORM: {
        DriftType.TABLE_MISSING: B,
        DriftType.COLUMN_MISSING: B,  # target can't receive a source column
        DriftType.COLUMN_EXTRA: W,
        DriftType.TYPE_CHANGED: B,
        DriftType.LENGTH_SHRUNK: B,  # target shorter than source: truncation
        DriftType.LENGTH_GREW: INF,
        DriftType.PRECISION_SHRUNK: B,
        DriftType.PRECISION_GREW: INF,
        DriftType.NULLABILITY_CHANGED: W,
        DriftType.ORDER_CHANGED: INF,
        DriftType.POSSIBLE_RENAME: B,  # the target still lacks the reference column either way
    },
    Comparison.LAYER: {
        DriftType.TABLE_MISSING: B,
        DriftType.COLUMN_MISSING: B,
        DriftType.COLUMN_EXTRA: B,
        DriftType.TYPE_CHANGED: B,
        DriftType.LENGTH_SHRUNK: B,
        DriftType.LENGTH_GREW: B,  # work wider than final: values won't fit on merge
        DriftType.PRECISION_SHRUNK: B,
        DriftType.PRECISION_GREW: B,
        DriftType.NULLABILITY_CHANGED: W,
        DriftType.ORDER_CHANGED: INF,
        DriftType.POSSIBLE_RENAME: B,
    },
    Comparison.DAY_OVER_DAY: {
        DriftType.TABLE_MISSING: W,
        DriftType.COLUMN_DROPPED: W,
        DriftType.COLUMN_ADDED: W,
        DriftType.TYPE_CHANGED: W,
        DriftType.LENGTH_SHRUNK: W,
        DriftType.LENGTH_GREW: INF,
        DriftType.PRECISION_SHRUNK: W,
        DriftType.PRECISION_GREW: INF,
        DriftType.NULLABILITY_CHANGED: W,
        DriftType.ORDER_CHANGED: INF,
        DriftType.POSSIBLE_RENAME: W,
    },
    Comparison.CONFIG: {DriftType.COLLATION_MISMATCH: W},
}


def _event(kind, dtype, table, column, side_a, side_b, va, vb, detail="", col_a=None, col_b=None, schema_b=None):
    return DriftEvent(
        comparison=kind,
        drift_type=dtype,
        table=table,
        column=column,
        side_a=side_a,
        side_b=side_b,
        value_a=va,
        value_b=vb,
        severity=SEVERITY[kind][dtype],
        detail=detail,
        col_a=col_a,
        col_b=col_b,
        schema_b=schema_b,
    )


def compare_table(
    table: str,
    cols_a: list[Column],
    cols_b: list[Column],
    kind: Comparison,
    side_a: str,
    side_b: str,
    ignore_columns: Iterable[str] = (),
) -> list[DriftEvent]:
    """Compare one table's columns on side A (reference/previous) against side B (target/current)."""
    ignore = {c.upper() for c in ignore_columns}
    a = {c.name.upper(): c for c in cols_a if c.name.upper() not in ignore}
    b = {c.name.upper(): c for c in cols_b if c.name.upper() not in ignore}
    schema_b = cols_b[0].schema if cols_b else None
    events: list[DriftEvent] = []

    if not cols_b:
        return [_event(kind, DriftType.TABLE_MISSING, table, None, side_a, side_b, "present", "absent")]

    missing = sorted(set(a) - set(b), key=lambda n: a[n].ordinal)
    extra = sorted(set(b) - set(a), key=lambda n: b[n].ordinal)
    day = kind is Comparison.DAY_OVER_DAY
    gone, new = (DriftType.COLUMN_DROPPED, DriftType.COLUMN_ADDED) if day else (DriftType.COLUMN_MISSING, DriftType.COLUMN_EXTRA)

    renames = _renames(table, missing, extra, a, b, kind, side_a, side_b, schema_b)
    paired = {e.value_a for e in renames} | {e.value_b for e in renames}
    # A paired rename replaces its missing/extra events, so there is one event (and one fix) per real change.
    for name in (m for m in missing if m not in paired):
        t = normalize(a[name])
        events.append(_event(kind, gone, table, name, side_a, side_b, str(t), None, col_a=a[name], schema_b=schema_b))
    for name in (x for x in extra if x not in paired):
        t = normalize(b[name])
        events.append(_event(kind, new, table, name, side_a, side_b, None, str(t), col_b=b[name], schema_b=schema_b))
    events.extend(renames)

    for name in sorted(set(a) & set(b), key=lambda n: a[n].ordinal):
        events.extend(_compare_column(table, a[name], b[name], kind, side_a, side_b, schema_b))

    common = set(a) & set(b)
    order_a = [n for n in sorted(common, key=lambda n: a[n].ordinal)]
    order_b = [n for n in sorted(common, key=lambda n: b[n].ordinal)]
    if order_a != order_b:
        pos = next(i for i, (x, y) in enumerate(zip(order_a, order_b, strict=True)) if x != y)
        events.append(
            _event(
                kind,
                DriftType.ORDER_CHANGED,
                table,
                None,
                side_a,
                side_b,
                order_a[pos],
                order_b[pos],
                detail=f"column order differs from position {pos + 1}",
            )
        )
    return events


def _compare_column(table, ca: Column, cb: Column, kind, side_a, side_b, schema_b) -> list[DriftEvent]:
    ta, tb = normalize(ca), normalize(cb)
    out: list[DriftEvent] = []

    def add(dtype, va, vb, detail=""):
        out.append(_event(kind, dtype, table, ca.name.upper(), side_a, side_b, va, vb, detail, ca, cb, schema_b))

    if not compatible(ta, tb):
        add(DriftType.TYPE_CHANGED, str(ta), str(tb))
        return out
    if {ta.family, tb.family} == {"INT", "DECIMAL"}:
        # An integer is a decimal with scale 0: INTEGER into NUMBER(20,2) fits, INTEGER into NUMBER(10,2) doesn't.
        ta, tb = _as_decimal(ta), _as_decimal(tb)
    if ta.length is not None and tb.length is not None and ta.length != tb.length:
        add(DriftType.LENGTH_SHRUNK if tb.length < ta.length else DriftType.LENGTH_GREW, str(ta), str(tb))
    _precision(ta, tb, add)
    if ca.nullable != cb.nullable:
        add(DriftType.NULLABILITY_CHANGED, "NULL" if ca.nullable else "NOT NULL", "NULL" if cb.nullable else "NOT NULL")
        # Breaking when the table that receives rows rejects nulls the sending table allows.
        # Cross-platform: rows flow A -> B. Final vs work: rows flow work (B) -> final (A).
        if kind is Comparison.CROSS_PLATFORM and ca.nullable and not cb.nullable:
            out[-1].severity = Severity.BREAKING
        if kind is Comparison.LAYER and cb.nullable and not ca.nullable:
            out[-1].severity = Severity.BREAKING
    return out


def _precision(ta: NormType, tb: NormType, add) -> None:
    if ta.family != tb.family or ta.precision is None or tb.precision is None:
        return
    sa, sb = ta.scale or 0, tb.scale or 0
    # Shrunk if B has fewer whole-number digits or fewer decimal places: DECIMAL(10,2) -> (10,4) overflows.
    shrunk = (tb.precision - sb) < (ta.precision - sa) or sb < sa
    grew = not shrunk and (tb.precision > ta.precision or sb > sa)
    if shrunk:
        add(DriftType.PRECISION_SHRUNK, str(ta), str(tb))
    elif grew:
        add(DriftType.PRECISION_GREW, str(ta), str(tb))


def _as_decimal(t: NormType) -> NormType:
    if t.family == "INT" and t.precision is not None:
        return NormType("DECIMAL", precision=t.precision, scale=0)
    return t


def _renames(table, missing, extra, a, b, kind, side_a, side_b, schema_b) -> list[DriftEvent]:
    """Pair a column that vanished with a column that appeared if they look like the same column."""
    out = []
    taken: set[str] = set()
    for m in missing:
        tm = normalize(a[m])
        best, best_score = None, 0.0
        for e in extra:
            if e in taken or not same_shape(tm, normalize(b[e])):
                continue
            score = difflib.SequenceMatcher(None, m, e).ratio()
            if score > best_score:
                best, best_score = e, score
        lone_pair = len(missing) == 1 and len(extra) == 1
        if best and (best_score >= 0.5 or (lone_pair and best_score >= 0.3)):
            taken.add(best)
            out.append(
                _event(
                    kind,
                    DriftType.POSSIBLE_RENAME,
                    table,
                    m,
                    side_a,
                    side_b,
                    m,
                    best,
                    detail=f"same type {tm}, name similarity {best_score:.2f}",
                    col_a=a[m],
                    col_b=b[best],
                    schema_b=schema_b,
                )
            )
    return out


def same_shape(a: NormType, b: NormType) -> bool:
    """Same data type, treating a missing length/precision (e.g. Databricks STRING) as a match."""
    if not compatible(a, b):
        return False
    for x, y in ((a.length, b.length), (a.precision, b.precision), (a.scale, b.scale)):
        if x is not None and y is not None and x != y:
            return False
    return True


def check_collation(table: str, cols: list[Column], platform: str, expected: str) -> list[DriftEvent]:
    out = []
    for c in cols:
        if c.collation and c.collation.lower() != expected.lower():
            out.append(
                _event(
                    Comparison.CONFIG,
                    DriftType.COLLATION_MISMATCH,
                    table,
                    c.name.upper(),
                    "config",
                    f"{platform}:{c.layer}",
                    expected,
                    c.collation,
                    col_b=c,
                    schema_b=c.schema,
                )
            )
    return out
