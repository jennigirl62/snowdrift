"""Run all comparisons for one snapshot of every platform."""

from __future__ import annotations

from dataclasses import dataclass, field

from .models import Column, Comparison, DriftEvent, Severity
from .rules import check_collation, compare_table


@dataclass
class DetectSettings:
    reference: str  # the platform everything should match, usually the source system
    targets: list[str]
    tables: list[str] = field(default_factory=list)  # empty = every table the sources return
    ignore_columns: list[str] = field(default_factory=list)  # e.g. load audit columns that exist only in targets
    expected_collation: dict[str, str] = field(default_factory=dict)  # platform -> collation
    min_severity: Severity = Severity.WARNING


def by_table(cols: list[Column], layer: str | None = None, warnings: list[str] | None = None) -> dict[str, list[Column]]:
    """Group columns by table name. If one name exists in several schemas of the same layer,
    keep the first schema alphabetically and warn, rather than silently mixing their columns."""
    schemas: dict[str, str] = {}
    out: dict[str, list[Column]] = {}
    skipped: set[tuple[str, str, str]] = set()
    for c in sorted(cols, key=lambda c: (c.schema.upper(), c.ordinal)):
        if layer is not None and c.layer != layer:
            continue
        t = c.table.upper()
        first = schemas.setdefault(t, c.schema.upper())
        if c.schema.upper() != first:
            skipped.add((c.platform, t, c.schema.upper()))
            continue
        out.setdefault(t, []).append(c)
    if warnings is not None:
        for platform, t, schema in sorted(skipped):
            warnings.append(
                f"{platform}: {t} exists in more than one {layer or ''} schema; compared {schemas[t]}, skipped {schema}"
            )
    return out


def detect(
    current: dict[str, list[Column]],
    previous: dict[str, list[Column]] | None,
    s: DetectSettings,
    warnings: list[str] | None = None,
) -> list[DriftEvent]:
    """Platforms missing from `current` (e.g. their fetch failed) are skipped, not treated as empty."""
    monitored = {t.upper() for t in s.tables}
    keep = (lambda t: t in monitored) if monitored else (lambda t: True)
    events: list[DriftEvent] = []

    # 1. Reference vs each target (final tables).
    ref = by_table(current.get(s.reference, []), "final", warnings)
    for target in s.targets:
        if target not in current or s.reference not in current:
            continue
        tgt = by_table(current[target], "final", warnings)
        for table in sorted(t for t in set(ref) | set(tgt) if keep(t)):
            if table not in ref:
                continue  # not in the reference; nothing to compare against
            events += compare_table(
                table, ref[table], tgt.get(table, []), Comparison.CROSS_PLATFORM, s.reference, f"{target}:final", s.ignore_columns
            )

    # 2. Final vs work tables on each platform.
    for platform, cols in current.items():
        final, work = by_table(cols, "final"), by_table(cols, "work", warnings)
        for table in sorted(t for t in work if keep(t) and t in final):
            events += compare_table(
                table, final[table], work[table], Comparison.LAYER, f"{platform}:final", f"{platform}:work", s.ignore_columns
            )

    # 3. Expected settings (collation).
    for platform, expected in s.expected_collation.items():
        for layer in ("final", "work"):
            for table, cols in sorted(by_table(current.get(platform, []), layer).items()):
                if keep(table):
                    events += check_collation(table, cols, platform, expected)

    # 4. Day over day, same platform.
    if previous:
        for platform, cols in current.items():
            if platform not in previous:
                continue
            for layer in ("final", "work"):
                prev, cur = by_table(previous[platform], layer), by_table(cols, layer)
                for table in sorted(t for t in set(prev) | set(cur) if keep(t)):
                    if table not in prev:
                        continue  # newly monitored table, nothing to compare yet
                    events += compare_table(
                        table,
                        prev[table],
                        cur.get(table, []),
                        Comparison.DAY_OVER_DAY,
                        f"{platform}:{layer}:previous",
                        f"{platform}:{layer}:current",
                        s.ignore_columns,
                    )
    return events
