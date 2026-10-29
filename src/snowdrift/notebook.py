"""One call to run snowdrift from a notebook cell.

    from snowdrift.notebook import check
    report = check(sources=..., reference="teradata", targets=["snowflake", "databricks"], store=...)

See notebooks/ for complete Databricks and Snowflake examples.
"""

from __future__ import annotations

import datetime as dt
from collections.abc import Iterable, Mapping

from . import agent
from .ai import LLMClient
from .detector import DetectSettings
from .impact import ImpactSettings
from .models import Severity
from .report import RunReport
from .sources import MetadataSource
from .store import Backend, Store


def _guess_dialect(platform: str) -> str:
    p = platform.lower()
    for name in ("snowflake", "databricks", "teradata"):
        if name in p:
            return name
    return p


def check(
    *,
    sources: Mapping[str, MetadataSource],
    reference: str,
    targets: Iterable[str],
    store: Store | Backend | str,
    tables: Iterable[str] = (),
    run_date: str | dt.date | None = None,
    ignore_columns: Iterable[str] = (),
    expected_collation: Mapping[str, str] | None = None,
    min_severity: str | Severity = "warning",
    key_columns: Mapping[str, list[str]] | None = None,
    full_refresh: Iterable[str] = (),
    stored_sql: Mapping[str, str] | None = None,
    views: Mapping[str, str] | None = None,
    dialects: Mapping[str, str] | None = None,
    llm: LLMClient | None = None,
    notifiers: Iterable = (),
    show: bool = True,
) -> RunReport:
    """Snapshot every source, detect drift, assess impact, draft fixes, explain, record and alert.

    store: a Store, a backend (SparkBackend / SnowparkBackend) or a SQLite file path.
    run_date: defaults to today; pass a date to re-run or backfill a day.
    """
    run_date = (run_date or dt.date.today()).isoformat() if not isinstance(run_date, str) else run_date
    store = store if isinstance(store, Store) else Store(store)
    settings = DetectSettings(
        reference=reference,
        targets=list(targets),
        tables=[t.upper() for t in tables],
        ignore_columns=list(ignore_columns),
        expected_collation=dict(expected_collation or {}),
        min_severity=Severity(min_severity),
    )
    impact = ImpactSettings(
        key_columns={k.upper(): list(v) for k, v in (key_columns or {}).items()},
        full_refresh={t.upper() for t in full_refresh},
        stored_sql=dict(stored_sql or {}),
        views=dict(views or {}),
    )
    dialects = dict(dialects or {p: _guess_dialect(p) for p in sources})
    report = agent.run(run_date, dict(sources), settings, store, impact, dialects, llm, notifiers)
    if show:
        report.show()
    return report
