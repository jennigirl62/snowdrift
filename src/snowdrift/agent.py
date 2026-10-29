"""The daily run: snapshot -> detect -> impact -> draft fix -> explain -> record -> alert."""

from __future__ import annotations

import logging
from collections.abc import Iterable

from . import ai
from .ddl import draft_fix
from .detector import DetectSettings, detect
from .impact import ImpactSettings, assess
from .models import Column
from .report import RunReport
from .sources import MetadataSource
from .store import Store

log = logging.getLogger(__name__)


def run(
    run_date: str,
    sources: dict[str, MetadataSource],
    detect_settings: DetectSettings,
    store: Store,
    impact_settings: ImpactSettings | None = None,
    dialects: dict[str, str] | None = None,
    llm: ai.LLMClient | None = None,
    notifiers: Iterable = (),
) -> RunReport:
    impact_settings = impact_settings or ImpactSettings()
    dialects = dialects or {}
    llm = llm or ai.NoLLM()
    warnings: list[str] = []

    # 1. Snapshot every platform.
    current: dict[str, list[Column]] = {}
    for platform, source in sources.items():
        try:
            current[platform] = source.fetch(detect_settings.tables)
        except Exception as exc:
            warnings.append(f"could not read {platform} metadata: {exc}")
            log.exception("source %s failed", platform)
            continue
        if not current[platform]:
            warnings.append(f"{platform} returned no columns; check the source settings")
        store.save_snapshot(run_date, platform, current[platform])

    if detect_settings.reference not in current:
        warnings.append(f"reference platform {detect_settings.reference} unavailable; cross-platform checks skipped")

    # 2. Detect, using yesterday's snapshot for day-over-day changes.
    prev_date, previous = store.previous_snapshot(run_date)
    events = detect(current, previous, detect_settings, warnings)

    # 3. Impact and draft fixes, then drop anything below the reporting threshold.
    for e in events:
        assess(e, impact_settings)
        sql = draft_fix(e, dialects)
        if sql:
            e.fix[e.platform_b] = sql
    events = [e for e in events if e.severity.rank >= detect_settings.min_severity.rank]

    # 4. Record. Only new events go to the model; open ones keep yesterday's explanation.
    reported, new_ids, suppressed = store.record_events(run_date, events, platforms_read=set(current))
    fresh = [e for e in reported if e.drift_id in new_ids]
    ai.enrich(fresh, llm, detect_settings.min_severity)
    store.save_summaries(fresh)
    store.load_summaries([e for e in reported if e.drift_id not in new_ids])

    report = RunReport(
        run_date=run_date, events=reported, new_ids=new_ids, suppressed=suppressed, previous_date=prev_date, warnings=warnings
    )
    report.summary = ai.summarize_run(report.sorted_events(), new_ids, llm)

    # 5. Alert.
    for n in notifiers:
        try:
            n.send(report)
        except Exception as exc:
            report.warnings.append(f"{type(n).__name__} failed: {exc}")
            log.exception("notifier failed")
    return report
