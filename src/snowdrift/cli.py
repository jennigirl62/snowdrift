"""Command line: snowdrift run | demo | events | mark."""

from __future__ import annotations

import argparse
import datetime as dt
import logging
import sys
import tempfile
from importlib.resources import files
from pathlib import Path

from . import agent, config
from .alerts import ConsoleNotifier
from .detector import DetectSettings
from .impact import ImpactSettings
from .models import Severity
from .sources import CsvSource, TeradataExportSource
from .store import STATUSES, Store


def _demo(args) -> int:
    data = files("snowdrift") / "demo_data"
    tables = ["CUSTOMERS", "JOBS", "INVOICES"]
    settings = DetectSettings(
        reference="teradata",
        targets=["snowflake", "databricks"],
        tables=tables,
        ignore_columns=["LOAD_TS", "BATCH_ID"],
        expected_collation={"snowflake": "en-ci"},
        min_severity=Severity(args.min_severity),
    )
    impact = ImpactSettings(
        key_columns={"JOBS": ["JOB_ID"], "INVOICES": ["INVOICE_ID"], "CUSTOMERS": ["CUSTOMER_ID"]},
        full_refresh={"CUSTOMERS"},
        stored_sql={p.name[:-4].upper(): p.read_text() for p in (data / "sql").iterdir() if p.name.endswith(".sql")},
        views={p.name[:-4].upper(): p.read_text() for p in (data / "views").iterdir() if p.name.endswith(".sql")},
    )
    dialects = {"snowflake": "snowflake", "databricks": "databricks", "teradata": "teradata"}

    def read(rel: str) -> str:
        # Read through importlib.resources so the demo also works when snowdrift is imported from a zip.
        return data.joinpath(*rel.split("/")).read_text(encoding="utf-8-sig")

    def sources(day: str):
        return {
            "teradata": TeradataExportSource("teradata", f"{day}/teradata_columns.csv", read_text=read),
            "snowflake": CsvSource("snowflake", f"{day}/snowflake_columns.csv", work_schemas=["STAGE"], read_text=read),
            "databricks": CsvSource("databricks", f"{day}/databricks_columns.csv", work_schemas=["STAGE"], read_text=read),
        }

    with tempfile.TemporaryDirectory() as tmp:
        store = Store(str(Path(tmp) / "demo.db"))
        print("Day 1: baseline snapshot of a fictional landscaping company's warehouse.\n")
        day1 = agent.run("2026-10-27", sources("day1"), settings, store, impact, dialects)
        print(f"Day 1 result: {len(day1.events)} event(s) at or above '{args.min_severity}'.\n")
        print("Day 2: the source system changed overnight.\n")
        day2 = agent.run("2026-10-28", sources("day2"), settings, store, impact, dialects, notifiers=[ConsoleNotifier()])
        store.close()
    if args.out:
        Path(args.out).write_text(day2.to_markdown())
        print(f"Report written to {args.out}")
    return 0


def _run(args) -> int:
    cfg = config.load(args.config)
    store = Store(cfg.store_path)
    try:
        report = agent.run(args.date, cfg.sources, cfg.detect, store, cfg.impact, cfg.dialects, cfg.llm, cfg.notifiers)
    finally:
        store.close()
    if args.out:
        Path(args.out).write_text(report.to_markdown())
    if args.fail_on_breaking and report.breaking_new:
        return 2
    return 0


def _events(args) -> int:
    cfg = config.load(args.config, connect_sources=False)
    store = Store(cfg.store_path)
    for r in store.list_events(args.status):
        print(
            f"{r['drift_id']}  {r['status']:<14} {r['severity']:<9} {r['drift_type']:<20} "
            f"{r['table_name']}.{r['column_name'] or '-'}  ({r['side_a']} vs {r['side_b']})"
        )
    store.close()
    return 0


def _mark(args) -> int:
    cfg = config.load(args.config, connect_sources=False)
    store = Store(cfg.store_path)
    ok = store.set_status(args.drift_id, args.status)
    store.close()
    print("updated" if ok else f"no event {args.drift_id}")
    return 0 if ok else 1


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="snowdrift", description="Catch schema drift before it breaks a load.")
    p.add_argument("-v", "--verbose", action="store_true")
    sub = p.add_subparsers(dest="cmd", required=True)

    d = sub.add_parser("demo", help="run a two-day demo on sample data")
    d.add_argument("--min-severity", default="warning", choices=[s.value for s in Severity])
    d.add_argument("--out", help="also write the day-2 report to this Markdown file")
    d.set_defaults(func=_demo)

    r = sub.add_parser("run", help="snapshot, detect, alert")
    r.add_argument("-c", "--config", required=True)
    r.add_argument("--date", default=dt.date.today().isoformat())
    r.add_argument("--out", help="write the report to this Markdown file")
    r.add_argument("--fail-on-breaking", action="store_true", help="exit 2 if new breaking drift is found")
    r.set_defaults(func=_run)

    e = sub.add_parser("events", help="list recorded drift events")
    e.add_argument("-c", "--config", required=True)
    e.add_argument("--status", choices=STATUSES)
    e.set_defaults(func=_events)

    m = sub.add_parser("mark", help="set an event's status, e.g. false_positive")
    m.add_argument("-c", "--config", required=True)
    m.add_argument("drift_id")
    m.add_argument("status", choices=STATUSES)
    m.set_defaults(func=_mark)

    args = p.parse_args(argv)
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.WARNING)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
