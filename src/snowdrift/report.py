"""Turn a run's results into Markdown (email, files) and short text (chat)."""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field

from .models import DriftEvent

ORDER = {"breaking": 0, "warning": 1, "info": 2}


@dataclass
class RunReport:
    run_date: str
    events: list[DriftEvent]
    new_ids: set[str]
    suppressed: int = 0
    summary: str = ""
    previous_date: str | None = None
    warnings: list[str] = field(default_factory=list)

    def sorted_events(self) -> list[DriftEvent]:
        return sorted(
            self.events, key=lambda e: (ORDER[e.severity.value], e.drift_id not in self.new_ids, e.table, e.column or "")
        )

    def counts(self) -> Counter:
        return Counter(e.severity.value for e in self.events)

    @property
    def breaking_new(self) -> list[DriftEvent]:
        return [e for e in self.events if e.severity.value == "breaking" and e.drift_id in self.new_ids]

    def to_text(self) -> str:
        c = self.counts()
        return (
            f"Schema drift {self.run_date}: {c.get('breaking', 0)} breaking, {c.get('warning', 0)} warning, "
            f"{c.get('info', 0)} info ({len(self.new_ids)} new).\n{self.summary}"
        )

    def to_markdown(self) -> str:
        c = self.counts()
        lines = [
            f"# Schema drift report: {self.run_date}",
            "",
            self.summary,
            "",
            "| Severity | Open | New today |",
            "| --- | --- | --- |",
        ]
        for sev in ("breaking", "warning", "info"):
            new = sum(1 for e in self.events if e.severity.value == sev and e.drift_id in self.new_ids)
            lines.append(f"| {sev} | {c.get(sev, 0)} | {new} |")
        lines.append("")
        if self.previous_date:
            lines.append(f"Compared with snapshot from {self.previous_date}.")
        if self.suppressed:
            lines.append(f"{self.suppressed} event(s) suppressed as false positives.")
        for w in self.warnings:
            lines.append(f"> Warning: {w}")
        lines.append("")
        for e in self.sorted_events():
            tag = "NEW " if e.drift_id in self.new_ids else ""
            lines += [f"## {tag}[{e.severity.value}] {e.label()}", "", f"`{e.drift_id}`  {e.detail}".rstrip(), ""]
            if e.summary:
                lines += [e.summary, ""]
            for i in e.impacts:
                lines.append(f"- {'BREAKS' if i.breaks else 'review'}: {i.object_type} {i.object_name}: {i.note}")
            if e.impacts:
                lines.append("")
            if e.fix:
                for platform, sql in e.fix.items():
                    lines += [f"Draft fix ({platform}):", "", "```sql", sql.rstrip(), "```", ""]
        return "\n".join(lines).rstrip() + "\n"

    def to_rows(self) -> list[dict]:
        """One flat row per event, ready for display(spark.createDataFrame(...)) or pandas.DataFrame(...).

        Every value is a non-null string (or a bool for `new`), so Spark and Snowpark can always infer the types.
        """
        rows = [
            {
                "drift_id": e.drift_id,
                "new": e.drift_id in self.new_ids,
                "severity": e.severity.value,
                "drift_type": e.drift_type.value,
                "table_name": e.table,
                "column_name": e.column,
                "side_a": e.side_a,
                "side_b": e.side_b,
                "value_a": e.value_a,
                "value_b": e.value_b,
                "breaks": ", ".join(f"{i.object_type} {i.object_name}" for i in e.impacts if i.breaks),
                "summary": e.summary,
                "fix_sql": "\n\n".join(e.fix.values()),
            }
            for e in self.sorted_events()
        ]
        return [{k: (v if isinstance(v, bool) else ("" if v is None else str(v))) for k, v in r.items()} for r in rows]

    def show(self) -> None:
        """Render the report in a notebook (Databricks, Snowflake, Jupyter); plain text anywhere else."""
        md = self.to_markdown()
        try:
            from IPython import get_ipython
            from IPython.display import Markdown, display

            if get_ipython() is not None:
                display(Markdown(md))
                return
        except ImportError:
            pass
        print(md)
