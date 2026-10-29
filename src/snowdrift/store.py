"""Run history: column snapshots and drift events.

All the bookkeeping (what's new, still open, resolved, suppressed) lives here, in plain
Python, so it behaves the same wherever the history is kept. Where it's kept is a
small backend:

    SqliteBackend("drift.db")                      laptop, CI, demos
    SparkBackend(spark, "main.snowdrift")          Delta tables, Databricks notebooks and jobs
    SnowparkBackend(session, "DB.SNOWDRIFT")       Snowflake tables, Snowflake notebooks
    MemoryBackend()                                tests
"""

from __future__ import annotations

import json
import re
import sqlite3
from collections.abc import Iterable
from dataclasses import asdict
from typing import Protocol

from .models import Column, DriftEvent

type Row = dict[str, object]

SNAPSHOT = "column_snapshot"
EVENTS = "drift_event"

# Column name -> simple type ("string" | "int" | "bool"). Order matters for backends that write positionally.
SCHEMAS: dict[str, list[tuple[str, str]]] = {
    SNAPSHOT: [
        ("run_date", "string"),
        ("platform", "string"),
        ("layer", "string"),
        ("schema_name", "string"),
        ("table_name", "string"),
        ("column_name", "string"),
        ("ordinal", "int"),
        ("raw_type", "string"),
        ("nullable", "bool"),
        ("length", "int"),
        ("precision", "int"),
        ("scale", "int"),
        ("collation", "string"),
    ],
    EVENTS: [
        ("drift_id", "string"),
        ("first_seen", "string"),
        ("last_seen", "string"),
        ("status", "string"),
        ("comparison", "string"),
        ("drift_type", "string"),
        ("table_name", "string"),
        ("column_name", "string"),
        ("side_a", "string"),
        ("side_b", "string"),
        ("value_a", "string"),
        ("value_b", "string"),
        ("severity", "string"),
        ("detail", "string"),
        ("summary", "string"),
        ("impacts_json", "string"),
        ("fix_json", "string"),
    ],
}

OPEN = ("open", "acknowledged")
STATUSES = ("open", "acknowledged", "fixed", "false_positive", "resolved")
_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_$]*(\.[A-Za-z_][A-Za-z0-9_$]*){0,2}$")
_DATE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def _check_ident(name: str) -> str:
    if not _IDENT.match(name):
        raise ValueError(f"unsafe identifier: {name!r}")
    return name


def _lit(value: object) -> str:
    """SQL string literal with quotes escaped. Only used for values snowdrift itself controls (dates, platforms)."""
    return "'" + str(value).replace("'", "''") + "'"


class Backend(Protocol):
    def read(self, table: str, where: dict[str, object] | None = None) -> list[Row]: ...
    def max_below(self, table: str, column: str, value: str) -> str | None: ...
    def delete(self, table: str, where: dict[str, object]) -> None: ...
    def delete_below(self, table: str, column: str, value: str) -> None: ...
    def append(self, table: str, rows: list[Row]) -> None: ...
    def replace(self, table: str, rows: list[Row]) -> None: ...


# --- backends --------------------------------------------------------------------------------


class MemoryBackend:
    def __init__(self):
        self.tables: dict[str, list[Row]] = {SNAPSHOT: [], EVENTS: []}

    def read(self, table, where=None):
        return [dict(r) for r in self.tables[table] if all(r.get(k) == v for k, v in (where or {}).items())]

    def max_below(self, table, column, value):
        vals = [r[column] for r in self.tables[table] if r[column] < value]
        return max(vals) if vals else None

    def delete(self, table, where):
        self.tables[table] = [r for r in self.tables[table] if not all(r.get(k) == v for k, v in where.items())]

    def delete_below(self, table, column, value):
        self.tables[table] = [r for r in self.tables[table] if not r[column] < value]

    def append(self, table, rows):
        self.tables[table].extend(dict(r) for r in rows)

    def replace(self, table, rows):
        self.tables[table] = [dict(r) for r in rows]


class SqliteBackend:
    TYPES = {"string": "TEXT", "int": "INTEGER", "bool": "INTEGER"}

    def __init__(self, path: str = "drift.db"):
        self.db = sqlite3.connect(path)
        self.db.row_factory = sqlite3.Row
        for table, cols in SCHEMAS.items():
            ddl = ", ".join(f"{c} {self.TYPES[t]}" for c, t in cols)
            self.db.execute(f"CREATE TABLE IF NOT EXISTS {table} ({ddl})")

    def close(self) -> None:
        self.db.close()

    @staticmethod
    def _where(where: dict) -> tuple[str, list]:
        if not where:
            return "", []
        return " WHERE " + " AND ".join(f"{_check_ident(k)} = ?" for k in where), list(where.values())

    def _out(self, table: str, r: sqlite3.Row) -> Row:
        types = dict(SCHEMAS[table])
        return {k: (bool(r[k]) if types[k] == "bool" and r[k] is not None else r[k]) for k in r.keys()}

    def read(self, table, where=None):
        clause, args = self._where(where or {})
        return [self._out(table, r) for r in self.db.execute(f"SELECT * FROM {table}{clause}", args)]

    def max_below(self, table, column, value):
        return self.db.execute(f"SELECT MAX({_check_ident(column)}) FROM {table} WHERE {column} < ?", (value,)).fetchone()[0]

    def delete(self, table, where):
        clause, args = self._where(where)
        with self.db:
            self.db.execute(f"DELETE FROM {table}{clause}", args)

    def delete_below(self, table, column, value):
        with self.db:
            self.db.execute(f"DELETE FROM {table} WHERE {_check_ident(column)} < ?", (value,))

    def append(self, table, rows):
        cols = [c for c, _ in SCHEMAS[table]]
        with self.db:
            self.db.executemany(
                f"INSERT INTO {table} ({', '.join(cols)}) VALUES ({', '.join('?' * len(cols))})",
                [[r.get(c) for c in cols] for r in rows],
            )

    def replace(self, table, rows):
        with self.db:
            self.db.execute(f"DELETE FROM {table}")
        self.append(table, rows)


class SparkBackend:
    """Delta tables in Databricks (or any Spark catalog). Tables are created on first write."""

    DDL = {"string": "STRING", "int": "BIGINT", "bool": "BOOLEAN"}

    def __init__(self, spark, schema: str = "main.snowdrift"):
        self.spark = spark
        self.schema = _check_ident(schema)

    def _name(self, table: str) -> str:
        return f"{self.schema}.{table}"

    def _exists(self, table: str) -> bool:
        return self.spark.catalog.tableExists(self._name(table))

    def _ddl(self, table: str) -> str:
        return ", ".join(f"{c} {self.DDL[t]}" for c, t in SCHEMAS[table])

    def read(self, table, where=None):
        if not self._exists(table):
            return []
        df = self.spark.table(self._name(table))
        for k, v in (where or {}).items():
            df = df.filter(df[_check_ident(k)] == v)
        return [r.asDict() for r in df.collect()]

    def max_below(self, table, column, value):
        if not self._exists(table):
            return None
        from pyspark.sql import functions as F

        row = self.spark.table(self._name(table)).filter(F.col(column) < value).agg(F.max(column)).first()
        return row[0] if row else None

    def delete(self, table, where):
        if self._exists(table):
            cond = " AND ".join(f"{_check_ident(k)} = {_lit(v)}" for k, v in where.items())
            self.spark.sql(f"DELETE FROM {self._name(table)} WHERE {cond}")

    def delete_below(self, table, column, value):
        if self._exists(table):
            self.spark.sql(f"DELETE FROM {self._name(table)} WHERE {_check_ident(column)} < {_lit(value)}")

    def _write(self, table, rows, mode):
        cols = [c for c, _ in SCHEMAS[table]]
        df = self.spark.createDataFrame([tuple(r.get(c) for c in cols) for r in rows], schema=self._ddl(table))
        df.write.mode(mode).option("overwriteSchema", "true" if mode == "overwrite" else "false").saveAsTable(self._name(table))

    def append(self, table, rows):
        if rows:
            self._write(table, rows, "append")

    def replace(self, table, rows):
        self._write(table, rows, "overwrite")


class SnowparkBackend:
    """Regular Snowflake tables, through a Snowpark session (e.g. get_active_session() in a notebook)."""

    def __init__(self, session, schema: str):
        self.session = session
        self.schema = _check_ident(schema)

    def _name(self, table: str) -> str:
        return f"{self.schema}.{table.upper()}"

    def _exists(self, table: str) -> bool:
        return bool(self.session.sql(f"SHOW TABLES LIKE {_lit(table.upper())} IN SCHEMA {self.schema}").collect())

    @staticmethod
    def _row(r) -> Row:
        return {k.lower(): v for k, v in r.as_dict().items()}

    def read(self, table, where=None):
        if not self._exists(table):
            return []
        from snowflake.snowpark.functions import col

        df = self.session.table(self._name(table))
        for k, v in (where or {}).items():
            df = df.filter(col(_check_ident(k).upper()) == v)
        return [self._row(r) for r in df.collect()]

    def max_below(self, table, column, value):
        if not self._exists(table):
            return None
        from snowflake.snowpark.functions import col
        from snowflake.snowpark.functions import max as max_

        c = col(_check_ident(column).upper())
        return self.session.table(self._name(table)).filter(c < value).select(max_(c)).collect()[0][0]

    def delete(self, table, where):
        if not self._exists(table):
            return
        from snowflake.snowpark.functions import col

        cond = None
        for k, v in where.items():
            term = col(_check_ident(k).upper()) == v
            cond = term if cond is None else cond & term
        self.session.table(self._name(table)).delete(cond)

    def delete_below(self, table, column, value):
        if self._exists(table):
            from snowflake.snowpark.functions import col

            self.session.table(self._name(table)).delete(col(_check_ident(column).upper()) < value)

    def _schema(self, table):
        from snowflake.snowpark.types import BooleanType, LongType, StringType, StructField, StructType

        types = {"string": StringType(), "int": LongType(), "bool": BooleanType()}
        return StructType([StructField(c.upper(), types[t]) for c, t in SCHEMAS[table]])

    def _write(self, table, rows, mode):
        cols = [c for c, _ in SCHEMAS[table]]
        df = self.session.create_dataframe([[r.get(c) for c in cols] for r in rows], schema=self._schema(table))
        df.write.mode(mode).save_as_table(self._name(table))

    def append(self, table, rows):
        if rows:
            self._write(table, rows, "append")

    def replace(self, table, rows):
        # Truncate + append instead of overwrite, so the table (and its grants and comments) survives.
        if self._exists(table):
            self.session.sql(f"TRUNCATE TABLE {self._name(table)}").collect()
        self.append(table, rows)


# --- the store --------------------------------------------------------------------------------


class Store:
    """Snapshot and event history on top of any backend."""

    def __init__(self, backend: Backend | str = "drift.db", keep_days: int | None = 90):
        self.backend: Backend = SqliteBackend(backend) if isinstance(backend, str) else backend
        self.keep_days = keep_days

    def close(self) -> None:
        if hasattr(self.backend, "close"):
            self.backend.close()

    # --- snapshots ---------------------------------------------------------
    def save_snapshot(self, run_date: str, platform: str, columns: list[Column]) -> None:
        if not _DATE.match(run_date):
            raise ValueError(f"run_date must be YYYY-MM-DD, got {run_date!r}")
        self.backend.delete(SNAPSHOT, {"run_date": run_date, "platform": platform})
        self.backend.append(
            SNAPSHOT,
            [
                {
                    "run_date": run_date,
                    "platform": c.platform,
                    "layer": c.layer,
                    "schema_name": c.schema,
                    "table_name": c.table,
                    "column_name": c.name,
                    "ordinal": c.ordinal,
                    "raw_type": c.raw_type,
                    "nullable": bool(c.nullable),
                    "length": c.length,
                    "precision": c.precision,
                    "scale": c.scale,
                    "collation": c.collation,
                }
                for c in columns
            ],
        )
        if self.keep_days:
            from datetime import date, timedelta

            cutoff = (date.fromisoformat(run_date) - timedelta(days=self.keep_days)).isoformat()
            self.backend.delete_below(SNAPSHOT, "run_date", cutoff)

    def previous_snapshot(self, run_date: str) -> tuple[str | None, dict[str, list[Column]]]:
        prev = self.backend.max_below(SNAPSHOT, "run_date", run_date)
        if not prev:
            return None, {}
        out: dict[str, list[Column]] = {}
        for r in self.backend.read(SNAPSHOT, {"run_date": prev}):
            out.setdefault(r["platform"], []).append(
                Column(
                    platform=r["platform"],
                    schema=r["schema_name"],
                    table=r["table_name"],
                    name=r["column_name"],
                    ordinal=int(r["ordinal"] or 0),
                    raw_type=r["raw_type"],
                    nullable=bool(r["nullable"]),
                    length=r["length"],
                    precision=r["precision"],
                    scale=r["scale"],
                    collation=r["collation"],
                    layer=r["layer"],
                )
            )
        return prev, out

    # --- events ------------------------------------------------------------
    def _events(self) -> dict[str, Row]:
        return {r["drift_id"]: r for r in self.backend.read(EVENTS)}

    def record_events(
        self, run_date: str, events: list[DriftEvent], platforms_read: Iterable[str] | None = None
    ) -> tuple[list[DriftEvent], set[str], int]:
        """Save this run's events. Returns (events to report, ids that are new, count suppressed).

        Open events this run didn't see are marked resolved, but only if both of their platforms
        were actually read this run; a source outage must not close (and later re-alert) real drift.
        """
        stored = self._events()
        report, new_ids, suppressed = [], set(), 0
        for e in events:
            fields = {
                "comparison": e.comparison.value,
                "drift_type": e.drift_type.value,
                "table_name": e.table,
                "column_name": e.column,
                "side_a": e.side_a,
                "side_b": e.side_b,
                "value_a": e.value_a,
                "value_b": e.value_b,
                "severity": e.severity.value,
                "impacts_json": json.dumps([asdict(i) for i in e.impacts]),
                "fix_json": json.dumps(e.fix),
            }
            row = stored.get(e.drift_id)
            if row is None:
                stored[e.drift_id] = {
                    "drift_id": e.drift_id,
                    "first_seen": run_date,
                    "last_seen": run_date,
                    "status": "open",
                    "detail": e.detail,
                    "summary": e.summary,
                    **fields,
                }
                new_ids.add(e.drift_id)
                report.append(e)
            elif row["status"] == "false_positive":
                suppressed += 1
                row["last_seen"] = run_date
            else:
                if row["status"] in ("fixed", "resolved"):
                    row["status"] = "open"
                    new_ids.add(e.drift_id)
                row.update(fields, last_seen=run_date)
                report.append(e)

        read = set(platforms_read) if platforms_read is not None else None
        for row in stored.values():
            if row["status"] in OPEN and row["last_seen"] < run_date:
                sides = {row["side_a"].split(":")[0], row["side_b"].split(":")[0]} - {"config"}
                if read is None or sides <= read:
                    row["status"] = "resolved"
        self.backend.replace(EVENTS, list(stored.values()))
        return report, new_ids, suppressed

    def save_summaries(self, events: list[DriftEvent]) -> None:
        if not events:
            return
        stored = self._events()
        for e in events:
            if e.drift_id in stored:
                stored[e.drift_id].update(summary=e.summary, detail=e.detail)
        self.backend.replace(EVENTS, list(stored.values()))

    def load_summaries(self, events: list[DriftEvent]) -> None:
        stored = self._events()
        for e in events:
            row = stored.get(e.drift_id)
            if row and row.get("summary"):
                e.summary, e.detail = row["summary"], row["detail"]

    def set_status(self, drift_id: str, status: str) -> bool:
        if status not in STATUSES:
            raise ValueError(f"status must be one of {STATUSES}")
        stored = self._events()
        if drift_id not in stored:
            return False
        stored[drift_id]["status"] = status
        self.backend.replace(EVENTS, list(stored.values()))
        return True

    def list_events(self, status: str | None = None) -> list[Row]:
        rows = self.backend.read(EVENTS, {"status": status} if status else None)
        return sorted(rows, key=lambda r: (r["status"], r["table_name"] or "", r["column_name"] or ""))
