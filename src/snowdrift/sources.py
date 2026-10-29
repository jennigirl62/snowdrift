"""Metadata sources: where column definitions come from.

Every source returns a list of Column. SQL sources take a `run_query` callable,
so they work with any connection you already have:

    dbapi_runner(conn)   # snowflake-connector-python, databricks-sql-connector, teradatasql, ...
    spark_runner(spark)  # inside a Databricks notebook or job

CSV sources cover platforms you can't query directly: export the catalog query
on a schedule and point the source at the file.
"""

from __future__ import annotations

import csv
import gzip
import io
import re
from collections.abc import Callable, Iterable
from pathlib import Path
from typing import Protocol

from .models import Column

type Rows = list[dict]
type Runner = Callable[[str], Rows]
type TextReader = Callable[[str], str]
_SAFE = re.compile(r"^[A-Za-z0-9_$]+$")


class MetadataSource(Protocol):
    platform: str

    def fetch(self, tables: list[str]) -> list[Column]: ...


def dbapi_runner(conn) -> Runner:
    """Wrap a DB-API connection, or a zero-argument function that opens one on first use."""
    state = {"conn": None if callable(conn) and not hasattr(conn, "cursor") else conn}

    def run(sql: str) -> Rows:
        if state["conn"] is None:
            state["conn"] = conn()
        cur = state["conn"].cursor()
        try:
            cur.execute(sql)
            names = [d[0].lower() for d in cur.description]
            return [dict(zip(names, row, strict=False)) for row in cur.fetchall()]
        finally:
            cur.close()

    return run


def spark_runner(spark) -> Runner:
    """Run SQL with the notebook's Spark session (Databricks)."""

    def run(sql: str) -> Rows:
        return [{k.lower(): v for k, v in r.asDict().items()} for r in spark.sql(sql).collect()]

    return run


def snowpark_runner(session) -> Runner:
    """Run SQL with a Snowpark session (Snowflake notebooks: get_active_session())."""

    def run(sql: str) -> Rows:
        return [{k.lower(): v for k, v in r.as_dict().items()} for r in session.sql(sql).collect()]

    return run


def spark_reader_runner(spark, fmt: str, options: dict[str, str]) -> Runner:
    """Run a query on another system through a Spark data source, from a Databricks notebook.

    Teradata over JDBC:  spark_reader_runner(spark, "jdbc", {"url": "jdbc:teradata://host/...", "user": ..., "password": ...,
                                                             "driver": "com.teradata.jdbc.TeraDriver"})
    Snowflake:           spark_reader_runner(spark, "snowflake", {"sfUrl": ..., "sfUser": ..., "sfPassword": ...,
                                                                  "sfDatabase": ..., "sfWarehouse": ...})
    Pull secrets from dbutils.secrets.get(...), never paste them into the notebook.
    """

    def run(sql: str) -> Rows:
        df = spark.read.format(fmt).options(**options).option("query", sql).load()
        return [{k.lower(): v for k, v in r.asDict().items()} for r in df.collect()]

    return run


def stage_reader(session) -> TextReader:
    """Read a text file from an internal Snowflake stage, e.g. '@MY_DB.MY_SCHEMA.EXPORTS/teradata_columns.csv'.

    PUT gzips uploads by default (file.csv -> file.csv.gz); both forms are handled.
    """

    def read(path: str) -> str:
        try:
            with session.file.get_stream(path) as fh:
                data = fh.read()
        except Exception:
            if path.endswith(".gz"):
                raise
            with session.file.get_stream(path + ".gz") as fh:
                data = fh.read()
        if data[:2] == b"\x1f\x8b":  # gzip magic number
            data = gzip.decompress(data)
        return data.decode("utf-8-sig")

    return read


def local_reader(path: str) -> str:
    """Read a local or mounted file, e.g. a Databricks Volume path '/Volumes/main/snowdrift/exports/x.csv'."""
    return Path(path).read_text(encoding="utf-8-sig")


def _in_list(names: Iterable[str], upper: bool = True) -> str:
    vals = []
    for n in names:
        if not _SAFE.match(n):
            raise ValueError(f"unsafe identifier: {n!r}")
        vals.append(f"'{n.upper() if upper else n.lower()}'")
    return ", ".join(vals)


def _int(v) -> int | None:
    if v is None or v == "":
        return None
    try:
        i = int(float(v))
    except (TypeError, ValueError):
        return None
    return i if i >= 0 else None


def _bool(v) -> bool:
    """Nullable flag. Missing values count as nullable, so absent data can't flood the run with NOT NULL drift."""
    if v is None or str(v).strip() == "":
        return True
    return str(v).strip().upper() in ("Y", "YES", "TRUE", "1")


def _get(row: dict, *names: str):
    low = {k.lower(): v for k, v in row.items()}
    for n in names:
        if n.lower() in low:
            return low[n.lower()]
    return None


class _Base:
    def __init__(self, platform: str, work_schemas: Iterable[str] = ()):
        self.platform = platform
        self.work_schemas = {s.upper() for s in work_schemas}

    def _layer(self, schema: str) -> str:
        return "work" if schema.upper() in self.work_schemas else "final"

    def _keep(self, cols: list[Column], tables: list[str]) -> list[Column]:
        if not tables:
            return cols
        wanted = {t.upper() for t in tables}
        return [c for c in cols if c.table.upper() in wanted]


class GenericCatalogMixin:
    """Map rows shaped like INFORMATION_SCHEMA.COLUMNS to Column."""

    def _row_to_column(self, r: dict) -> Column:
        schema = str(_get(r, "schema", "schema_name", "table_schema")).strip().upper()
        return Column(
            platform=self.platform,
            schema=schema,
            table=str(_get(r, "table", "table_name")).strip().upper(),
            name=str(_get(r, "column", "column_name")).strip().upper(),
            ordinal=_int(_get(r, "ordinal", "ordinal_position")) or 0,
            raw_type=str(_get(r, "raw_type", "full_data_type", "data_type")).strip(),
            nullable=_bool(_get(r, "nullable", "is_nullable")),
            length=_int(_get(r, "length", "char_length", "character_maximum_length")),
            precision=_int(_get(r, "precision", "num_precision", "numeric_precision")),
            scale=_int(_get(r, "scale", "num_scale", "numeric_scale")),
            collation=(_get(r, "collation", "collation_name") or None),
            layer=self._layer(schema),
        )


class CsvSource(_Base, GenericCatalogMixin):
    """CSV with columns like INFORMATION_SCHEMA.COLUMNS (schema, table, column, ordinal, raw_type, nullable, ...)."""

    def __init__(self, platform: str, path: str | Path, work_schemas: Iterable[str] = (), read_text: TextReader = local_reader):
        super().__init__(platform, work_schemas)
        self.path = str(path)
        self.read_text = read_text

    def fetch(self, tables: list[str]) -> list[Column]:
        rows = csv.DictReader(io.StringIO(self.read_text(self.path)))
        return self._keep([self._row_to_column(r) for r in rows], tables)


class TeradataExportSource(CsvSource):
    """CSV export of DBC.ColumnsV. Accepts raw column names or the aliases in sql/teradata_columns.sql."""

    def _row_to_column(self, r: dict) -> Column:
        schema = str(_get(r, "DatabaseName", "schema_name")).strip().upper()
        code = str(_get(r, "ColumnType", "type_code") or "").strip().upper()
        length = _int(_get(r, "ColumnLength", "col_length"))
        # ColumnLength is in bytes; Unicode (CharType 2) uses 2 bytes per character.
        if code in ("CV", "CF") and length and _int(_get(r, "CharType", "char_type")) == 2:
            length //= 2
        is_decimal = code in ("D", "N")
        return Column(
            platform=self.platform,
            schema=schema,
            table=str(_get(r, "TableName", "table_name")).strip().upper(),
            name=str(_get(r, "ColumnName", "column_name")).strip().upper(),
            ordinal=_int(_get(r, "ColumnId", "ordinal")) or 0,
            raw_type=code,
            nullable=_bool(_get(r, "Nullable", "is_nullable")),
            length=length if code in ("CV", "CF", "BF", "BV") else None,
            precision=_int(_get(r, "DecimalTotalDigits", "num_precision")) if is_decimal else None,
            scale=_int(_get(r, "DecimalFractionalDigits", "num_scale")) if is_decimal else None,
            layer=self._layer(schema),
        )


class SnowflakeSource(_Base, GenericCatalogMixin):
    def __init__(
        self,
        run_query: Runner,
        database: str,
        schemas: list[str],
        work_schemas: Iterable[str] = (),
        platform: str = "snowflake",
    ):
        super().__init__(platform, work_schemas)
        if not _SAFE.match(database):
            raise ValueError(f"unsafe identifier: {database!r}")
        self.run_query, self.database, self.schemas = run_query, database, schemas

    def sql(self, tables: list[str]) -> str:
        where = f"table_schema IN ({_in_list(self.schemas)})"
        if tables:
            where += f" AND table_name IN ({_in_list(tables)})"
        return (
            "SELECT table_schema, table_name, column_name, ordinal_position, data_type, character_maximum_length, "
            f"numeric_precision, numeric_scale, is_nullable, collation_name FROM {self.database}.INFORMATION_SCHEMA.COLUMNS "
            f"WHERE {where}"
        )

    def fetch(self, tables: list[str]) -> list[Column]:
        return [self._row_to_column(r) for r in self.run_query(self.sql(tables))]


class DatabricksSource(_Base, GenericCatalogMixin):
    def __init__(
        self,
        run_query: Runner,
        catalog: str,
        schemas: list[str],
        work_schemas: Iterable[str] = (),
        platform: str = "databricks",
    ):
        super().__init__(platform, work_schemas)
        if not _SAFE.match(catalog):
            raise ValueError(f"unsafe identifier: {catalog!r}")
        self.run_query, self.catalog, self.schemas = run_query, catalog, schemas

    def sql(self, tables: list[str]) -> str:
        where = f"lower(table_schema) IN ({_in_list(self.schemas, upper=False)})"
        if tables:
            where += f" AND lower(table_name) IN ({_in_list(tables, upper=False)})"
        return (
            "SELECT table_schema, table_name, column_name, ordinal_position, full_data_type, character_maximum_length, "
            f"numeric_precision, numeric_scale, is_nullable FROM {self.catalog}.information_schema.columns WHERE {where}"
        )

    def fetch(self, tables: list[str]) -> list[Column]:
        # Databricks reports integer precision in bits; types.normalize ignores it for integers.
        return [self._row_to_column(r) for r in self.run_query(self.sql(tables))]


class TeradataSource(_Base):
    """Direct Teradata connection (e.g. teradatasql). Use TeradataExportSource if you can't connect."""

    def __init__(self, run_query: Runner, databases: list[str], work_schemas: Iterable[str] = (), platform: str = "teradata"):
        super().__init__(platform, work_schemas)
        self.run_query, self.databases = run_query, databases
        self._mapper = TeradataExportSource(platform, "-", work_schemas)

    def sql(self, tables: list[str]) -> str:
        where = f"DatabaseName IN ({_in_list(self.databases)})"
        if tables:
            where += f" AND TableName IN ({_in_list(tables)})"
        return (
            "SELECT DatabaseName, TableName, ColumnName, ColumnId, ColumnType, ColumnLength, "
            "DecimalTotalDigits, DecimalFractionalDigits, Nullable, CharType FROM DBC.ColumnsV "
            f"WHERE {where}"
        )

    def fetch(self, tables: list[str]) -> list[Column]:
        return [self._mapper._row_to_column(r) for r in self.run_query(self.sql(tables))]


def snowflake_views(run_query: Runner, database: str, schemas: list[str]) -> dict[str, str]:
    """View definitions from Snowflake, for the impact check (which views name a changed column)."""
    if not _SAFE.match(database):
        raise ValueError(f"unsafe identifier: {database!r}")
    sql = (
        f"SELECT table_schema, table_name, view_definition FROM {database}.INFORMATION_SCHEMA.VIEWS "
        f"WHERE table_schema IN ({_in_list(schemas)})"
    )
    return {f"{r['table_schema']}.{r['table_name']}".upper(): r["view_definition"] or "" for r in run_query(sql)}


def databricks_views(run_query: Runner, catalog: str, schemas: list[str]) -> dict[str, str]:
    """View definitions from Unity Catalog, for the impact check."""
    if not _SAFE.match(catalog):
        raise ValueError(f"unsafe identifier: {catalog!r}")
    sql = (
        f"SELECT table_schema, table_name, view_definition FROM {catalog}.information_schema.views "
        f"WHERE lower(table_schema) IN ({_in_list(schemas, upper=False)})"
    )
    return {f"{r['table_schema']}.{r['table_name']}".upper(): r["view_definition"] or "" for r in run_query(sql)}
