import unittest

from fakes import FakeSession, FakeSpark, install_fake_modules
from helpers import col

from snowdrift.ai import AIQueryClient, CortexClient
from snowdrift.models import DriftType
from snowdrift.notebook import check
from snowdrift.sources import (
    CsvSource,
    DatabricksSource,
    SnowflakeSource,
    TeradataExportSource,
    databricks_views,
    snowflake_views,
    snowpark_runner,
    spark_reader_runner,
    spark_runner,
    stage_reader,
)
from snowdrift.store import SNAPSHOT, MemoryBackend, SnowparkBackend, SparkBackend, Store

install_fake_modules()


class Src:
    def __init__(self, platform, cols):
        self.platform, self.cols = platform, cols

    def fetch(self, tables):
        return self.cols


TD = [col("ID", "I"), col("NOTE", "CV", ordinal=2, length=10)]
SF_MISSING = [col("ID", "NUMBER", "snowflake", precision=10, schema="PUBLIC")]
SF_OK = SF_MISSING + [col("NOTE", "TEXT", "snowflake", ordinal=2, length=10, schema="PUBLIC")]


def run(store, day, sf_cols, **kw):
    sources = {"teradata": Src("teradata", TD), "snowflake": Src("snowflake", sf_cols)}
    return check(sources=sources, reference="teradata", targets=["snowflake"], store=store, run_date=day, show=False, **kw)


class BackendLifecycle:
    """Same scenario on every backend: new -> still open -> fixed -> resolved, plus status changes."""

    def make_backend(self):
        raise NotImplementedError

    def test_lifecycle(self):
        store = Store(self.make_backend())
        r1 = run(store, "2026-01-01", SF_MISSING)
        self.assertEqual([e.drift_type for e in r1.events], [DriftType.COLUMN_MISSING])
        self.assertEqual(len(r1.new_ids), 1)
        self.assertIn('ADD COLUMN "NOTE"', r1.events[0].fix["snowflake"])

        r2 = run(store, "2026-01-02", SF_MISSING)
        self.assertEqual(r2.new_ids, set())
        self.assertEqual(r2.previous_date, "2026-01-01")

        run(store, "2026-01-03", SF_OK)
        statuses = {r["drift_id"]: r["status"] for r in store.list_events()}
        self.assertEqual(statuses[r1.events[0].drift_id], "resolved")

        self.assertTrue(store.set_status(r1.events[0].drift_id, "false_positive"))
        self.assertEqual(len(store.list_events("false_positive")), 1)

    def test_snapshot_rerun_same_day_replaces_rows(self):
        store = Store(self.make_backend())
        run(store, "2026-01-01", SF_MISSING)
        run(store, "2026-01-01", SF_MISSING)
        rows = store.backend.read(SNAPSHOT, {"run_date": "2026-01-01", "platform": "snowflake"})
        self.assertEqual(len(rows), 1)

    def test_old_snapshots_pruned(self):
        store = Store(self.make_backend(), keep_days=5)
        run(store, "2026-01-01", SF_MISSING)
        run(store, "2026-01-10", SF_MISSING)
        self.assertEqual(store.backend.read(SNAPSHOT, {"run_date": "2026-01-01"}), [])


class MemoryBackendTests(BackendLifecycle, unittest.TestCase):
    def make_backend(self):
        return MemoryBackend()


class SparkBackendTests(BackendLifecycle, unittest.TestCase):
    def make_backend(self):
        self.spark = FakeSpark()
        return SparkBackend(self.spark, "main.snowdrift")

    def test_tables_land_in_schema(self):
        store = Store(self.make_backend())
        run(store, "2026-01-01", SF_MISSING)
        self.assertIn("main.snowdrift.drift_event", self.spark.tables)
        self.assertIn("main.snowdrift.column_snapshot", self.spark.tables)

    def test_rejects_unsafe_schema(self):
        with self.assertRaises(ValueError):
            SparkBackend(FakeSpark(), "main.x; DROP TABLE y")


class SnowparkBackendTests(BackendLifecycle, unittest.TestCase):
    def make_backend(self):
        self.session = FakeSession()
        return SnowparkBackend(self.session, "OPS.SNOWDRIFT")

    def test_tables_are_uppercase(self):
        store = Store(self.make_backend())
        run(store, "2026-01-01", SF_MISSING)
        self.assertIn("OPS.SNOWDRIFT.DRIFT_EVENT", self.session.tables)
        self.assertEqual(sorted(self.session.tables["OPS.SNOWDRIFT.DRIFT_EVENT"][0])[0], "COLUMN_NAME")


class RunnerTests(unittest.TestCase):
    ROW = {"TABLE_SCHEMA": "PUBLIC", "TABLE_NAME": "ORDERS", "COLUMN_NAME": "ID", "ORDINAL_POSITION": 1,
           "DATA_TYPE": "NUMBER", "NUMERIC_PRECISION": 10, "NUMERIC_SCALE": 0, "IS_NULLABLE": "NO"}  # fmt: skip

    def test_snowpark_runner_feeds_snowflake_source(self):
        session = FakeSession()
        session.query_rows = [self.ROW]
        cols = SnowflakeSource(snowpark_runner(session), "OPS", ["PUBLIC"]).fetch(["ORDERS"])
        self.assertEqual((cols[0].name, cols[0].precision, cols[0].nullable), ("ID", 10, False))
        self.assertIn("OPS.INFORMATION_SCHEMA.COLUMNS", session.queries[0])

    def test_spark_runner_feeds_databricks_source(self):
        spark = FakeSpark()
        spark.query_rows = [{"table_schema": "sales", "table_name": "orders", "column_name": "id",
                             "ordinal_position": 0, "full_data_type": "int", "is_nullable": "NO"}]  # fmt: skip
        cols = DatabricksSource(spark_runner(spark), "main", ["sales"]).fetch(["orders"])
        self.assertEqual((cols[0].table, cols[0].raw_type), ("ORDERS", "int"))

    def test_spark_reader_runner_passes_query(self):
        spark = FakeSpark()
        spark.query_rows = [self.ROW]
        run_q = spark_reader_runner(spark, "snowflake", {"sfUrl": "x"})
        self.assertEqual(run_q("SELECT 1")[0]["column_name"], "ID")
        self.assertEqual(spark.queries[-1], "SELECT 1")

    def test_stage_csv(self):
        csv_text = "DatabaseName,TableName,ColumnName,ColumnId,ColumnType,ColumnLength,Nullable,CharType\nS,T,A,1025,CV,20,Y,2\n"
        session = FakeSession(files={"@OPS.EXPORTS/td.csv": csv_text})
        cols = TeradataExportSource("teradata", "@OPS.EXPORTS/td.csv", read_text=stage_reader(session)).fetch([])
        self.assertEqual((cols[0].name, cols[0].length), ("A", 10))

    def test_csv_source_default_reads_files(self):
        import tempfile
        from pathlib import Path

        with tempfile.TemporaryDirectory() as tmp:
            p = Path(tmp) / "x.csv"
            p.write_text("schema,table,column,ordinal,raw_type\nS,T,A,1,string\n")
            self.assertEqual(CsvSource("databricks", p).fetch([])[0].name, "A")

    def test_view_loaders(self):
        session = FakeSession()
        session.query_rows = [{"TABLE_SCHEMA": "RPT", "TABLE_NAME": "V1", "VIEW_DEFINITION": "select a from t"}]
        self.assertEqual(snowflake_views(snowpark_runner(session), "OPS", ["RPT"]), {"RPT.V1": "select a from t"})
        spark = FakeSpark()
        spark.query_rows = [{"table_schema": "rpt", "table_name": "v1", "view_definition": None}]
        self.assertEqual(databricks_views(spark_runner(spark), "main", ["rpt"]), {"RPT.V1": ""})


class AIClientTests(unittest.TestCase):
    def test_ai_query_binds_prompt(self):
        spark = FakeSpark()
        out = AIQueryClient(spark, "my-endpoint").complete("hello world")
        self.assertEqual(out, "summary for: hello world")
        self.assertIn("ai_query('my-endpoint', :prompt)", spark.queries[0])

    def test_cortex_binds_prompt(self):
        session = FakeSession()
        self.assertEqual(CortexClient(session, "mistral-large2").complete("hello"), "cortex says: hello")
        self.assertIn("COMPLETE('mistral-large2', ?)", session.queries[0])

    def test_model_names_validated(self):
        with self.assertRaises(ValueError):
            CortexClient(FakeSession(), "x'); DROP TABLE y; --")

    def test_check_with_cortex_and_rows(self):
        session = FakeSession()
        report = run(Store(MemoryBackend()), "2026-01-01", SF_MISSING, llm=CortexClient(session, "m1"))
        self.assertTrue(report.events[0].summary.startswith("cortex says"))
        rows = report.to_rows()
        self.assertEqual(rows[0]["severity"], "breaking")
        self.assertTrue(rows[0]["new"])
        self.assertIn("ADD COLUMN", rows[0]["fix_sql"])


if __name__ == "__main__":
    unittest.main()


class ReviewFixTests(unittest.TestCase):
    def test_gzipped_stage_file(self):
        import gzip

        text = "schema,table,column,ordinal,raw_type\nS,T,A,1,string\n"
        session = FakeSession(files={"@S.E/db.csv.gz": gzip.compress(text.encode())})
        cols = CsvSource("databricks", "@S.E/db.csv", read_text=stage_reader(session)).fetch([])
        self.assertEqual(cols[0].name, "A")

    def test_rows_have_no_nulls(self):
        report = run(Store(MemoryBackend()), "2026-01-01", SF_MISSING)
        for row in report.to_rows():
            self.assertTrue(all(v is not None for v in row.values()))
        self.assertEqual(report.to_rows()[0]["value_b"], "")

    def test_null_model_answer_falls_back_to_template(self):
        session = FakeSession()
        session.sql = lambda q, params=None: type("D", (), {"collect": lambda self: [[None]]})()
        self.assertEqual(CortexClient(session, "m").complete("x"), "")
        report = run(Store(MemoryBackend()), "2026-01-01", SF_MISSING, llm=CortexClient(session, "m"))
        self.assertIn("ORDERS.NOTE", report.events[0].summary)

    def test_snowflake_replace_keeps_table(self):
        session = FakeSession()
        store = Store(SnowparkBackend(session, "OPS.SNOWDRIFT"))
        run(store, "2026-01-01", SF_MISSING)
        run(store, "2026-01-02", SF_OK)
        self.assertTrue(any(q.startswith("TRUNCATE TABLE OPS.SNOWDRIFT.DRIFT_EVENT") for q in session.queries))
        ids = [r["DRIFT_ID"] for r in session.tables["OPS.SNOWDRIFT.DRIFT_EVENT"]]
        self.assertEqual(len(ids), len(set(ids)))  # rewritten in place, no duplicate rows
        self.assertEqual(len(ids), 2)  # day 1's missing column (now resolved) + day 2's column_added
