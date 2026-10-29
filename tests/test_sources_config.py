import tempfile
import textwrap
import unittest
from pathlib import Path

from snowdrift import config
from snowdrift.sources import DatabricksSource, SnowflakeSource, TeradataExportSource, TeradataSource


class SourceTests(unittest.TestCase):
    def test_teradata_export_halves_unicode_length(self):
        with tempfile.TemporaryDirectory() as tmp:
            p = Path(tmp) / "td.csv"
            p.write_text(
                "DatabaseName,TableName,ColumnName,ColumnId,ColumnType,ColumnLength,DecimalTotalDigits,"
                "DecimalFractionalDigits,Nullable,CharType\n"
                "SALES ,ORDERS ,NAME ,1026,CV,100,,,Y,2\n"
                "SALES,ORDERS,AMT,1027,D,8,12,2,N,0\n"
                "SALES,ORDERS,CODE,1028,CF,3,-1,-1,Y,1\n"
            )
            cols = {c.name: c for c in TeradataExportSource("teradata", p).fetch(["orders"])}
        self.assertEqual(cols["NAME"].length, 50)
        self.assertEqual(cols["NAME"].schema, "SALES")
        self.assertEqual((cols["AMT"].precision, cols["AMT"].scale, cols["AMT"].nullable), (12, 2, False))
        self.assertEqual((cols["CODE"].length, cols["CODE"].precision), (3, None))

    def test_sql_sources_map_rows(self):
        def run(sql):
            self.assertIn("ORDERS", sql.upper())
            return [{"TABLE_SCHEMA": "STAGE", "TABLE_NAME": "ORDERS", "COLUMN_NAME": "ID", "ORDINAL_POSITION": 1,
                     "DATA_TYPE": "NUMBER", "NUMERIC_PRECISION": 38, "NUMERIC_SCALE": 0, "IS_NULLABLE": "NO"}]  # fmt: skip

        cols = SnowflakeSource(run, "WH", ["PUBLIC", "STAGE"], work_schemas=["STAGE"]).fetch(["ORDERS"])
        self.assertEqual((cols[0].layer, cols[0].nullable, cols[0].precision), ("work", False, 38))

    def test_queries_reject_injection(self):
        src = DatabricksSource(lambda q: [], "main", ["sales"])
        with self.assertRaises(ValueError):
            src.sql(["orders'; DROP TABLE x; --"])
        with self.assertRaises(ValueError):
            SnowflakeSource(lambda q: [], "WH; DROP", ["PUBLIC"])
        self.assertIn("DBC.ColumnsV", TeradataSource(lambda q: [], ["SALES"]).sql(["ORDERS"]))


class ConfigTests(unittest.TestCase):
    def test_load_example_style_config(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            (base / "views").mkdir()
            (base / "views" / "v_orders.sql").write_text("SELECT ID FROM ORDERS")
            (base / "td.csv").write_text("DatabaseName,TableName,ColumnName,ColumnId,ColumnType\n")
            (base / "c.toml").write_text(
                textwrap.dedent("""
                [run]
                reference = "teradata"
                targets = ["snowflake"]
                tables = ["orders"]
                min_severity = "breaking"
                store = "x.db"

                [sources.teradata]
                type = "teradata_export"
                path = "td.csv"

                [sources.snowflake]
                type = "csv"
                path = "sf.csv"
                dialect = "snowflake"

                [collation]
                snowflake = "en-ci"

                [impact]
                full_refresh = ["orders"]
                views_dir = "views"
                [impact.key_columns]
                orders = ["ID"]

                [alerts]
                console = false
                [alerts.teams]
                webhook_env = "HOOK"
            """)
            )
            cfg = config.load(base / "c.toml")
        self.assertEqual(cfg.detect.tables, ["ORDERS"])
        self.assertEqual(cfg.detect.min_severity.value, "breaking")
        self.assertEqual(cfg.impact.key_columns, {"ORDERS": ["ID"]})
        self.assertIn("V_ORDERS", cfg.impact.views)
        self.assertEqual(cfg.dialects["snowflake"], "snowflake")
        self.assertEqual([type(n).__name__ for n in cfg.notifiers], ["TeamsWebhookNotifier"])
        self.assertTrue(cfg.store_path.endswith("x.db"))


if __name__ == "__main__":
    unittest.main()
