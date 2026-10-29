import unittest

from helpers import col

from snowdrift.ddl import draft_fix, render_type
from snowdrift.impact import ImpactSettings, assess
from snowdrift.models import Comparison, Severity
from snowdrift.rules import compare_table
from snowdrift.types import NormType

X = Comparison.CROSS_PLATFORM
MERGE = "MERGE INTO ORDERS t USING STAGE.ORDERS s ON t.ID = s.ID WHEN MATCHED THEN UPDATE SET AMT = s.AMT"
VIEW = "CREATE VIEW V AS SELECT ID, AMT FROM ORDERS"


def one(a, b, kind=X, side_b="snowflake:final"):
    events = compare_table("ORDERS", a, b, kind, "teradata", side_b)
    assert len(events) == 1, events
    return events[0]


class ImpactTests(unittest.TestCase):
    def test_key_column_type_change_breaks(self):
        e = one([col("ID", "I")], [col("ID", "TEXT", "snowflake", length=10)])
        assess(e, ImpactSettings(key_columns={"ORDERS": ["ID"]}))
        self.assertTrue(any(i.check == "key_columns" and i.breaks for i in e.impacts))

    def test_key_column_widening_is_not_an_impact(self):
        e = one([col("ID", "I")], [col("ID", "NUMBER", "snowflake", precision=38, scale=0)])
        assess(e, ImpactSettings(key_columns={"ORDERS": ["ID"]}))
        self.assertEqual(e.impacts, [])
        self.assertEqual(e.severity, Severity.INFO)

    def test_stored_merge_breaks_on_missing_column(self):
        e = one([col("ID", "I"), col("NEW", "CV", ordinal=2, length=5)], [col("ID", "NUMBER", "snowflake", precision=10)])
        assess(e, ImpactSettings(stored_sql={"MERGE_ORDERS": MERGE}, views={"V": VIEW}))
        by_check = {i.check: i for i in e.impacts}
        self.assertTrue(by_check["stored_sql"].breaks)  # explicit column list misses NEW
        self.assertFalse(by_check["views"].breaks)  # a view that doesn't use NEW still works

    def test_view_using_column_breaks_on_drop(self):
        a = [col("ID", "I"), col("AMT", "D", ordinal=2, precision=10, scale=2)]
        e = one(a, [col("ID", "NUMBER", "snowflake", precision=10)])
        assess(e, ImpactSettings(views={"V": VIEW}))
        self.assertTrue(e.impacts[0].breaks)
        self.assertEqual(e.severity, Severity.BREAKING)

    def test_column_name_must_match_whole_word(self):
        a = [col("ID", "I"), col("AM", "I", ordinal=2)]
        e = one(a, [col("ID", "NUMBER", "snowflake", precision=10)])
        assess(e, ImpactSettings(views={"V": VIEW}))  # view has AMT, not AM
        self.assertFalse(e.impacts[0].breaks)

    def test_day_over_day_is_not_escalated(self):
        e = one([col("ID", "I")], [col("ID", "I"), col("NEW", "CV", ordinal=2)], Comparison.DAY_OVER_DAY, "td:cur")
        assess(e, ImpactSettings(stored_sql={"M": MERGE}))
        self.assertEqual(e.severity, Severity.WARNING)


class DdlTests(unittest.TestCase):
    dialects = {"snowflake": "snowflake", "databricks": "databricks"}

    def test_add_missing_column(self):
        a = [col("ID", "I"), col("NOTE", "CV", ordinal=2, length=40)]
        sf = one(a, [col("ID", "NUMBER", "snowflake", precision=10, schema="PUBLIC")])
        db = one(a, [col("ID", "int", "databricks", schema="SALES")], side_b="databricks:final")
        self.assertIn('ALTER TABLE "PUBLIC"."ORDERS" ADD COLUMN "NOTE" VARCHAR(40);', draft_fix(sf, self.dialects))
        self.assertIn("ALTER TABLE `SALES`.`ORDERS` ADD COLUMNS (`NOTE` STRING);", draft_fix(db, self.dialects))

    def test_every_draft_has_the_review_header(self):
        e = one([col("D", "DA")], [col("D", "TEXT", "snowflake")])
        sql = draft_fix(e, self.dialects)
        self.assertTrue(sql.startswith("-- DRAFT"))
        self.assertIn("No safe in-place change", sql)

    def test_widen_varchar_in_snowflake(self):
        e = one([col("N", "CV", length=50)], [col("N", "TEXT", "snowflake", length=20, schema="P")])
        self.assertIn('ALTER TABLE "P"."ORDERS" ALTER COLUMN "N" SET DATA TYPE VARCHAR(50);', draft_fix(e, self.dialects))

    def test_no_fix_for_day_over_day_or_unknown_dialect(self):
        e = one([col("ID", "I")], [col("ID", "I"), col("NEW", "CV", ordinal=2)], Comparison.DAY_OVER_DAY, "td:cur")
        self.assertIsNone(draft_fix(e, self.dialects))

    def test_render_types(self):
        self.assertEqual(render_type(NormType("INT", precision=19), "databricks"), "BIGINT")
        self.assertEqual(render_type(NormType("INT", precision=38), "databricks"), "DECIMAL(38,0)")
        self.assertEqual(render_type(NormType("INT", precision=10), "databricks"), "INT")
        self.assertEqual(render_type(NormType("DECIMAL", precision=12, scale=2), "snowflake"), "NUMBER(12,2)")
        self.assertEqual(render_type(NormType("TIMESTAMP"), "snowflake"), "TIMESTAMP_NTZ")


if __name__ == "__main__":
    unittest.main()
