"""Regression tests for issues found in code review."""

import tempfile
import unittest
from pathlib import Path

from helpers import col

from snowdrift import agent
from snowdrift.ddl import draft_fix
from snowdrift.detector import DetectSettings, detect
from snowdrift.models import Comparison, DriftType, Severity
from snowdrift.rules import compare_table
from snowdrift.sources import _bool
from snowdrift.store import Store

X = Comparison.CROSS_PLATFORM
DIALECTS = {"snowflake": "snowflake", "databricks": "databricks"}


class Src:
    def __init__(self, platform, cols=None, fail=False):
        self.platform, self.cols, self.fail = platform, cols or [], fail

    def fetch(self, tables):
        if self.fail:
            raise ConnectionError("timeout")
        return self.cols


class ReviewFixTests(unittest.TestCase):
    def test_failed_target_is_not_reported_as_missing_tables(self):
        cur = {"teradata": [col("ID", "I")]}
        self.assertEqual(detect(cur, None, DetectSettings("teradata", ["snowflake"])), [])

    def test_outage_does_not_resolve_then_realert(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = Store(str(Path(tmp) / "s.db"))
            td = [col("ID", "I"), col("NOTE", "CV", ordinal=2, length=5)]
            sf = [col("ID", "NUMBER", "snowflake", precision=10)]
            s = DetectSettings("teradata", ["snowflake"])
            r1 = agent.run("2026-01-01", {"teradata": Src("teradata", td), "snowflake": Src("snowflake", sf)}, s, store)
            agent.run("2026-01-02", {"teradata": Src("teradata", fail=True), "snowflake": Src("snowflake", sf)}, s, store)
            r3 = agent.run("2026-01-03", {"teradata": Src("teradata", td), "snowflake": Src("snowflake", sf)}, s, store)
            store.close()
        self.assertEqual(len(r1.new_ids), 1)
        self.assertEqual(r3.new_ids, set())  # still the same open event, not re-alerted

    def test_fewer_whole_digits_is_shrunk(self):
        e = compare_table(
            "T", [col("A", "D", precision=10, scale=2)], [col("A", "NUMBER", "sf", precision=10, scale=4)], X, "td", "sf"
        )
        self.assertEqual([x.drift_type for x in e], [DriftType.PRECISION_SHRUNK])

    def test_layer_nullability_direction(self):
        final_not_null = [col("A", "TEXT", "sf", nullable=False)]
        work_nullable = [col("A", "TEXT", "sf", nullable=True, layer="work")]
        e = compare_table("T", final_not_null, work_nullable, Comparison.LAYER, "sf:final", "sf:work")
        self.assertEqual(e[0].severity, Severity.BREAKING)
        e = compare_table("T", work_nullable, final_not_null, Comparison.LAYER, "sf:final", "sf:work")
        self.assertEqual(e[0].severity, Severity.WARNING)

    def test_databricks_fix_never_narrows(self):
        e = compare_table(
            "T", [col("A", "D", precision=10, scale=4)], [col("A", "decimal(12,2)", "databricks")], X, "td", "databricks:final"
        )[0]
        sql = draft_fix(e, DIALECTS)
        self.assertNotIn("ALTER COLUMN", sql.replace("-- ", "#"))
        self.assertIn("No safe in-place change", sql)

    def test_duplicate_table_names_warn_instead_of_mixing(self):
        cur = {
            "teradata": [col("ID", "I")],
            "snowflake": [
                col("ID", "NUMBER", "snowflake", precision=10, schema="ARCHIVE"),
                col("ID", "TEXT", "snowflake", schema="PUBLIC"),
            ],
        }
        warnings = []
        events = detect(cur, None, DetectSettings("teradata", ["snowflake"]), warnings)
        self.assertEqual(events, [])  # compared ARCHIVE only
        self.assertTrue(any("more than one" in w for w in warnings))

    def test_reserved_words_quoted_and_unknown_types_not_drafted(self):
        e = compare_table("T", [col("ID", "I"), col("ORDER", "JN", ordinal=2)], [col("ID", "int", "databricks")], X, "td",
                          "databricks:final")[0]  # fmt: skip
        sql = draft_fix(e, DIALECTS)
        self.assertNotIn("UNKNOWN", sql.split("\n", 3)[-1])
        self.assertIn("no mapping yet", sql)

    def test_rename_replaces_missing_and_extra(self):
        a = [col("ID", "I"), col("PHONE", "CV", ordinal=2, length=20)]
        b = [col("ID", "int", "db"), col("PHONE_NUMBER", "string", "db", ordinal=2)]
        kinds = [e.drift_type for e in compare_table("T", a, b, X, "td", "db")]
        self.assertEqual(kinds, [DriftType.POSSIBLE_RENAME])

    def test_unrelated_lone_columns_are_not_a_rename(self):
        a = [col("ID", "I"), col("PHONE", "CV", ordinal=2)]
        b = [col("ID", "int", "db"), col("XYZ", "string", "db", ordinal=2)]
        kinds = sorted(e.drift_type for e in compare_table("T", a, b, X, "td", "db"))
        self.assertNotIn(DriftType.POSSIBLE_RENAME, kinds)

    def test_integer_into_wide_decimal_is_fine(self):
        e = compare_table("T", [col("A", "I")], [col("A", "NUMBER", "sf", precision=20, scale=2)], X, "td", "sf")
        self.assertTrue(all(x.severity is Severity.INFO for x in e))

    def test_missing_nullable_value_means_nullable(self):
        self.assertTrue(_bool(None))
        self.assertTrue(_bool(""))
        self.assertFalse(_bool("N"))


if __name__ == "__main__":
    unittest.main()
