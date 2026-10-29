import unittest

from helpers import col

from snowdrift.detector import DetectSettings, detect
from snowdrift.models import Comparison, DriftType, Severity
from snowdrift.rules import compare_table

X = Comparison.CROSS_PLATFORM


def types(events):
    return sorted(e.drift_type for e in events)


class CompareTableTests(unittest.TestCase):
    def test_identical_tables_have_no_drift(self):
        a = [col("ID", "I", ordinal=1), col("NAME", "CV", ordinal=2, length=20)]
        b = [col("ID", "NUMBER", "snowflake", ordinal=1, precision=10, scale=0),
             col("NAME", "TEXT", "snowflake", ordinal=2, length=20)]  # fmt: skip
        self.assertEqual(compare_table("ORDERS", a, b, X, "teradata", "snowflake:final"), [])

    def test_missing_column_is_breaking(self):
        a = [col("ID", "I"), col("NOTE", "CV", ordinal=2, length=10)]
        b = [col("ID", "int", "databricks")]
        events = compare_table("ORDERS", a, b, X, "teradata", "databricks:final")
        self.assertEqual(types(events), [DriftType.COLUMN_MISSING])
        self.assertEqual(events[0].severity, Severity.BREAKING)
        self.assertEqual(events[0].column, "NOTE")

    def test_ignored_audit_columns(self):
        a = [col("ID", "I")]
        b = [col("ID", "int", "databricks"), col("LOAD_TS", "timestamp", "databricks", ordinal=2)]
        self.assertEqual(compare_table("ORDERS", a, b, X, "td", "db", ignore_columns=["load_ts"]), [])

    def test_length_shrunk_vs_grew(self):
        a = [col("NAME", "CV", length=50)]
        shorter = compare_table("ORDERS", a, [col("NAME", "TEXT", "sf", length=30)], X, "td", "sf")
        longer = compare_table("ORDERS", a, [col("NAME", "TEXT", "sf", length=80)], X, "td", "sf")
        self.assertEqual(types(shorter), [DriftType.LENGTH_SHRUNK])
        self.assertEqual(shorter[0].severity, Severity.BREAKING)
        self.assertEqual(longer[0].severity, Severity.INFO)

    def test_string_without_length_is_not_drift(self):
        a = [col("NAME", "CV", length=50)]
        self.assertEqual(compare_table("ORDERS", a, [col("NAME", "string", "db")], X, "td", "db"), [])

    def test_type_change(self):
        events = compare_table("ORDERS", [col("D", "DA")], [col("D", "string", "db")], X, "td", "db")
        self.assertEqual(types(events), [DriftType.TYPE_CHANGED])

    def test_precision_shrunk(self):
        a = [col("AMT", "D", precision=12, scale=2)]
        b = [col("AMT", "decimal(10,2)", "db")]
        self.assertEqual(types(compare_table("ORDERS", a, b, X, "td", "db")), [DriftType.PRECISION_SHRUNK])

    def test_not_null_target_is_breaking(self):
        a = [col("S", "CV", length=2, nullable=True)]
        b = [col("S", "string", "db", nullable=False)]
        events = compare_table("ORDERS", a, b, X, "td", "db")
        self.assertEqual(types(events), [DriftType.NULLABILITY_CHANGED])
        self.assertEqual(events[0].severity, Severity.BREAKING)

    def test_rename_detected_even_when_one_side_has_no_length(self):
        a = [col("ID", "I"), col("PHONE", "CV", ordinal=2, length=20)]
        b = [col("ID", "int", "db"), col("PHONE_NUMBER", "string", "db", ordinal=2)]
        events = compare_table("ORDERS", a, b, X, "td", "db")
        rename = [e for e in events if e.drift_type is DriftType.POSSIBLE_RENAME]
        self.assertEqual(len(rename), 1)
        self.assertEqual((rename[0].value_a, rename[0].value_b), ("PHONE", "PHONE_NUMBER"))

    def test_order_change(self):
        a = [col("A", "I", ordinal=1), col("B", "I", ordinal=2)]
        b = [col("A", "int", "db", ordinal=2), col("B", "int", "db", ordinal=1)]
        self.assertEqual(types(compare_table("T", a, b, X, "td", "db")), [DriftType.ORDER_CHANGED])

    def test_table_missing(self):
        events = compare_table("ORDERS", [col("ID", "I")], [], X, "td", "db")
        self.assertEqual(types(events), [DriftType.TABLE_MISSING])

    def test_day_over_day_uses_added_and_dropped(self):
        prev = [col("ID", "I")]
        cur = [col("ID", "I"), col("NEW", "CV", ordinal=2, length=5)]
        events = compare_table("ORDERS", prev, cur, Comparison.DAY_OVER_DAY, "td:prev", "td:cur")
        self.assertEqual(types(events), [DriftType.COLUMN_ADDED])
        self.assertEqual(events[0].severity, Severity.WARNING)

    def test_drift_id_is_stable(self):
        a, b = [col("ID", "I"), col("X", "I", ordinal=2)], [col("ID", "int", "db")]
        one = compare_table("ORDERS", a, b, X, "td", "db")[0].drift_id
        two = compare_table("ORDERS", a, b, X, "td", "db")[0].drift_id
        self.assertEqual(one, two)


class DetectTests(unittest.TestCase):
    def test_layer_and_collation_and_filtering(self):
        current = {
            "teradata": [col("ID", "I")],
            "snowflake": [
                col("ID", "NUMBER", "snowflake", precision=10, scale=0, schema="PUBLIC"),
                col("ID", "NUMBER", "snowflake", precision=10, scale=0, schema="STAGE", layer="work"),
                col("EXTRA", "TEXT", "snowflake", ordinal=2, schema="STAGE", layer="work", length=5, collation="en-cs"),
            ],
        }
        s = DetectSettings(reference="teradata", targets=["snowflake"], tables=["ORDERS"],
                           expected_collation={"snowflake": "en-ci"})  # fmt: skip
        events = detect(current, None, s)
        kinds = sorted((e.comparison, e.drift_type) for e in events)
        self.assertIn((Comparison.LAYER, DriftType.COLUMN_EXTRA), kinds)
        self.assertIn((Comparison.CONFIG, DriftType.COLLATION_MISMATCH), kinds)

        s.tables = ["OTHER"]
        self.assertEqual(detect(current, None, s), [])


if __name__ == "__main__":
    unittest.main()
