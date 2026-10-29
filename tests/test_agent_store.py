import contextlib
import io
import tempfile
import unittest
from pathlib import Path

from helpers import col

from snowdrift import agent
from snowdrift.cli import main
from snowdrift.detector import DetectSettings
from snowdrift.models import DriftType
from snowdrift.store import Store


class FakeSource:
    def __init__(self, platform, cols):
        self.platform, self.cols = platform, cols

    def fetch(self, tables):
        return self.cols


class BrokenSource:
    platform = "snowflake"

    def fetch(self, tables):
        raise ConnectionError("network down")


SETTINGS = DetectSettings(reference="teradata", targets=["snowflake"])
TD = [col("ID", "I"), col("NOTE", "CV", ordinal=2, length=10)]
SF_OK = [col("ID", "NUMBER", "snowflake", precision=10), col("NOTE", "TEXT", "snowflake", ordinal=2, length=10)]
SF_MISSING = [col("ID", "NUMBER", "snowflake", precision=10)]


class AgentStoreTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.store = Store(str(Path(self.tmp.name) / "t.db"))

    def tearDown(self):
        self.store.close()
        self.tmp.cleanup()

    def _run(self, day, sf_cols):
        sources = {"teradata": FakeSource("teradata", TD), "snowflake": FakeSource("snowflake", sf_cols)}
        return agent.run(day, sources, SETTINGS, self.store, dialects={"snowflake": "snowflake"})

    def test_new_then_still_open_then_resolved(self):
        r1 = self._run("2026-01-01", SF_MISSING)
        self.assertEqual(len(r1.events), 1)
        self.assertEqual(len(r1.new_ids), 1)
        self.assertIn('ADD COLUMN "NOTE"', r1.events[0].fix["snowflake"])
        self.assertTrue(r1.events[0].summary)  # template fallback with no model

        r2 = self._run("2026-01-02", SF_MISSING)
        self.assertEqual(len(r2.events), 1)
        self.assertEqual(r2.new_ids, set())  # not re-alerted
        self.assertEqual(r2.events[0].summary, r1.events[0].summary)  # explanation carried over

        r3 = self._run("2026-01-03", SF_OK)  # someone applied the fix
        self.assertEqual([e.drift_type for e in r3.events], [DriftType.COLUMN_ADDED])  # the fix itself, as context
        statuses = {row["drift_id"]: row["status"] for row in self.store.list_events()}
        self.assertEqual(statuses[r1.events[0].drift_id], "resolved")

    def test_false_positive_is_suppressed(self):
        r1 = self._run("2026-01-01", SF_MISSING)
        self.store.set_status(r1.events[0].drift_id, "false_positive")
        r2 = self._run("2026-01-02", SF_MISSING)
        self.assertEqual(r2.events, [])
        self.assertEqual(r2.suppressed, 1)

    def test_day_over_day_uses_previous_snapshot(self):
        self._run("2026-01-01", SF_OK)
        r2 = self._run("2026-01-02", SF_MISSING)
        kinds = {e.drift_type for e in r2.events}
        self.assertIn(DriftType.COLUMN_DROPPED, kinds)
        self.assertEqual(r2.previous_date, "2026-01-01")

    def test_failed_source_becomes_a_warning(self):
        sources = {"teradata": FakeSource("teradata", TD), "snowflake": BrokenSource()}
        report = agent.run("2026-01-01", sources, SETTINGS, self.store)
        self.assertTrue(any("network down" in w for w in report.warnings))

    def test_bad_status_rejected(self):
        with self.assertRaises(ValueError):
            self.store.set_status("x", "done")


class DemoTests(unittest.TestCase):
    def test_demo_finds_the_planted_drift(self):
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp) / "report.md"
            with contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(main(["demo", "--out", str(out)]), 0)
            text = out.read_text()
        for expected in [
            "column_missing on JOBS.CREW_LEAD (teradata vs snowflake:final)",
            "precision_shrunk on INVOICES.AMOUNT (teradata vs databricks:final)",
            "length_shrunk on CUSTOMERS.EMAIL (teradata vs snowflake:final)",
            "possible_rename on CUSTOMERS.PHONE (teradata vs databricks:final)",
            "nullability_changed on JOBS.STATUS (teradata vs databricks:final)",
            "collation_mismatch on CUSTOMERS.LAST_NAME",
            'ALTER TABLE "PUBLIC"."JOBS" ADD COLUMN "CREW_LEAD" VARCHAR(40);',
        ]:
            self.assertIn(expected, text)
        self.assertNotIn("precision_grew on CUSTOMERS.CUSTOMER_ID", text)  # info is filtered out


if __name__ == "__main__":
    unittest.main()
