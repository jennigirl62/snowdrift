import json
import unittest
from unittest import mock

from helpers import col

from snowdrift import ai, alerts
from snowdrift.models import Comparison, DriftType, Severity
from snowdrift.report import RunReport
from snowdrift.rules import compare_table


def missing_event():
    a = [col("ID", "I"), col("NOTE", "CV", ordinal=2, length=10)]
    return compare_table("ORDERS", a, [col("ID", "int", "databricks")], Comparison.CROSS_PLATFORM, "td", "databricks:final")[0]


class StubLLM:
    def __init__(self, answer):
        self.answer, self.prompts = answer, []

    def complete(self, prompt):
        self.prompts.append(prompt)
        return self.answer


class AITests(unittest.TestCase):
    def test_fallback_when_model_fails(self):
        text = ai.summarize_event(missing_event(), ai.NoLLM())
        self.assertIn("ORDERS.NOTE", text)

    def test_prompt_contains_only_metadata_facts(self):
        llm = StubLLM("NOTE is missing in Databricks.")
        self.assertEqual(ai.summarize_event(missing_event(), llm), "NOTE is missing in Databricks.")
        self.assertIn("Use only the facts listed", llm.prompts[0])
        self.assertIn("Table: ORDERS", llm.prompts[0])

    def test_rename_verdict_parsing(self):
        a = [col("PHONE", "CV", length=20)]
        b = [col("PHONE_NO", "string", "db")]
        e = [x for x in compare_table("T", a, b, Comparison.CROSS_PLATFORM, "td", "db")
             if x.drift_type is DriftType.POSSIBLE_RENAME][0]  # fmt: skip
        self.assertEqual(ai.score_rename(e, StubLLM("YES. Same meaning, longer name.")), "YES: Same meaning, longer name")
        self.assertIsNone(ai.score_rename(e, StubLLM("Maybe")))

    def test_enrich_skips_low_severity(self):
        e = missing_event()
        e.severity = Severity.INFO
        llm = StubLLM("x")
        ai.enrich([e], llm, Severity.WARNING)
        self.assertEqual(llm.prompts, [])

    def test_openai_compatible_request(self):
        client = ai.OpenAICompatibleClient("https://example.test/serving-endpoints/", "my-endpoint", "KEY_ENV")
        captured = {}

        class Resp(dict):
            def __enter__(self):
                return self

            def __exit__(self, *a):
                return False

            def read(self, *a):
                return json.dumps({"choices": [{"message": {"content": " hi "}}]}).encode()

        def fake_urlopen(req, timeout):
            captured["url"], captured["body"] = req.full_url, json.loads(req.data)
            captured["auth"] = req.headers["Authorization"]
            return Resp()

        with mock.patch.dict("os.environ", {"KEY_ENV": "secret"}), mock.patch("urllib.request.urlopen", fake_urlopen):
            self.assertEqual(client.complete("prompt"), "hi")
        self.assertEqual(captured["url"], "https://example.test/serving-endpoints/chat/completions")
        self.assertEqual(captured["body"]["model"], "my-endpoint")
        self.assertEqual(captured["auth"], "Bearer secret")


class AlertTests(unittest.TestCase):
    def report(self, new=True):
        e = missing_event()
        return RunReport("2026-01-01", [e], {e.drift_id} if new else set(), summary="One breaking change.")

    def test_servicenow_one_incident_per_table_for_new_breaking(self):
        n = alerts.ServiceNowNotifier("https://snow.example", "Data Platform")
        incidents = n.incidents(self.report())
        self.assertEqual(len(incidents), 1)
        self.assertIn("ORDERS", incidents[0]["short_description"])
        self.assertEqual(n.incidents(self.report(new=False)), [])

    def test_teams_payload_is_adaptive_card(self):
        p = alerts.TeamsWebhookNotifier().payload(self.report())
        card = p["attachments"][0]["content"]
        self.assertEqual(card["type"], "AdaptiveCard")
        self.assertIn("1 breaking", card["body"][0]["text"])

    def test_teams_skips_without_url(self):
        with mock.patch.dict("os.environ", {}, clear=True), mock.patch("snowdrift.alerts._post_json") as post:
            alerts.TeamsWebhookNotifier().send(self.report())
        post.assert_not_called()

    def test_markdown_report(self):
        md = self.report().to_markdown()
        self.assertIn("# Schema drift report: 2026-01-01", md)
        self.assertIn("NEW [breaking]", md)


if __name__ == "__main__":
    unittest.main()
