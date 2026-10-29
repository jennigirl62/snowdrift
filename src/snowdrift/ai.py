"""AI layer: plain-English explanations and rename scoring.

Rules for the model:
- It only sees metadata (names, types, impacts), never row data.
- It explains drift the SQL rules already found; it does not decide whether drift exists.
- Every call has a deterministic fallback, so the agent still works with no model at all.
"""

from __future__ import annotations

import json
import logging
import os
import re
import urllib.request
from collections import Counter
from typing import Protocol

from .models import DriftEvent, Severity

log = logging.getLogger(__name__)


class LLMClient(Protocol):
    def complete(self, prompt: str) -> str: ...


class NoLLM:
    """Use when no model is available. Everything falls back to templates."""

    def complete(self, prompt: str) -> str:
        raise RuntimeError("no model configured")


class OpenAICompatibleClient:
    """Any /chat/completions endpoint: Databricks model serving, Azure OpenAI, a local server, etc.

    Databricks: base_url = https://<workspace-host>/serving-endpoints, model = <endpoint name>.
    """

    def __init__(self, base_url: str, model: str, api_key_env: str = "LLM_API_KEY", timeout: int = 60):
        self.url = base_url.rstrip("/") + "/chat/completions"
        self.model = model
        self.api_key_env = api_key_env
        self.timeout = timeout

    def complete(self, prompt: str) -> str:
        key = os.environ.get(self.api_key_env)
        if not key:
            raise RuntimeError(f"environment variable {self.api_key_env} is not set")
        body = json.dumps(
            {
                "model": self.model,
                "messages": [{"role": "user", "content": prompt}],
                "max_tokens": 400,
                "temperature": 0,
            }
        ).encode()
        req = urllib.request.Request(
            self.url,
            data=body,
            method="POST",
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {key}",
            },
        )
        with urllib.request.urlopen(req, timeout=self.timeout) as resp:
            data = json.load(resp)
        return data["choices"][0]["message"]["content"].strip()


_MODEL_NAME = re.compile(r"^[A-Za-z0-9_.\-]+$")


def _model_literal(name: str) -> str:
    # Model / endpoint names go in as SQL literals (both functions want a constant); the prompt is always bound.
    if not _MODEL_NAME.match(name):
        raise ValueError(f"unexpected model name: {name!r}")
    return f"'{name}'"


class AIQueryClient:
    """Databricks notebooks and jobs: ai_query() against a model serving endpoint."""

    def __init__(self, spark, endpoint: str):
        self.spark = spark
        self.sql = f"SELECT ai_query({_model_literal(endpoint)}, :prompt) AS answer"

    def complete(self, prompt: str) -> str:
        value = self.spark.sql(self.sql, args={"prompt": prompt}).first()[0]
        return "" if value is None else str(value).strip()


class CortexClient:
    """Snowflake notebooks: SNOWFLAKE.CORTEX.COMPLETE with a Cortex model name (e.g. 'mistral-large2')."""

    def __init__(self, session, model: str):
        self.session = session
        self.sql = f"SELECT SNOWFLAKE.CORTEX.COMPLETE({_model_literal(model)}, ?) AS ANSWER"

    def complete(self, prompt: str) -> str:
        value = self.session.sql(self.sql, params=[prompt]).collect()[0][0]
        return "" if value is None else str(value).strip()


EVENT_PROMPT = """You are a senior data engineer writing a short alert for teammates.
Explain the schema drift below in at most 3 plain sentences: what changed, what it breaks, and the next step.
Use only the facts listed. Do not invent tables, numbers or causes. No preamble.

Facts:
- Change: {drift_type} ({comparison})
- Table: {table}
- Column: {column}
- {side_a}: {value_a}
- {side_b}: {value_b}
- Severity: {severity}
- Downstream impacts: {impacts}
- Draft fix available: {has_fix}
"""

RUN_PROMPT = """You are a senior data engineer. Write a 2-3 sentence summary of today's schema drift run for a team channel.
Lead with what needs action. Use only these facts; do not invent anything.

Counts by severity: {counts}
New today: {new}
Events:
{lines}
"""

RENAME_PROMPT = """Two columns in table {table} look like a rename: '{old}' disappeared and '{new}' appeared, both {type}.
Is '{new}' most likely a rename of '{old}'? Answer YES or NO, then one short reason."""


def template_summary(e: DriftEvent) -> str:
    where = f"{e.table}.{e.column}" if e.column else e.table
    change = e.drift_type.value.replace("_", " ")
    text = f"{change.capitalize()} on {where}: {e.side_a} has {e.value_a or 'nothing'}, {e.side_b} has {e.value_b or 'nothing'}."
    breaking = [i for i in e.impacts if i.breaks]
    if breaking:
        text += " Breaks: " + "; ".join(f"{i.object_type} {i.object_name}" for i in breaking) + "."
    if e.fix:
        text += " A draft fix is ready for review."
    return text


def summarize_event(e: DriftEvent, client: LLMClient) -> str:
    impacts = (
        "; ".join(f"{i.object_type} {i.object_name} ({'breaks' if i.breaks else 'review'}): {i.note}" for i in e.impacts)
        or "none found"
    )
    prompt = EVENT_PROMPT.format(
        drift_type=e.drift_type.value,
        comparison=e.comparison.value,
        table=e.table,
        column=e.column or "(table level)",
        side_a=e.side_a,
        value_a=e.value_a or "absent",
        side_b=e.side_b,
        value_b=e.value_b or "absent",
        severity=e.severity.value,
        impacts=impacts,
        has_fix="yes" if e.fix else "no",
    )
    return _ask(client, prompt) or template_summary(e)


def summarize_run(events: list[DriftEvent], new_ids: set[str], client: LLMClient) -> str:
    counts = Counter(e.severity.value for e in events)
    if not events:
        return "No schema drift found."
    lines = "\n".join(f"- [{e.severity.value}] {e.label()}" for e in events[:40])
    prompt = RUN_PROMPT.format(counts=dict(counts), new=len(new_ids), lines=lines)
    fallback = (
        f"{len(events)} open drift events ({counts.get('breaking', 0)} breaking, "
        f"{counts.get('warning', 0)} warning, {counts.get('info', 0)} info); {len(new_ids)} new today."
    )
    return _ask(client, prompt) or fallback


def score_rename(e: DriftEvent, client: LLMClient) -> str | None:
    """Ask the model whether a possible rename is real. Returns 'YES: reason', 'NO: reason' or None."""
    prompt = RENAME_PROMPT.format(table=e.table, old=e.value_a, new=e.value_b, type=e.detail.split(",")[0])
    answer = _ask(client, prompt)
    if not answer:
        return None
    first = answer.split()[0].strip(".,:").upper()
    if first not in ("YES", "NO"):
        return None
    return f"{first}: {answer[len(answer.split()[0]) :].strip(' .,:')}"


def _ask(client: LLMClient, prompt: str) -> str | None:
    try:
        text = client.complete(prompt)
        return text or None
    except Exception as exc:  # the agent must keep running without a model
        log.debug("LLM call failed, using fallback: %s", exc)
        return None


def enrich(events: list[DriftEvent], client: LLMClient, min_severity: Severity = Severity.WARNING) -> None:
    for e in events:
        if e.severity.rank < min_severity.rank:
            continue
        if e.drift_type.value == "possible_rename":
            verdict = score_rename(e, client)
            if verdict:
                e.detail += f"; model says {verdict}"
        e.summary = summarize_event(e, client)
