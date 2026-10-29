"""Send the run report somewhere people will see it. One message per channel per run."""

from __future__ import annotations

import base64
import json
import logging
import os
import smtplib
import urllib.request
from email.message import EmailMessage
from typing import Protocol

from .models import DriftEvent
from .report import RunReport

log = logging.getLogger(__name__)


class Notifier(Protocol):
    def send(self, report: RunReport) -> None: ...


def _post_json(url: str, payload: dict, headers: dict | None = None, timeout: int = 30) -> dict:
    req = urllib.request.Request(
        url, data=json.dumps(payload).encode(), method="POST", headers={"Content-Type": "application/json", **(headers or {})}
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        body = resp.read()
    return json.loads(body) if body else {}


class ConsoleNotifier:
    def send(self, report: RunReport) -> None:
        print(report.to_markdown())


class EmailNotifier:
    """Plain SMTP. Sends only when there are open events, unless always=True."""

    def __init__(
        self,
        host: str,
        sender: str,
        to: list[str],
        port: int = 25,
        starttls: bool = False,
        user_env: str | None = None,
        password_env: str | None = None,
        always: bool = False,
    ):
        self.host, self.port, self.sender, self.to = host, port, sender, to
        self.starttls, self.user_env, self.password_env, self.always = starttls, user_env, password_env, always

    def send(self, report: RunReport) -> None:
        if not report.events and not self.always:
            return
        msg = EmailMessage()
        c = report.counts()
        msg["Subject"] = f"[snowdrift] {report.run_date}: {c.get('breaking', 0)} breaking, {len(report.new_ids)} new"
        msg["From"], msg["To"] = self.sender, ", ".join(self.to)
        msg.set_content(report.to_markdown())
        with smtplib.SMTP(self.host, self.port, timeout=30) as smtp:
            if self.starttls:
                smtp.starttls()
            if self.user_env and self.password_env:
                if not self.starttls:
                    raise RuntimeError("refusing to send SMTP credentials without starttls = true")
                smtp.login(os.environ[self.user_env], os.environ[self.password_env])
            smtp.send_message(msg)


class TeamsWebhookNotifier:
    """Microsoft Teams 'post to a channel when a webhook request is received' workflow."""

    def __init__(self, webhook_env: str = "TEAMS_WEBHOOK_URL", only_new: bool = True):
        self.webhook_env, self.only_new = webhook_env, only_new

    def payload(self, report: RunReport) -> dict:
        return {
            "type": "message",
            "attachments": [
                {
                    "contentType": "application/vnd.microsoft.card.adaptive",
                    "content": {
                        "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
                        "type": "AdaptiveCard",
                        "version": "1.4",
                        "body": [
                            {"type": "TextBlock", "text": line, "wrap": True}
                            for line in report.to_text().splitlines()
                            if line.strip()
                        ],
                    },
                }
            ],
        }

    def send(self, report: RunReport) -> None:
        if self.only_new and not report.new_ids:
            return
        url = os.environ.get(self.webhook_env)
        if not url:
            log.warning("Teams webhook env var %s not set; skipping", self.webhook_env)
            return
        _post_json(url, self.payload(report))


class ServiceNowNotifier:
    """Opens one incident per table with NEW breaking drift, via the ServiceNow Table API."""

    def __init__(
        self,
        instance_url: str,
        assignment_group: str,
        user_env: str = "SNOW_USER",
        password_env: str = "SNOW_PASSWORD",
        extra_fields: dict | None = None,
    ):
        self.url = instance_url.rstrip("/") + "/api/now/table/incident"
        self.assignment_group, self.user_env, self.password_env = assignment_group, user_env, password_env
        self.extra_fields = extra_fields or {}

    def incidents(self, report: RunReport) -> list[dict]:
        by_table: dict[str, list[DriftEvent]] = {}
        for e in report.breaking_new:
            by_table.setdefault(e.table, []).append(e)
        out = []
        for table, events in sorted(by_table.items()):
            lines = [f"- {e.label()} [{e.drift_id}]" + (f"\n  {e.summary}" if e.summary else "") for e in events]
            out.append(
                {
                    "short_description": f"Breaking schema drift on {table} ({len(events)} change(s))",
                    "description": "\n".join(lines) + "\n\nDraft fixes are in the snowdrift report.",
                    "assignment_group": self.assignment_group,
                    **self.extra_fields,
                }
            )
        return out

    def send(self, report: RunReport) -> None:
        user, pw = os.environ.get(self.user_env), os.environ.get(self.password_env)
        if not (user and pw):
            log.warning("ServiceNow credentials not set; skipping")
            return
        auth = base64.b64encode(f"{user}:{pw}".encode()).decode()
        for incident in self.incidents(report):
            _post_json(self.url, incident, headers={"Authorization": f"Basic {auth}", "Accept": "application/json"})
