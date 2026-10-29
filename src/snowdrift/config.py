"""Load a TOML config file and build everything the agent needs. See examples/config.example.toml."""

from __future__ import annotations

import importlib
import os
import tomllib
from dataclasses import dataclass, field
from pathlib import Path

from . import ai, alerts
from .detector import DetectSettings
from .impact import ImpactSettings
from .models import Severity
from .sources import (
    CsvSource,
    DatabricksSource,
    SnowflakeSource,
    TeradataExportSource,
    TeradataSource,
    dbapi_runner,
)


@dataclass
class Config:
    detect: DetectSettings
    impact: ImpactSettings
    sources: dict
    dialects: dict[str, str]
    llm: ai.LLMClient
    notifiers: list = field(default_factory=list)
    store_path: str = "drift.db"


def _read_sql_dir(path: str | None, base: Path) -> dict[str, str]:
    if not path:
        return {}
    d = (base / path) if not Path(path).is_absolute() else Path(path)
    return {p.stem.upper(): p.read_text() for p in sorted(d.glob("*.sql"))}


def _connect(kind: str, params: dict):
    """Open a DB-API connection with an optional driver. Secrets come from env vars named in the config."""
    missing = [v for k, v in params.items() if k.endswith("_env") and v not in os.environ]
    if missing:
        raise RuntimeError(f"{kind} connection needs environment variable(s) {', '.join(missing)}")
    resolved = {k[:-4] if k.endswith("_env") else k: (os.environ[v] if k.endswith("_env") else v) for k, v in params.items()}
    if kind == "snowflake":
        return importlib.import_module("snowflake.connector").connect(**resolved)
    if kind == "databricks":
        return importlib.import_module("databricks.sql").connect(**resolved)
    if kind == "teradata":
        return importlib.import_module("teradatasql").connect(**resolved)
    raise ValueError(f"unknown connection kind {kind}")


def build_source(platform: str, spec: dict, base: Path):
    kind = spec["type"]
    work = spec.get("work_schemas", [])
    if kind == "csv":
        return CsvSource(platform, base / spec["path"], work)
    if kind == "teradata_export":
        return TeradataExportSource(platform, base / spec["path"], work)
    # Connect lazily, inside fetch(), so one unreachable platform becomes a run warning, not a crash.
    run_query = dbapi_runner(lambda: _connect(kind, spec.get("connection", {})))
    if kind == "snowflake":
        return SnowflakeSource(run_query, spec["database"], spec["schemas"], work, platform)
    if kind == "databricks":
        return DatabricksSource(run_query, spec["catalog"], spec["schemas"], work, platform)
    if kind == "teradata":
        return TeradataSource(run_query, spec["databases"], work, platform)
    raise ValueError(f"unknown source type {kind!r} for {platform}")


def build_llm(spec: dict) -> ai.LLMClient:
    provider = spec.get("provider", "none")
    if provider == "none":
        return ai.NoLLM()
    if provider == "openai_compatible":
        return ai.OpenAICompatibleClient(spec["base_url"], spec["model"], spec.get("api_key_env", "LLM_API_KEY"))
    raise ValueError(
        f"unknown ai provider {provider!r} (in notebooks, pass AIQueryClient or CortexClient to snowdrift.notebook.check)"
    )


def build_notifiers(spec: dict) -> list:
    out: list = []
    if spec.get("console", True):
        out.append(alerts.ConsoleNotifier())
    if "email" in spec:
        e = spec["email"]
        out.append(
            alerts.EmailNotifier(
                e["host"],
                e["from"],
                e["to"],
                e.get("port", 25),
                e.get("starttls", False),
                e.get("user_env"),
                e.get("password_env"),
            )
        )
    if "teams" in spec:
        out.append(alerts.TeamsWebhookNotifier(spec["teams"].get("webhook_env", "TEAMS_WEBHOOK_URL")))
    if "servicenow" in spec:
        s = spec["servicenow"]
        out.append(
            alerts.ServiceNowNotifier(
                s["instance_url"],
                s["assignment_group"],
                s.get("user_env", "SNOW_USER"),
                s.get("password_env", "SNOW_PASSWORD"),
                s.get("extra_fields"),
            )
        )
    return out


def load(path: str | Path, connect_sources: bool = True) -> Config:
    path = Path(path)
    base = path.parent
    raw = tomllib.loads(path.read_text())
    r = raw.get("run", {})
    detect = DetectSettings(
        reference=r["reference"],
        targets=r.get("targets", []),
        tables=[t.upper() for t in r.get("tables", [])],
        ignore_columns=r.get("ignore_columns", []),
        expected_collation=raw.get("collation", {}),
        min_severity=Severity(r.get("min_severity", "warning")),
    )
    imp = raw.get("impact", {})
    impact = ImpactSettings(
        key_columns={k.upper(): v for k, v in imp.get("key_columns", {}).items()},
        full_refresh={t.upper() for t in imp.get("full_refresh", [])},
        stored_sql=_read_sql_dir(imp.get("stored_sql_dir"), base),
        views=_read_sql_dir(imp.get("views_dir"), base),
    )
    src_specs = raw.get("sources", {})
    sources = {p: build_source(p, s, base) for p, s in src_specs.items()} if connect_sources else {}
    dialects = {
        p: s.get("dialect", s["type"] if s["type"] in ("snowflake", "databricks", "teradata") else p)
        for p, s in src_specs.items()
    }
    store_path = r.get("store", "drift.db")
    return Config(
        detect=detect,
        impact=impact,
        sources=sources,
        dialects=dialects,
        llm=build_llm(raw.get("ai", {})),
        notifiers=build_notifiers(raw.get("alerts", {})),
        store_path=str(base / store_path) if not Path(store_path).is_absolute() else store_path,
    )
