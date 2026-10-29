# Adapting snowdrift to your platform

snowdrift knows nothing about your company. Everything specific to your setup goes in the config file, or in a small Python module if you need more. Nothing in the package has to change.

## 1. Map your replication setup

| Your setup has... | Put it in |
| --- | --- |
| A source system everything copies from | `[run] reference` |
| Warehouses that receive copies | `[run] targets` and one `[sources.<name>]` each |
| Staging / work tables loaded before the final table | `work_schemas` on that source |
| Columns only the targets have (load timestamps, batch IDs) | `[run] ignore_columns` |
| A table of merge/upsert keys | `[impact.key_columns]`, or load it in code (below) |
| Hand-written MERGE statements or procedures stored somewhere | export each to a `.sql` file in `stored_sql_dir` |
| Views over the replicated tables | export definitions to `views_dir` |
| Tables reloaded in full each run | `[impact] full_refresh` |
| An expected collation | `[collation]` |

## 2. Load impact settings from your own control tables

If your keys or stored SQL already live in database tables, build `ImpactSettings` in code instead of the TOML file:

```python
from snowdrift import ImpactSettings, run
from snowdrift.sources import dbapi_runner

q = dbapi_runner(conn)
keys = {}
for r in q("SELECT table_name, column_name FROM my_control.merge_keys"):
    keys.setdefault(r["table_name"].upper(), []).append(r["column_name"].upper())
stored = {r["name"]: r["sql_text"] for r in q("SELECT name, sql_text FROM my_control.stored_merges")}

impact = ImpactSettings(key_columns=keys, stored_sql=stored)
```

## 3. Add your own impact check

A check is a function `(event, settings) -> list[Impact]`. Append it to the list passed to `impact.assess`:

```python
from snowdrift.impact import CHECKS, Impact


def dedup_rules(event, settings):
    if event.table in MY_DEDUP_TABLES and event.column in MY_DEDUP_TABLES[event.table]:
        return [Impact("dedup", "DEDUP", event.table, True, "Column is used to pick the surviving row.")]
    return []


CHECKS.append(dedup_rules)
```

## 4. Plug in a model

- **Databricks model serving / any OpenAI-compatible API:** `[ai] provider = "openai_compatible"` with `base_url` and `model`.
- **Databricks `ai_query` or Snowflake Cortex:** pass `AIQueryClient(spark, endpoint)` or `CortexClient(session, model)` as `llm=` to `snowdrift.notebook.check` (see `notebooks/`).
- **No model approved yet:** leave `provider = "none"`. Every explanation falls back to a template.

The model only receives metadata (names, types, impacts). It never sees row data, and it never writes the DDL.

## 5. Roll it out safely

1. Start with 10–20 tables and `min_severity = "warning"`.
2. Run daily for a few days and review every event. Mark noise with `snowdrift mark -c drift.toml <id> false_positive`, then tune `ignore_columns` or the type map.
3. Turn on alerts once a run produces no false positives.
4. Treat fix drafts as drafts: test on a clone, then go through your normal change process.
