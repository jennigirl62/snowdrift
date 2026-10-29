# Databricks notebook source
# MAGIC %md
# MAGIC # snowdrift: daily schema drift check (Databricks)
# MAGIC
# MAGIC Compares the source system (Teradata) with its copies in Databricks and Snowflake, flags what breaks,
# MAGIC drafts fix SQL for review, and keeps history in Delta tables.
# MAGIC
# MAGIC **Needs:** Databricks Runtime 16.4 LTS or newer (Python 3.12). Schedule it as a daily job after the
# MAGIC Teradata metadata export lands. Fill in every `<...>` below.

# COMMAND ----------

# MAGIC %pip install git+https://github.com/jennigirl62/snowdrift.git
# MAGIC # No GitHub access from the workspace? Upload the wheel or snowdrift.zip to a Volume and install that instead:
# MAGIC # %pip install /Volumes/<catalog>/<schema>/<volume>/snowdrift-0.1.0-py3-none-any.whl

# COMMAND ----------

from snowdrift.ai import AIQueryClient
from snowdrift.notebook import check
from snowdrift.sources import (
    DatabricksSource,
    SnowflakeSource,
    TeradataExportSource,
    databricks_views,
    spark_reader_runner,
    spark_runner,
)
from snowdrift.store import SparkBackend

# ---- What to compare ---------------------------------------------------------
TABLES = ["CUSTOMERS", "JOBS", "INVOICES"]  # start with 10-20 tables
CATALOG = "<catalog>"  # Unity Catalog holding the Databricks copies
DB_SCHEMAS = ["<final_schema>", "<work_schema>"]
DB_WORK_SCHEMAS = ["<work_schema>"]
HISTORY_SCHEMA = "<catalog>.snowdrift"  # where snowdrift keeps its own history (Delta)
TERADATA_EXPORT = "/Volumes/<catalog>/<schema>/<volume>/teradata_columns.csv"  # export of sql/teradata_columns.sql
MODEL_ENDPOINT = "<serving-endpoint-name>"  # or None for template explanations

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {HISTORY_SCHEMA}")  # noqa: F821

# COMMAND ----------

# ---- Where metadata comes from -------------------------------------------------
sources = {
    # Teradata: a scheduled export file (or swap in TeradataSource(spark_reader_runner(spark, "jdbc", {...}), [...]))
    "teradata": TeradataExportSource("teradata", TERADATA_EXPORT),
    # Databricks: this workspace
    "databricks": DatabricksSource(spark_runner(spark), CATALOG, DB_SCHEMAS, work_schemas=DB_WORK_SCHEMAS),  # noqa: F821
}

# Snowflake (optional): through the Spark Snowflake connector. Keep credentials in a secret scope.
SNOWFLAKE_OPTIONS = None  # e.g. {"sfUrl": "<account>.snowflakecomputing.com", "sfDatabase": "<db>", "sfWarehouse": "<wh>",
#        "sfUser": dbutils.secrets.get("<scope>", "sf-user"), "sfPassword": dbutils.secrets.get("<scope>", "sf-password")}
if SNOWFLAKE_OPTIONS:
    sources["snowflake"] = SnowflakeSource(
        spark_reader_runner(spark, "snowflake", SNOWFLAKE_OPTIONS),  # noqa: F821
        SNOWFLAKE_OPTIONS["sfDatabase"],
        ["<FINAL_SCHEMA>", "<WORK_SCHEMA>"],
        work_schemas=["<WORK_SCHEMA>"],
    )

# COMMAND ----------

report = check(
    sources=sources,
    reference="teradata",
    targets=[p for p in sources if p != "teradata"],
    tables=TABLES,
    store=SparkBackend(spark, HISTORY_SCHEMA),  # noqa: F821
    ignore_columns=["<LOAD_AUDIT_COLUMN>"],  # columns that only exist in the copies
    key_columns={"JOBS": ["JOB_ID"]},  # merge/upsert keys per table
    views=databricks_views(spark_runner(spark), CATALOG, DB_SCHEMAS),  # noqa: F821
    llm=AIQueryClient(spark, MODEL_ENDPOINT) if MODEL_ENDPOINT else None,  # noqa: F821
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Results as a table (sort, filter, export)

# COMMAND ----------

if report.events:
    display(spark.createDataFrame(report.to_rows()))  # noqa: F821
else:
    print("No drift at or above the reporting threshold.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Mark a false positive
# MAGIC Copy a `drift_id` from the table above. It stays quiet until that column changes again.

# COMMAND ----------

# from snowdrift.store import Store
# Store(SparkBackend(spark, HISTORY_SCHEMA)).set_status("<drift_id>", "false_positive")
