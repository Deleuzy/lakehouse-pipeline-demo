# Databricks notebook source
# MAGIC %md
# MAGIC # Lakehouse pipeline demo
# MAGIC Runs bronze -> silver -> gold on the saved Energinet sample using the same code as the local run.
# MAGIC See `databricks/README.md` for setup. **Not yet run on Databricks.**

# COMMAND ----------

import os
import sys
from pathlib import Path

# Terraform passes workspace_root as a job parameter. The default is only used when the notebook
# runs by hand, and must equal the default in infra/variables.tf (a test checks this).
DEFAULT_WORKSPACE_ROOT = "/Shared/lakehouse-pipeline"
try:
    workspace_root = dbutils.widgets.get("workspace_root")  # noqa: F821  (dbutils exists on Databricks)
except Exception:
    workspace_root = DEFAULT_WORKSPACE_ROOT
REPO_PATH = f"/Workspace{workspace_root}"
sys.path.insert(0, f"{REPO_PATH}/src")
os.environ["LAKEHOUSE_ROOT"] = REPO_PATH

# COMMAND ----------

from lakehouse.pipeline import run_pipeline

report = run_pipeline(spark, database="lakehouse_demo", source="sample", report_dir=Path("/tmp/lakehouse_reports"))
report.to_dict()

# COMMAND ----------

display(spark.table("lakehouse_demo.gold_daily_area_summary"))

# COMMAND ----------

display(spark.table("lakehouse_demo.quarantine_production_5min"))
