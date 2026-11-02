# Databricks notebook source
# MAGIC %md
# MAGIC # Lakehouse pipeline demo
# MAGIC Runs bronze -> silver -> gold on the saved Energinet sample using the same code as the local run.
# MAGIC See `databricks/README.md` for setup. **Not yet run on Databricks.**

# COMMAND ----------

import os
import sys
from pathlib import Path

REPO_PATH = "/Workspace/Users/<your-user>/lakehouse-pipeline-demo"  # set this to your clone
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
