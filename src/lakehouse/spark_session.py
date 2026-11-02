"""Create a Spark session with Delta Lake enabled, locally or on Databricks."""

import os
from pathlib import Path

from pyspark.sql import SparkSession

from lakehouse import config


def get_spark(app_name: str = "lakehouse-pipeline", warehouse: Path | None = None) -> SparkSession:
    """Return the Databricks session, or start a local Delta-enabled one."""
    if "DATABRICKS_RUNTIME_VERSION" in os.environ:
        return SparkSession.builder.getOrCreate()

    from delta import configure_spark_with_delta_pip

    warehouse = warehouse or config.WAREHOUSE_DIR
    metastore = warehouse / "_metastore"
    metastore.mkdir(parents=True, exist_ok=True)

    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.warehouse.dir", str(warehouse))
        # Keep the local Derby metastore inside the warehouse, not in the current directory.
        .config("spark.hadoop.javax.jdo.option.ConnectionURL", f"jdbc:derby:;databaseName={metastore / 'metastore_db'};create=true")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.enabled", "false")
        # Persist table definitions across runs. Without the Hive catalog, tables are forgotten on exit.
        .enableHiveSupport()
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()
