"""Silver layer: typed, deduplicated, validated, merged into one table per concept."""

from dataclasses import dataclass, field

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from lakehouse import config
from lakehouse.sql_templates import run_sql


@dataclass
class SilverCounts:
    bronze_rows: int
    valid_rows: int
    quarantined_rows: int
    rule_counts: dict[str, int] = field(default_factory=dict)


def build_silver_price_areas(spark: SparkSession, database: str) -> None:
    run_sql(spark, "silver_price_areas.sql", database=database)


def build_silver_production(spark: SparkSession, database: str, batch_id: str) -> SilverCounts:
    """Process one bronze batch. Each row ends up either in silver or in quarantine."""
    batch = spark.table(f"{database}.bronze_energinet_prod_5min").where(F.col("batch_id") == batch_id)
    batch.createOrReplaceTempView("bronze_batch")
    bronze_rows = batch.count()

    run_sql(spark, "silver_production_parse.sql")
    run_sql(spark, "silver_production_checks.sql", max_abs_mw=config.MAX_ABS_MW)
    run_sql(spark, "silver_production_dedupe.sql")

    rule_rows = spark.sql(
        "SELECT rule, count(*) AS n FROM (SELECT explode(failed_rules) AS rule FROM checked_production) GROUP BY rule"
    ).collect()
    rule_counts = {row["rule"]: row["n"] for row in rule_rows}

    run_sql(spark, "silver_production_merge.sql", database=database)
    run_sql(spark, "quarantine_merge.sql", database=database)

    valid_rows = spark.table("valid_production").count()
    quarantined_rows = spark.table("quarantined_production").count()
    return SilverCounts(bronze_rows, valid_rows, quarantined_rows, rule_counts)
