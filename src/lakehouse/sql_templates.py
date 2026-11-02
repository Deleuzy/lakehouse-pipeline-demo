"""Load SQL files from src/lakehouse/sql, fill in parameters and run them.

Each .sql file holds statements separated by semicolons. Names are filled in
with ${placeholders} so the same file works for any database.
"""

from pathlib import Path
from string import Template

from pyspark.sql import DataFrame, SparkSession

SQL_DIR = Path(__file__).parent / "sql"


def render(name: str, **params: object) -> list[str]:
    text = (SQL_DIR / name).read_text(encoding="utf-8")
    filled = Template(text).substitute({key: str(value) for key, value in params.items()})
    return [statement.strip() for statement in filled.split(";") if statement.strip()]


def run_sql(spark: SparkSession, name: str, **params: object) -> DataFrame | None:
    """Run every statement in a SQL file in order. Returns the last result, if any."""
    result = None
    for statement in render(name, **params):
        result = spark.sql(statement)
    return result
