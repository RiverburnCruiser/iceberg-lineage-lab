"""Data contracts: the rules a governed job's inputs and outputs must satisfy."""
from pyspark.sql import DataFrame, SparkSession


class DataContractError(Exception):
    """A dataset broke its contract. Never retried: running again will not fix the data."""


def current_snapshot(spark: SparkSession, table: str, ref: str = "main") -> int | None:
    """Snapshot ID that `ref` (a branch or tag) points to, or None if it does not exist."""
    rows = spark.sql(f"SELECT snapshot_id FROM {table}.refs WHERE name = '{ref}'").collect()
    return rows[0].snapshot_id if rows else None


def check_inputs(spark: SparkSession, contracts: dict[str, list[str]]) -> None:
    """Each input table must have its required columns and at least one row."""
    for table, required in contracts.items():
        df = spark.table(table)
        if missing := set(required) - set(df.columns):
            raise DataContractError(f"{table}: missing columns {sorted(missing)}")
        if df.isEmpty():
            raise DataContractError(f"{table}: is empty")


def check_output(df: DataFrame, table: str, keys: list[str], pii: list[str]) -> None:
    """Output must not expose raw PII columns, and its keys must be non-null and unique."""
    if leaked := set(pii) & set(df.columns):
        raise DataContractError(f"{table}: PII columns {sorted(leaked)} not allowed")
    if df.filter(" OR ".join(f"{k} IS NULL" for k in keys)).count():
        raise DataContractError(f"{table}: null values in key {keys}")
    if df.groupBy(*keys).count().filter("count > 1").count():
        raise DataContractError(f"{table}: duplicate rows for key {keys}")
