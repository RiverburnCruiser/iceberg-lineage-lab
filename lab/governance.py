"""@governed_job: the single door every Spark job goes through.

Before the job:  input data contracts (required columns, not empty).
During the job:  writes land on a private Iceberg branch (Write-Audit-Publish).
After the job:   output checks (no PII, keys not null, keys unique), then
                 fast-forward `main` to the branch. Readers never see bad data.
Every run:       OpenLineage-style START / COMPLETE / FAIL events with the
                 Iceberg snapshot ID of every input and output.
"""
import functools
import uuid

from pyspark.sql import DataFrame, SparkSession

from . import lineage


class DataContractError(Exception):
    pass


def current_snapshot(spark: SparkSession, table: str, ref: str = "main") -> int | None:
    rows = spark.sql(f"SELECT snapshot_id FROM {table}.refs WHERE name = '{ref}'").collect()
    return rows[0].snapshot_id if rows else None


def check_inputs(spark: SparkSession, contracts: dict[str, list[str]]) -> None:
    for table, required in contracts.items():
        df = spark.table(table)
        if missing := set(required) - set(df.columns):
            raise DataContractError(f"{table}: missing columns {sorted(missing)}")
        if df.isEmpty():
            raise DataContractError(f"{table}: is empty")


def check_output(df: DataFrame, table: str, keys: list[str], pii: list[str]) -> None:
    if leaked := set(pii) & set(df.columns):
        raise DataContractError(f"{table}: PII columns {sorted(leaked)} not allowed")
    if df.filter(" OR ".join(f"{k} IS NULL" for k in keys)).count():
        raise DataContractError(f"{table}: null values in key {keys}")
    if df.groupBy(*keys).count().filter("count > 1").count():
        raise DataContractError(f"{table}: duplicate rows for key {keys}")


def governed_job(*, inputs: dict[str, list[str]], output: str,
                 keys: list[str], pii: list[str] = ()):
    def decorator(job):
        @functools.wraps(job)
        def wrapper(spark: SparkSession, **params):
            run_id = str(uuid.uuid4())
            branch = f"wap_{run_id[:8]}"
            ins = [lineage.dataset(t, current_snapshot(spark, t)) for t in inputs]
            outs = [lineage.dataset(output, current_snapshot(spark, output))]
            lineage.emit("START", run_id, job.__name__, ins, outs, params=params)
            try:
                check_inputs(spark, inputs)
                spark.conf.set("spark.wap.branch", branch)    # write: to the branch
                try:
                    result = job(spark, **params)
                finally:
                    spark.conf.unset("spark.wap.branch")
                check_output(spark.table(f"{output}.branch_{branch}"),   # audit
                             output, keys, list(pii))
                spark.sql(f"CALL system.fast_forward('{output}', 'main', '{branch}')")  # publish
            except Exception as e:
                lineage.emit("FAIL", run_id, job.__name__, ins, outs, error=str(e))
                raise
            finally:
                spark.sql(f"ALTER TABLE {output} DROP BRANCH IF EXISTS {branch}")
            outs = [lineage.dataset(output, current_snapshot(spark, output))]
            lineage.emit("COMPLETE", run_id, job.__name__, ins, outs)
            return result
        return wrapper
    return decorator
