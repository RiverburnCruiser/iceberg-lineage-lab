"""Append-only audit log of governed runs, kept in the Iceberg table audit.job_runs.

Code only ever appends to it. `operations` lets anyone check that: an
append-only table has only `append` snapshots. In production you would also
grant writers INSERT only, in the catalog.
"""
import functools
import getpass
import json
import subprocess
from datetime import datetime
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

TABLE = "audit.job_runs"
SCHEMA = ("run_id STRING, job STRING, run_key STRING, user STRING, code_version STRING, "
          "started_at TIMESTAMP, finished_at TIMESTAMP, input_snapshots STRING, "
          "output_snapshots STRING, result STRING, error STRING")
REPO = Path(__file__).resolve().parents[1]


@functools.lru_cache
def code_version() -> str:
    """Git SHA of the code that ran, with `-dirty` if there were uncommitted changes."""
    git = lambda *args: subprocess.run(["git", *args], cwd=REPO, capture_output=True,
                                       text=True, check=True).stdout.strip()
    try:
        return git("rev-parse", "--short", "HEAD") + ("-dirty" if git("status", "--porcelain") else "")
    except (OSError, subprocess.CalledProcessError):
        return "unknown"


def ensure_table(spark: SparkSession) -> None:
    spark.sql("CREATE NAMESPACE IF NOT EXISTS audit")
    spark.sql(f"CREATE TABLE IF NOT EXISTS {TABLE} ({SCHEMA}) USING iceberg")


def record(spark: SparkSession, run_id: str, job: str, run_key: str | None,
           started_at: datetime, inputs: dict, outputs: dict, result: str,
           error: str | None = None) -> None:
    """Append one audit row. `result` is SUCCESS, FAILED or SKIPPED."""
    ensure_table(spark)
    row = (run_id, job, run_key, getpass.getuser(), code_version(), started_at, datetime.now(),
           json.dumps(inputs), json.dumps(outputs), result, error)
    spark.createDataFrame([row], SCHEMA).writeTo(TABLE).append()


def already_succeeded(spark: SparkSession, job: str, run_key: str) -> bool:
    ensure_table(spark)
    return not spark.table(TABLE).filter(
        (F.col("job") == job) & (F.col("run_key") == run_key) & (F.col("result") == "SUCCESS")
    ).isEmpty()


def operations(spark: SparkSession) -> list[str]:
    """Distinct commit operations on the audit table; ['append'] means nothing was rewritten."""
    return sorted(r.operation for r in
                  spark.sql(f"SELECT DISTINCT operation FROM {TABLE}.snapshots").collect())
