"""Periodic "screenshots" of Iceberg table metadata, kept in audit.table_metadata_history.

Each screenshot is one row per table: the snapshot `main` points to, its
operation, row / file / byte totals (from the snapshot summary, so no data is
scanned), and a hash of the schema. Comparing consecutive screenshots finds:

  * schema drift      schema_hash changed (ALTER TABLE makes no snapshot, so
                      watching snapshot IDs alone would miss it)
  * row-count jumps   |rows - previous| / previous above a threshold
  * unpublished work  snapshots that exist but were never on main, such as a
                      WAP branch whose audit failed

Run it on a schedule:  python -m lab.metadata_monitor --every 60 raw.trades curated.positions
"""
import argparse
import hashlib
import time
from datetime import datetime

from pyspark.sql import DataFrame, SparkSession

TABLE = "audit.table_metadata_history"
SCHEMA = ("captured_at TIMESTAMP, `table` STRING, snapshot_id BIGINT, operation STRING, "
          "row_count BIGINT, file_count BIGINT, total_size_bytes BIGINT, schema_hash STRING")


def schema_hash(spark: SparkSession, table: str) -> str:
    return hashlib.sha256(spark.table(table).schema.json().encode()).hexdigest()[:12]


def screenshot(spark: SparkSession, table: str) -> tuple:
    """One history row describing the current state of `table`'s main branch."""
    row = spark.sql(f"""
        SELECT s.snapshot_id, s.operation, s.summary FROM {table}.snapshots s
        JOIN {table}.refs r ON s.snapshot_id = r.snapshot_id WHERE r.name = 'main'""").first()
    summary = (row.summary if row else None) or {}
    total = lambda key: int(summary.get(key, 0))
    return (datetime.now(), table, row and row.snapshot_id, row and row.operation,
            total("total-records"), total("total-data-files"), total("total-files-size"),
            schema_hash(spark, table))


def capture(spark: SparkSession, tables: list[str]) -> DataFrame:
    """Take a screenshot of every table and append it to the history table."""
    spark.sql("CREATE NAMESPACE IF NOT EXISTS audit")
    spark.sql(f"CREATE TABLE IF NOT EXISTS {TABLE} ({SCHEMA}) USING iceberg")
    shots = spark.createDataFrame([screenshot(spark, t) for t in tables], SCHEMA)
    shots.writeTo(TABLE).append()
    return shots


def detect(spark: SparkSession, row_jump: float = 0.5,
           since: datetime | None = None) -> list[str]:
    """Alerts from comparing each screenshot (taken at or after `since`) with the
    previous screenshot of the same table."""
    rows = spark.sql(f"""
        SELECT `table`, captured_at, snapshot_id, row_count, schema_hash,
               LAG(row_count) OVER w AS prev_rows, LAG(schema_hash) OVER w AS prev_hash
        FROM {TABLE} WINDOW w AS (PARTITION BY `table` ORDER BY captured_at)
        ORDER BY captured_at, `table`""").collect()
    alerts = []
    for r in rows:
        if since and r.captured_at < since:
            continue
        at = f"{r.captured_at:%H:%M:%S} {r.table}"
        if r.prev_hash and r.schema_hash != r.prev_hash:
            alerts.append(f"SCHEMA DRIFT  {at}: schema {r.prev_hash} -> {r.schema_hash} "
                          f"(snapshot unchanged: {r.snapshot_id})")
        if r.prev_rows and abs(r.row_count - r.prev_rows) / r.prev_rows > row_jump:
            alerts.append(f"ROW JUMP      {at}: {r.prev_rows} -> {r.row_count} rows "
                          f"(> {row_jump:.0%} change)")
    return alerts


def unpublished(spark: SparkSession, table: str) -> DataFrame:
    """Snapshots that were never the current state of main (e.g. a rejected WAP branch)."""
    return spark.sql(f"""
        SELECT '{table}' AS `table`, snapshot_id, committed_at, operation,
               summary['added-records'] AS added_records
        FROM {table}.snapshots
        WHERE snapshot_id NOT IN (SELECT snapshot_id FROM {table}.history)""")


def monitor(spark: SparkSession, tables: list[str], every_s: int, times: int | None = None) -> None:
    """Capture, alert, sleep; repeat `times` times (forever if None)."""
    n = 0
    while True:
        started = datetime.now()
        capture(spark, tables)
        for alert in detect(spark, since=started):
            print(alert)
        n += 1
        if times is not None and n >= times:
            return
        time.sleep(every_s)


if __name__ == "__main__":
    from .spark import get_spark
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("tables", nargs="+")
    p.add_argument("--every", type=int, default=60, help="seconds between screenshots")
    p.add_argument("--times", type=int, default=None, help="stop after N screenshots")
    a = p.parse_args()
    monitor(get_spark(), a.tables, a.every, a.times)
