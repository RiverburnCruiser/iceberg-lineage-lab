"""Change capture from Iceberg snapshots.

Iceberg writes a snapshot on every commit. A watcher remembers the last
snapshot it processed and, on each poll, reads only what changed since then.
"""
import json
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession


def snapshot_history(spark: SparkSession, table: str) -> DataFrame:
    return spark.sql(f"""
        SELECT committed_at, snapshot_id, parent_id, operation,
               summary['added-records'] AS added, summary['deleted-records'] AS deleted
        FROM {table}.snapshots ORDER BY committed_at""")


def incremental_appends(spark: SparkSession, table: str, start: int, end: int) -> DataFrame:
    """Rows from data files *appended* in (start, end]. Cheap, but blind to updates and
    deletes: MERGE / UPDATE / DELETE commit `overwrite` snapshots, which this read skips."""
    return (spark.read.option("start-snapshot-id", str(start))
            .option("end-snapshot-id", str(end)).table(table))


def changes_between(spark: SparkSession, table: str, start: int | None,
                    end: int, keys: list[str]) -> DataFrame:
    """Row-level changes in (start, end]: INSERT, DELETE, UPDATE_BEFORE, UPDATE_AFTER."""
    opts = f"'end-snapshot-id', '{end}'" + (f", 'start-snapshot-id', '{start}'" if start else "")
    view = table.replace(".", "_") + "_changes"
    spark.sql(f"""
        CALL system.create_changelog_view(
            table => '{table}', changelog_view => '{view}',
            options => map({opts}),
            compute_updates => true,
            identifier_columns => array({", ".join(f"'{k}'" for k in keys)}))""")
    return spark.table(view)


class SnapshotWatcher:
    """Poll a table; return the rows changed since the last poll."""

    def __init__(self, spark: SparkSession, table: str, keys: list[str],
                 checkpoint: str = "checkpoints"):
        self.spark, self.table, self.keys = spark, table, keys
        self.path = Path(checkpoint) / f"{table}.json"

    def poll(self, handle) -> bool:
        """Call handle(changes_df) if the table moved; checkpoint only after it succeeds."""
        last = json.loads(self.path.read_text())["snapshot_id"] if self.path.exists() else None
        rows = self.spark.sql(
            f"SELECT snapshot_id FROM {self.table}.refs WHERE name = 'main'").collect()
        current = rows[0].snapshot_id if rows else None
        if current is None or current == last:
            return False
        handle(changes_between(self.spark, self.table, last, current, self.keys))
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(json.dumps({"snapshot_id": current}))
        return True
