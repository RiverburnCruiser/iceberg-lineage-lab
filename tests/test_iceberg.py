"""Q1: Iceberg internals."""
from datetime import datetime

from jobs.positions import load_trades
from lab.contracts import current_snapshot
from lab.show import scan

D1, D2 = datetime(2026, 11, 9, 10), datetime(2026, 11, 10, 10)


def test_hidden_partitioning_prunes_by_day(spark):
    load_trades(spark, [(1, "A1", "Asha Rao", "MSFT", 10, 400.0)], trade_ts=D1)
    load_trades(spark, [(2, "B2", "Ben Li", "MSFT", 20, 400.0)], trade_ts=D2)
    days = {r.day for r in spark.sql("SELECT partition.trade_ts_day AS day FROM raw.trades.files").collect()}
    assert len(days) == 2
    read, skipped = scan(spark.table("raw.trades").where("trade_ts >= '2026-11-10'"))
    assert (read, skipped) == (1, 1)


def test_schema_evolution_rewrites_nothing_and_rollback_restores(spark):
    load_trades(spark, [(1, "A1", "Asha Rao", "MSFT", 10, 400.0)], trade_ts=D1)
    good = current_snapshot(spark, "raw.trades")
    load_trades(spark, [(99, "Z9", "Bad Feed", "MSFT", -1, 0.0)], trade_ts=D1)
    spark.sql(f"CALL system.rollback_to_snapshot('raw.trades', {good})")
    assert spark.table("raw.trades").count() == 1

    files = lambda: {r.file_path for r in spark.sql("SELECT file_path FROM raw.trades.files").collect()}
    before = files()
    spark.sql("ALTER TABLE raw.trades ADD COLUMN venue STRING")
    assert files() == before
    assert current_snapshot(spark, "raw.trades") == good       # schema change is not a snapshot
    assert spark.table("raw.trades").first().venue is None
