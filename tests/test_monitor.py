"""Q7: metadata screenshots, alerts, incremental read vs changelog."""
import pytest

from jobs.positions import build_positions, build_positions_buggy, load_trades
from lab import metadata_monitor as mm
from lab.cdc import changes_between, incremental_appends
from lab.contracts import DataContractError, current_snapshot

DAY1 = [(1, "A1", "Asha Rao", "MSFT", 10, 400.0), (2, "B2", "Ben Li", "MSFT", 20, 400.0)]
DAY2 = [(3, "B2", "Ben Li", "MSFT", 5, 410.0), (4, "C3", "Chen Wu", "TSLA", 2, 250.0)]
TABLES = ["raw.trades", "curated.positions"]


def test_monitor_finds_drift_jumps_and_unpublished_snapshots(spark):
    load_trades(spark, DAY1)
    build_positions(spark)
    mm.capture(spark, TABLES)
    load_trades(spark, DAY2)
    build_positions(spark)                                   # MERGE through a WAP branch
    spark.sql("ALTER TABLE raw.trades ADD COLUMN venue STRING")
    with pytest.raises(DataContractError):
        build_positions_buggy(spark)
    shots = {r.table: r for r in mm.capture(spark, TABLES).collect()}

    assert shots["curated.positions"].row_count == spark.table("curated.positions").count() == 3
    alerts = mm.detect(spark, row_jump=0.5)
    assert any(a.startswith("SCHEMA DRIFT") and "raw.trades" in a for a in alerts)
    assert any(a.startswith("ROW JUMP") and "raw.trades: 2 -> 4" in a for a in alerts)
    assert not any("curated.positions" in a for a in alerts)     # 2 -> 3 is under 50%
    assert mm.unpublished(spark, "curated.positions").count() == 1
    assert mm.unpublished(spark, "raw.trades").count() == 0


def test_incremental_read_misses_merge_changelog_does_not(spark):
    load_trades(spark, DAY1)
    build_positions(spark)
    raw1, pos1 = current_snapshot(spark, "raw.trades"), current_snapshot(spark, "curated.positions")
    load_trades(spark, DAY2)
    build_positions(spark)
    raw2, pos2 = current_snapshot(spark, "raw.trades"), current_snapshot(spark, "curated.positions")

    assert incremental_appends(spark, "raw.trades", raw1, raw2).count() == 2
    assert incremental_appends(spark, "curated.positions", pos1, pos2).count() == 0
    changes = {(r.account_id, r._change_type)
               for r in changes_between(spark, "curated.positions", pos1, pos2, ["account_id", "ticker"]).collect()}
    assert changes == {("B2", "UPDATE_BEFORE"), ("B2", "UPDATE_AFTER"), ("C3", "INSERT")}
