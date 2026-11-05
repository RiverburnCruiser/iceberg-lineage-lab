import json

import pytest

import lab.lineage
from jobs.positions import build_positions, build_positions_buggy, load_trades
from lab.cdc import SnapshotWatcher
from lab.governance import DataContractError, check_output, current_snapshot

DAY1 = [(1, "A1", "Asha Rao", "MSFT", 10, 400.0), (2, "B2", "Ben Li", "MSFT", 20, 400.0)]


def events():
    return [json.loads(l) for l in lab.lineage.LINEAGE_FILE.read_text().splitlines()]


def test_governed_job_publishes_and_emits_lineage(spark):
    load_trades(spark, DAY1)
    build_positions(spark)
    assert spark.table("curated.positions").count() == 2
    start, complete = events()
    assert (start["eventType"], complete["eventType"]) == ("START", "COMPLETE")
    assert start["run"]["runId"] == complete["run"]["runId"]
    out_snap = complete["outputs"][0]["facets"]["icebergSnapshot"]["snapshotId"]
    assert out_snap == current_snapshot(spark, "curated.positions")


def test_empty_input_is_rejected_before_any_write(spark):
    with pytest.raises(DataContractError, match="is empty"):
        build_positions(spark)
    assert current_snapshot(spark, "curated.positions") is None
    assert events()[-1]["eventType"] == "FAIL"


def test_bad_output_never_reaches_main(spark):
    load_trades(spark, DAY1)
    build_positions(spark)
    before = current_snapshot(spark, "curated.positions")
    with pytest.raises(DataContractError, match="duplicate"):
        build_positions_buggy(spark)
    assert current_snapshot(spark, "curated.positions") == before
    assert spark.table("curated.positions").count() == 2


def test_pii_column_is_blocked(spark):
    df = spark.createDataFrame([("A1", "Asha Rao")], ["account_id", "client_name"])
    with pytest.raises(DataContractError, match="PII"):
        check_output(df, "t", keys=["account_id"], pii=["client_name"])


def test_watcher_returns_only_new_changes(spark, tmp_path):
    watcher = SnapshotWatcher(spark, "curated.positions", ["account_id", "ticker"], str(tmp_path))
    seen = []
    handle = lambda df: seen.append(sorted((r.account_id, r._change_type) for r in df.collect()))

    load_trades(spark, DAY1)
    build_positions(spark)
    assert watcher.poll(handle)
    assert not watcher.poll(handle)          # nothing new

    load_trades(spark, [(3, "B2", "Ben Li", "MSFT", 5, 410.0)])
    build_positions(spark)
    assert watcher.poll(handle)
    assert seen[-1] == [("B2", "UPDATE_AFTER"), ("B2", "UPDATE_BEFORE")]
