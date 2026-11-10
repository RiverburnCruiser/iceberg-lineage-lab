"""Q4: PII, audit log, idempotent reruns, erasure."""
from datetime import datetime

import pytest
from pyspark.sql import functions as F

from jobs.positions import erase_client, load_trades, publish_trades
from lab import audit
from lab.compliance import pii_hash, rows_on_disk
from lab.contracts import current_snapshot

D1 = datetime(2026, 11, 9, 10)
TRADES = [(1, "A1", "Asha Rao", "MSFT", 10, 400.0), (2, "B2", "Ben Li", "MSFT", 20, 400.0)]


def test_pii_is_hashed_audit_is_append_only_and_rerun_is_idempotent(spark):
    load_trades(spark, TRADES, trade_ts=D1)
    publish_trades(spark, run_key="2026-11-09")
    publish_trades(spark, run_key="2026-11-09")                  # scheduler retry

    curated = spark.table("curated.trades")
    assert curated.count() == 2
    assert "client_name" not in curated.columns
    assert {r.client_name_sha256 for r in curated.collect()} == {pii_hash("Asha Rao"), pii_hash("Ben Li")}

    runs = spark.table(audit.TABLE).orderBy("started_at").collect()
    assert [r.result for r in runs] == ["SUCCESS", "SKIPPED"]
    assert runs[0].code_version != "unknown" and runs[0].user
    assert str(current_snapshot(spark, "curated.trades")) in runs[0].output_snapshots
    assert audit.operations(spark) == ["append"]


def test_erasure_removes_subject_from_disk_and_time_travel(spark):
    load_trades(spark, TRADES, trade_ts=D1)
    publish_trades(spark, run_key="2026-11-09")
    old = current_snapshot(spark, "raw.trades")
    asha = F.col("client_name") == "Asha Rao"
    assert rows_on_disk(spark, "raw.trades", asha) == 1

    deleted = erase_client(spark, client_name="Asha Rao")

    assert deleted == {"raw.trades": 1, "curated.trades": 1}
    assert rows_on_disk(spark, "raw.trades", asha) == 0
    assert rows_on_disk(spark, "curated.trades", F.col("client_name_sha256") == pii_hash("Asha Rao")) == 0
    assert spark.table("raw.trades").count() == 1                 # Ben Li stays
    with pytest.raises(Exception, match="Cannot find snapshot"):
        spark.sql(f"SELECT * FROM raw.trades VERSION AS OF {old}").collect()
