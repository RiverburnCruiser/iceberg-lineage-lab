"""Q5 / Q6: composable decorators and why their order matters."""
import pytest

from jobs.positions import POSITIONS_SQL, build_positions, load_trades
from lab.contracts import DataContractError, current_snapshot
from lab.decorators import TransientError, layers, retry, write_audit_publish

KEYS = ["account_id", "ticker"]


def test_wraps_keeps_identity_and_layers_are_in_order():
    assert build_positions.__name__ == "build_positions"
    assert [l.split("(")[0].split(" ")[0] for l in layers(build_positions)] == [
        "audited", "emit_lineage", "validate_inputs", "write_audit_publish", "build_positions"]


def test_retry_only_retries_listed_exceptions():
    calls = []

    @retry(times=3, on=(TransientError,))
    def job():
        calls.append(1)
        raise (TransientError if len(calls) < 3 else ValueError)("boom")

    with pytest.raises(ValueError):
        job()
    assert len(calls) == 3

    calls.clear()
    with pytest.raises(DataContractError):
        retry(times=3)(lambda: calls.append(1) or (_ for _ in ()).throw(DataContractError("x")))()
    assert len(calls) == 1


def test_retry_must_sit_outside_write_audit_publish(spark):
    load_trades(spark, [(1, "A1", "Asha Rao", "MSFT", 10, 400.0)])
    wap = write_audit_publish("curated.positions", keys=KEYS)

    def flaky():
        calls = []

        def job(spark):
            spark.sql(f"INSERT INTO curated.positions {POSITIONS_SQL}")
            calls.append(1)
            if len(calls) == 1:
                raise TransientError("lost executor")
        return job

    with pytest.raises(DataContractError, match="duplicate"):     # retry inside: same branch
        wap(retry(times=2)(flaky()))(spark)
    assert current_snapshot(spark, "curated.positions") is None

    retry(times=2)(wap(flaky()))(spark)                            # retry outside: fresh branch
    assert spark.table("curated.positions").count() == 1
    assert [r.name for r in spark.sql("SELECT name FROM curated.positions.refs").collect()] == ["main"]
