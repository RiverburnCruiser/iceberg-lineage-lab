import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    from lab.spark import get_spark
    s = get_spark(str(tmp_path_factory.mktemp("warehouse")))
    s.sparkContext.setLogLevel("ERROR")
    yield s
    s.stop()


@pytest.fixture(autouse=True)
def fresh_tables(spark, tmp_path, monkeypatch):
    import lab.lineage
    from jobs.positions import create_tables
    from lab.spark import reset_warehouse
    monkeypatch.setattr(lab.lineage, "LINEAGE_FILE", tmp_path / "events.jsonl")
    monkeypatch.delenv("MARQUEZ_URL", raising=False)
    reset_warehouse(spark)
    create_tables(spark)
