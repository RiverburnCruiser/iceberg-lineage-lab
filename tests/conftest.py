import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

NAMESPACES = ("raw", "curated", "audit")


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    from lab.spark import get_spark
    s = get_spark(str(tmp_path_factory.mktemp("warehouse")))
    s.sparkContext.setLogLevel("ERROR")
    yield s
    s.stop()


def drop_all_tables(spark) -> None:
    for ns in NAMESPACES:
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {ns}")
        for t in spark.sql(f"SHOW TABLES IN {ns}").collect():
            spark.sql(f"DROP TABLE IF EXISTS {ns}.{t.tableName} PURGE")


@pytest.fixture(autouse=True)
def fresh_tables(spark, tmp_path, monkeypatch):
    import lab.lineage
    from jobs.positions import create_tables
    monkeypatch.setattr(lab.lineage, "LINEAGE_FILE", tmp_path / "events.jsonl")
    monkeypatch.delenv("MARQUEZ_URL", raising=False)
    drop_all_tables(spark)
    create_tables(spark)
