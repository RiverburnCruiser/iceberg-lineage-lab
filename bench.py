"""Benchmark the governed pipeline on synthetic trades (single machine, Spark local mode).

    python bench.py --rows 2000000 --days 30

Uses its own warehouse (bench_warehouse/), so the demos and tests are untouched.
"""
import argparse
import os
import platform
import shutil
import time
from datetime import datetime, timedelta, timezone

WAREHOUSE = "bench_warehouse"
os.environ["LINEAGE_FILE"] = f"{WAREHOUSE}/lineage/events.jsonl"   # before lab imports

from pyspark.sql import SparkSession, functions as F  # noqa: E402

from jobs.positions import build_positions, create_tables  # noqa: E402
from lab.cdc import changes_between  # noqa: E402
from lab.contracts import current_snapshot  # noqa: E402
from lab.show import scan  # noqa: E402
from lab.spark import get_spark  # noqa: E402

START = datetime(2026, 1, 1, tzinfo=timezone.utc)   # Iceberg days() partitions by UTC day
SEED, ACCOUNTS, TICKERS = 42, 5000, 200


def trades(spark: SparkSession, first_id: int, n: int, day0: int, days: int):
    """n synthetic trades with ids first_id.., trade_ts spread evenly over `days` days from day0."""
    secs = days * 86400
    i = F.col("id") - first_id
    acct = (F.rand(SEED + first_id) * ACCOUNTS).cast("int")
    return spark.range(first_id, first_id + n).select(
        F.col("id").alias("trade_id"),
        F.format_string("A%05d", acct).alias("account_id"),
        F.format_string("Client %05d", acct).alias("client_name"),
        F.format_string("T%03d", (F.rand(SEED + 1 + first_id) * TICKERS).cast("int")).alias("ticker"),
        (F.rand(SEED + 2 + first_id) * 100 + 1).cast("int").alias("qty"),
        F.round(F.rand(SEED + 3 + first_id) * 490 + 10, 2).alias("price"),
        F.timestamp_seconds(F.lit(int(START.timestamp()) + day0 * 86400)
                            + F.floor(i * secs / n)).alias("trade_ts"))


def reset_positions(spark: SparkSession) -> None:
    spark.sql("DROP TABLE IF EXISTS curated.positions PURGE")
    create_tables(spark)


def timed(fn, *args) -> float:
    t = time.perf_counter()
    fn(*args)
    return time.perf_counter() - t


def unwrapped(fn):
    while hasattr(fn, "__wrapped__"):
        fn = fn.__wrapped__
    return fn


def machine() -> str:
    cpu = next((l.split(":", 1)[1].strip() for l in open("/proc/cpuinfo") if l.startswith("model name")),
               platform.processor())
    ram = os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES") / 2**30
    return f"{cpu}, {os.cpu_count()} logical cores, {ram:.1f} GiB RAM, {platform.system()} {platform.release()}"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--rows", type=int, default=2_000_000)
    ap.add_argument("--days", type=int, default=30)
    a = ap.parse_args()
    total_t = time.perf_counter()
    shutil.rmtree(WAREHOUSE, ignore_errors=True)
    spark = get_spark(WAREHOUSE)
    spark.sparkContext.setLogLevel("ERROR")
    spark.conf.set("spark.sql.session.timeZone", "UTC")   # so a filter day == a partition day
    create_tables(spark)

    t = timed(lambda: trades(spark, 0, a.rows, 0, a.days).writeTo("raw.trades").append())
    in_rows = spark.table("raw.trades").count()
    print(f"generated + appended {in_rows} trades in {t:.2f} s")

    # 1 + 2: governed vs bare business logic, each run twice on an empty output, 2nd run kept
    inner = unwrapped(build_positions)
    runs = {}
    for name, fn in [("governed", build_positions), ("bare", inner)]:
        for i in (1, 2):
            reset_positions(spark)
            runs[name, i] = timed(fn, spark)
            print(f"{name} run {i}: {runs[name, i]:.2f} s")
    gov, bare = runs["governed", 2], runs["bare", 2]
    out_rows = spark.table("curated.positions").count()

    # 3: partition pruning with a one-day filter
    day = START + timedelta(days=a.days // 2)
    q = spark.table("raw.trades").where(f"trade_ts >= '{day:%Y-%m-%d}' AND trade_ts < '{day + timedelta(days=1):%Y-%m-%d}'")
    read, skipped = scan(q)
    files = spark.sql("SELECT count(*) AS n FROM raw.trades.files").first().n

    # 4: one more day of trades, one governed MERGE, then the changelog of that MERGE
    extra = a.rows // a.days
    trades(spark, a.rows, extra, a.days, 1).writeTo("raw.trades").append()
    before = current_snapshot(spark, "curated.positions")
    cdc_t = timed(build_positions, spark)
    after = current_snapshot(spark, "curated.positions")
    by_type = {r._change_type: r["count"] for r in changes_between(
        spark, "curated.positions", before, after, ["account_id", "ticker"])
        .groupBy("_change_type").count().collect()}
    pos_total = spark.table("curated.positions").count()

    rows = [("Input trades (raw.trades)", in_rows),
            ("Days / data files in raw.trades", f"{a.days} / {files}"),
            ("Output positions (curated.positions)", out_rows),
            ("Governed build_positions, 2nd run (s)", f"{gov:.2f}"),
            ("Governed throughput (input rows/s)", f"{in_rows / gov:,.0f}"),
            ("Bare MERGE without decorators, 2nd run (s)", f"{bare:.2f}"),
            ("Governance overhead", f"{(gov - bare) / bare * 100:.1f}%"),
            ("One-day filter: data files read / total", f"{read} / {files}"),
            ("One-day filter: skippedDataFiles metric", skipped),
            ("Extra day appended (trades)", extra),
            ("Governed incremental MERGE (s)", f"{cdc_t:.2f}"),
            *[(f"Changelog rows: {k}", by_type[k]) for k in sorted(by_type)],
            ("Rows in curated.positions after MERGE", pos_total),
            ("Total script time (s)", f"{time.perf_counter() - total_t:.2f}")]
    print("\n| Metric | Value |\n| --- | --- |")
    for k, v in rows:
        print(f"| {k} | {v} |")
    print(f"\nMachine: {machine()}")
    print(f"Spark {spark.version}, master={spark.sparkContext.master}, "
          f"shuffle.partitions={spark.conf.get('spark.sql.shuffle.partitions')}, "
          f"driver.memory={spark.sparkContext.getConf().get('spark.driver.memory', '1g (default)')}")


if __name__ == "__main__":
    main()
