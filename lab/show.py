"""Interview demos, one per question.

    python -m lab.show q1        # or q2 ... q7, several at once, or all

Each demo starts from an empty local warehouse and prints what it shows,
where the code is, and then the live output.
"""
import functools
import inspect
import json
import logging
import os
import shutil
import sys
import textwrap
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from jobs.positions import (POSITIONS_SQL, build_positions, build_positions_buggy, create_tables,
                            erase_client, load_trades, publish_trades)
from lab import audit, lineage, metadata_monitor
from lab.cdc import SnapshotWatcher, changes_between, incremental_appends
from lab.compliance import pii_hash, rows_on_disk
from lab.contracts import DataContractError, current_snapshot
from lab.decorators import (TransientError, governed_job, layers, retry, write_audit_publish)
from lab.spark import get_spark, reset_warehouse

DAY1, DAY2 = datetime(2026, 11, 9, 10), datetime(2026, 11, 10, 10)
DAY1_TRADES = [(1, "A1", "Asha Rao", "MSFT", 10, 400.0), (2, "A1", "Asha Rao", "AAPL", 5, 200.0),
               (3, "B2", "Ben Li", "MSFT", 20, 400.0)]
DAY2_TRADES = [(4, "B2", "Ben Li", "MSFT", 5, 410.0), (5, "C3", "Chen Wu", "TSLA", 2, 250.0)]
KEYS = ["account_id", "ticker"]


@dataclass
class Demo:
    question: str
    shows: str
    code: str
    run: Callable
    needs_spark: bool


DEMOS: dict[str, Demo] = {}


def demo(key: str, question: str, shows: str, code: str, needs_spark: bool = True):
    """Register a demo function under `key` (a decorator, fittingly)."""
    def register(fn):
        DEMOS[key] = Demo(question, shows, code, fn, needs_spark)
        return fn
    return register


def step(title: str) -> None:
    print(f"\n--- {title}")


def run_pipeline(spark: SparkSession) -> None:
    """Two days of trades, three governed jobs, one buggy job that WAP blocks."""
    load_trades(spark, DAY1_TRADES, trade_ts=DAY1)
    build_positions(spark)
    publish_trades(spark, run_key="2026-11-09")
    load_trades(spark, DAY2_TRADES, trade_ts=DAY2)
    build_positions(spark)
    try:
        build_positions_buggy(spark)
    except DataContractError:
        pass


def scan(df: DataFrame) -> tuple[int, int]:
    """Run the query; return Iceberg's (data files read, data files skipped) scan metrics."""
    df.collect()
    nodes, found = [df._jdf.queryExecution().executedPlan()], (0, 0)
    while nodes:
        node = nodes.pop()
        if node.nodeName().startswith("BatchScan"):
            m = node.metrics()
            found = (m.apply("resultDataFiles").value(), m.apply("skippedDataFiles").value())
        children = node.children().iterator()
        while children.hasNext():
            nodes.append(children.next())
    return found


# ---------------------------------------------------------------- Q1
@demo("q1", "Do you know Iceberg?",
      "Iceberg is a table format: metadata.json -> snapshots -> manifests -> data files with "
      "column stats. That is what gives atomic commits, time travel, schema evolution without "
      "rewriting data, and hidden partitioning.",
      "jobs/positions.py:create_tables, Iceberg metadata tables (<table>.snapshots, .files, ...)")
def q1(spark: SparkSession) -> None:
    load_trades(spark, DAY1_TRADES, trade_ts=DAY1)
    load_trades(spark, DAY2_TRADES, trade_ts=DAY2)
    step("metadata.json: the one file the catalog points to; everything else hangs off it")
    print(spark.sql("SELECT file FROM raw.trades.metadata_log_entries "
                    "ORDER BY timestamp DESC").first().file)
    step("snapshots: one per commit")
    spark.sql("SELECT committed_at, snapshot_id, parent_id, operation, "
              "summary['added-records'] AS added FROM raw.trades.snapshots").show(truncate=False)
    step("manifests: lists of data files, with the partition range they cover")
    spark.sql("SELECT added_snapshot_id, added_data_files_count AS files, partition_summaries "
              "FROM raw.trades.manifests").show(truncate=False)
    step("data files: row count and per-column min/max, used to skip files at query time")
    spark.sql("SELECT partition.trade_ts_day AS day, record_count AS rows, "
              "readable_metrics.qty.lower_bound AS qty_min, readable_metrics.qty.upper_bound AS qty_max, "
              "readable_metrics.ticker.lower_bound AS ticker_min, "
              "readable_metrics.ticker.upper_bound AS ticker_max FROM raw.trades.files").show()
    step("hidden partitioning: PARTITIONED BY days(trade_ts); queries filter trade_ts itself")
    total = spark.table("raw.trades.files").count()
    read, skipped = scan(spark.table("raw.trades").where("trade_ts >= '2026-11-10'"))
    print(f"WHERE trade_ts >= '2026-11-10'  ->  read {read} of {total} data files "
          f"(Iceberg scan metrics: resultDataFiles={read}, skippedDataFiles={skipped})")
    step("rollback: undo a bad load by moving main back; no data is copied")
    good = current_snapshot(spark, "raw.trades")
    load_trades(spark, [(99, "Z9", "Bad Feed", "MSFT", -1_000_000, 0.0)], trade_ts=DAY2)
    print("rows after bad load:", spark.table("raw.trades").count())
    spark.sql(f"CALL system.rollback_to_snapshot('raw.trades', {good})")
    print("rows after rollback_to_snapshot:", spark.table("raw.trades").count())
    spark.sql("SELECT made_current_at, snapshot_id, is_current_ancestor "
              "FROM raw.trades.history").show(truncate=False)
    step("schema evolution: ADD COLUMN only writes new metadata")
    files = lambda: {r.file_path for r in spark.sql("SELECT file_path FROM raw.trades.files").collect()}
    before, snaps = files(), spark.table("raw.trades.snapshots").count()
    spark.sql("ALTER TABLE raw.trades ADD COLUMN venue STRING")
    print(f"data files rewritten: {len(before ^ files())}, new snapshots: "
          f"{spark.table('raw.trades.snapshots').count() - snaps}; old rows read venue as NULL:")
    spark.sql("SELECT trade_id, ticker, venue FROM raw.trades ORDER BY trade_id LIMIT 3").show()


# ---------------------------------------------------------------- Q2
@demo("q2", "Do you know data lineage?",
      "Lineage answers 'where did this data come from and what did it feed?'. Every governed run "
      "here emits OpenLineage events holding the exact Iceberg snapshot of each input and "
      "output, so any table version can be traced and rebuilt.",
      "lab/lineage.py:emit, provenance, downstream; lab/decorators.py:emit_lineage")
def q2(spark: SparkSession) -> None:
    run_pipeline(spark)
    load_trades(spark, [(6, "C3", "Chen Wu", "TSLA", 1, 260.0)], trade_ts=DAY2)  # raw moves on
    events = lineage.read_events()
    step("lineage/events.jsonl: a START and a COMPLETE or FAIL event per run")
    for e in events:
        snaps = lambda ds: ", ".join(f"{d['name']}@{lineage.snapshot_of(d)}" for d in ds)
        print(f"{e['eventType']:8} {e['job']['name']:21} in: {snaps(e['inputs'])}  "
              f"out: {snaps(e['outputs'])}  {lineage.error_of(e)[:45]}")
    snap = current_snapshot(spark, "curated.positions")
    step(f"provenance: which run built curated.positions@{snap}, and from what?")
    p = lineage.provenance(events, "curated.positions", snap)
    for k, v in p.items():
        print(f"  {k:13} {v}")
    step("reproduce: rerun the job's SQL on the exact input snapshot (time travel)")
    src = p["inputs"]["raw.trades"]
    print(f"raw.trades is now at {current_snapshot(spark, 'raw.trades')}, the run read {src}")
    replay = spark.sql(POSITIONS_SQL.replace("FROM raw.trades", f"FROM raw.trades VERSION AS OF {src}"))
    published = spark.table("curated.positions")
    print("replay == published:", replay.exceptAll(published).isEmpty()
          and published.exceptAll(replay).isEmpty())
    step("impact analysis: what is downstream of raw.trades?")
    print(lineage.downstream(events, "raw.trades"))


# ---------------------------------------------------------------- Q3
@demo("q3", "How would you implement data lineage?",
      "Three parts: (1) the platform decorator emits OpenLineage events, so job authors write no "
      "lineage code; (2) events go to an append-only store (JSONL here, Marquez if MARQUEZ_URL is "
      "set); (3) a graph is built from the events, with snapshot IDs on the edges.",
      "lab/decorators.py:emit_lineage, lab/lineage.py:emit, post_to_marquez, to_mermaid")
def q3(spark: SparkSession) -> None:
    run_pipeline(spark)
    events = lineage.read_events()
    step("1. capture: one COMPLETE event as written by @emit_lineage (_producer keys hidden)")
    strip = lambda o: ({k: strip(v) for k, v in o.items() if not k.startswith("_")}
                       if isinstance(o, dict) else [strip(x) for x in o] if isinstance(o, list) else o)
    for k, v in strip(next(e for e in events if e["eventType"] == "COMPLETE")).items():
        print(f"  {k:9} {json.dumps(v)}")
    step("2. graph: lab.lineage.write_graph -> Mermaid (renders on GitHub)")
    path = lineage.write_graph()
    print(f"wrote {path}\n")
    print(lineage.to_mermaid(events))
    step("3. backend")
    if url := os.getenv("MARQUEZ_URL"):
        print(f"each event was also POSTed to {url}/api/v1/lineage; UI: http://localhost:3000")
    else:
        print("MARQUEZ_URL not set, so events stayed local. To see the Marquez UI:\n"
              "  docker compose up -d && MARQUEZ_URL=http://localhost:5000 python -m lab.show q3")


# ---------------------------------------------------------------- Q4
@demo("q4", "How would you ensure data integrity and compliance while automating Spark jobs?",
      "Every automated run passes the same gates: input contracts, Write-Audit-Publish, PII "
      "pseudonymisation, an append-only audit log, idempotent reruns, and GDPR erasure that "
      "really deletes (snapshot expiry).",
      "lab/decorators.py:governed_job, lab/contracts.py, lab/audit.py, lab/compliance.py")
def q4(spark: SparkSession) -> None:
    step("1. input contract: a job needs a column raw.trades does not have")

    @governed_job(inputs={"raw.trades": ["account_id", "currency"]},
                  output="curated.positions", keys=KEYS)
    def positions_in_usd(spark):
        raise AssertionError("never runs")
    try:
        positions_in_usd(spark)
    except DataContractError as e:
        print("REJECTED before any write:", e)
    load_trades(spark, DAY1_TRADES, trade_ts=DAY1)
    build_positions(spark)
    step("2. Write-Audit-Publish: a buggy job writes duplicates to its branch")
    before = current_snapshot(spark, "curated.positions")
    try:
        build_positions_buggy(spark)
    except DataContractError as e:
        print("BLOCKED:", e)
    print("main unchanged:", before == current_snapshot(spark, "curated.positions"))
    step("3. PII: curated.trades holds a salted SHA-256 of client_name, never the name")
    publish_trades(spark, run_key="2026-11-09")
    spark.table("curated.trades").select("trade_id", "account_id", "client_name_sha256").show(truncate=False)
    step("4. idempotent rerun: the scheduler retries run_key=2026-11-09")
    publish_trades(spark, run_key="2026-11-09")
    print("curated.trades rows:", spark.table("curated.trades").count(), "(still 3)")
    step("5. audit log: audit.job_runs, append-only")
    spark.table("audit.job_runs").select("job", "run_key", "user", "code_version",
                                         "output_snapshots", "result").orderBy("started_at") \
        .show(truncate=False)
    print("commit operations on audit.job_runs:", audit.operations(spark))
    step("6. GDPR erasure of Asha Rao: delete the rows, then expire older snapshots")
    old = current_snapshot(spark, "raw.trades")
    on_disk = lambda: (rows_on_disk(spark, "raw.trades", F.col("client_name") == "Asha Rao"),
                       rows_on_disk(spark, "curated.trades",
                                    F.col("client_name_sha256") == pii_hash("Asha Rao")))
    print("Asha's rows in data files on disk (raw, curated):", on_disk())
    print("data files deleted by expire_snapshots:", erase_client(spark, client_name="Asha Rao"))
    print("Asha's rows in data files on disk (raw, curated):", on_disk())
    try:
        spark.sql(f"SELECT * FROM raw.trades VERSION AS OF {old}").collect()
    except Exception as e:
        print("time travel to the pre-erasure snapshot:", str(e).splitlines()[0][:90])


# ---------------------------------------------------------------- Q5
@demo("q5", "Do you know decorators?",
      "A decorator is a function that takes a function and returns a new one, so behaviour can be "
      "added around code without touching it. functools.wraps keeps the original name and "
      "docstring. Stacking order matters: @a @b f means a(b(f)).",
      "lab/decorators.py:_layer, layers; lab/show.py:demo (the demo registry is a decorator too)",
      needs_spark=False)
def q5(spark: SparkSession | None = None) -> None:
    def logged(fn):
        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            print(f"  -> enter {wrapper.label}")
            result = fn(*args, **kwargs)
            print(f"  <- exit  {wrapper.label}")
            return result
        wrapper.label = "logged"
        return wrapper

    def tagged(tag: str):                      # a decorator factory: takes arguments
        def decorator(fn):
            @functools.wraps(fn)
            def wrapper(*args, **kwargs):
                print(f"  -> enter tagged({tag!r})")
                return fn(*args, **kwargs)
            return wrapper
        return decorator

    @logged
    @tagged("pricing")
    def market_value(qty: int, price: float) -> float:
        """qty * price."""
        print("  .. market_value runs")
        return qty * price

    step("@logged @tagged('pricing') def market_value  ==  logged(tagged('pricing')(market_value))")
    print("result:", market_value(10, 400.0))
    step("functools.wraps keeps the identity of the wrapped function")
    print(f"__name__={market_value.__name__!r}  __doc__={market_value.__doc__!r}  "
          f"__wrapped__ -> {market_value.__wrapped__.__name__}")
    step("the real stack on jobs.positions.build_positions, outermost first (via __wrapped__)")
    for i, name in enumerate(layers(build_positions)):
        print(f"  {'  ' * i}{name}")


# ---------------------------------------------------------------- Q6
@demo("q6", "How would you use decorators for data integrity and encapsulation in automation?",
      "Each concern is one small decorator; @governed_job stacks them. Job authors write business "
      "logic only: no credentials, branch names, lineage or audit code. Order is a design "
      "decision: retry must sit outside Write-Audit-Publish, and retries only transient errors.",
      "lab/decorators.py:retry, write_audit_publish, governed_job; jobs/positions.py")
def q6(spark: SparkSession) -> None:
    step("1. what a job author writes (jobs/positions.py)")
    print(inspect.getsource(publish_trades).split('    """')[0].rstrip())
    print("    ...  # two lines of business logic; no credentials, branches, lineage or audit code")
    load_trades(spark, DAY1_TRADES, trade_ts=DAY1)
    wap = write_audit_publish("curated.positions", keys=KEYS)

    def flaky():
        calls = []

        def flaky_positions(spark):
            """Appends positions, then fails transiently on its first attempt."""
            spark.sql(f"INSERT INTO curated.positions {POSITIONS_SQL}")
            calls.append(1)
            if len(calls) == 1:
                raise TransientError("executor lost after writing")
        return flaky_positions

    step("2. retry INSIDE write_audit_publish: wap(retry(job)), both attempts share one branch")
    try:
        wap(retry(times=2)(flaky()))(spark)
    except DataContractError as e:
        print("BLOCKED:", e, "<- attempt 1's partial write was still on the branch")
    step("3. retry OUTSIDE write_audit_publish: retry(wap(job)), each attempt gets a fresh branch")
    retry(times=2)(wap(flaky()))(spark)
    print("rows on main:", spark.table("curated.positions").count(), "(3 positions, no duplicates)")
    step("4. retry(on=(TransientError,)) does not retry a broken contract")
    attempts = []

    @retry(times=3)
    @write_audit_publish("curated.positions", keys=KEYS)
    def duplicates(spark):
        attempts.append(1)
        spark.sql(f"INSERT INTO curated.positions {POSITIONS_SQL}")
    try:
        duplicates(spark)
    except DataContractError as e:
        print(f"failed after {len(attempts)} attempt(s): {e}")


# ---------------------------------------------------------------- Q7
@demo("q7", "How do you capture changes in a data pipeline?",
      "Take periodic 'screenshots' of each table's Iceberg metadata into "
      "audit.table_metadata_history and compare them: schema drift, row-count jumps, unpublished "
      "snapshots. For row-level changes, read the changelog view between two snapshots.",
      "lab/metadata_monitor.py:capture, detect, unpublished; lab/cdc.py:changes_between, SnapshotWatcher")
def q7(spark: SparkSession) -> None:
    tables = ["raw.trades", "curated.positions"]
    load_trades(spark, DAY1_TRADES, trade_ts=DAY1)
    build_positions(spark)
    metadata_monitor.capture(spark, tables)                               # screenshot 1
    raw1, pos1 = current_snapshot(spark, "raw.trades"), current_snapshot(spark, "curated.positions")
    load_trades(spark, DAY2_TRADES, trade_ts=DAY2)
    build_positions(spark)
    metadata_monitor.capture(spark, tables)                               # screenshot 2
    spark.sql("ALTER TABLE raw.trades ADD COLUMN venue STRING")
    try:
        build_positions_buggy(spark)
    except DataContractError:
        pass
    metadata_monitor.capture(spark, tables)                               # screenshot 3
    step("audit.table_metadata_history: three screenshots of two tables")
    spark.table(metadata_monitor.TABLE).orderBy("captured_at", "table").show(truncate=False)
    step("alerts from comparing consecutive screenshots")
    print("\n".join(metadata_monitor.detect(spark, row_jump=0.5)))
    step("snapshots never published to main")
    functools.reduce(DataFrame.unionByName,
                     [metadata_monitor.unpublished(spark, t) for t in tables]).show(truncate=False)
    raw2, pos2 = current_snapshot(spark, "raw.trades"), current_snapshot(spark, "curated.positions")
    step("incremental read (start-snapshot-id) vs changelog view, day 1 -> day 2")
    print("raw.trades (append only), incremental read rows:", incremental_appends(spark, "raw.trades", raw1, raw2).count())
    print("curated.positions (MERGE), incremental read rows:",
          incremental_appends(spark, "curated.positions", pos1, pos2).count())
    changes_between(spark, "curated.positions", pos1, pos2, KEYS).orderBy("account_id", "_change_type").show()
    print("Why: MERGE rewrites data files and commits an 'overwrite' snapshot. An incremental read only\n"
          "returns rows from appended files, so it silently misses updates and deletes. The changelog\n"
          "view compares removed and added files and returns INSERT / DELETE / UPDATE_BEFORE / UPDATE_AFTER.")
    step("SnapshotWatcher: the same changelog, polled, with a checkpoint written after success")
    watcher = SnapshotWatcher(spark, "curated.positions", KEYS)
    print("first poll handled changes:", watcher.poll(lambda df: print("  rows:", df.count())))
    print("second poll handled changes:", watcher.poll(lambda df: None), "(nothing new)")


# ---------------------------------------------------------------- runner
_SPARK: SparkSession | None = None


def fresh_spark() -> SparkSession:
    """A Spark session over an empty warehouse, with the example tables created."""
    global _SPARK
    for d in ("checkpoints", "lineage"):
        shutil.rmtree(d, ignore_errors=True)
    if _SPARK is None:
        shutil.rmtree("warehouse", ignore_errors=True)
        _SPARK = get_spark()
        _SPARK.sparkContext.setLogLevel("ERROR")
    else:
        reset_warehouse(_SPARK)
    create_tables(_SPARK)
    return _SPARK


def run(key: str) -> None:
    d = DEMOS[key]
    bar = "=" * 96
    print(f"\n{bar}\n{key.upper()}  {d.question}")
    print(textwrap.fill(d.shows, 92, initial_indent="    ", subsequent_indent="    "))
    print(f"    code: {d.code}\n{bar}")
    d.run(fresh_spark() if d.needs_spark else None)


def main(argv: list[str]) -> int:
    keys = list(DEMOS) if argv == ["all"] else argv
    if not keys or any(k not in DEMOS for k in keys):
        print(f"usage: python -m lab.show {'|'.join(DEMOS)}|all [more ...]")
        return 2
    logging.basicConfig(format="    [%(name)s] %(message)s", stream=sys.stdout)
    logging.getLogger("lab").setLevel(logging.INFO)
    for k in keys:
        run(k)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
