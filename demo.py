"""End-to-end demo. Run: python demo.py  (per-question demos: python -m lab.show all)"""
import shutil

from lab.cdc import SnapshotWatcher, snapshot_history
from lab.governance import DataContractError, current_snapshot
from lab.lineage import error_of, read_events, snapshot_of
from lab.spark import get_spark
from jobs.positions import build_positions, build_positions_buggy, create_tables, load_trades

for d in ("warehouse", "checkpoints", "lineage"):
    shutil.rmtree(d, ignore_errors=True)

spark = get_spark()
spark.sparkContext.setLogLevel("ERROR")
create_tables(spark)
watcher = SnapshotWatcher(spark, "curated.positions", keys=["account_id", "ticker"])
show_changes = lambda df: df.orderBy("account_id", "ticker", "_change_type").show()


def step(title):
    print(f"\n=== {title} ===")


step("1. Day 1 trades -> governed job -> positions")
load_trades(spark, [(1, "A1", "Asha Rao", "MSFT", 10, 400.0),
                    (2, "A1", "Asha Rao", "AAPL", 5, 200.0),
                    (3, "B2", "Ben Li", "MSFT", 20, 400.0)])
build_positions(spark)
spark.table("curated.positions").orderBy("account_id", "ticker").show()
day1 = current_snapshot(spark, "curated.positions")

step("2. Change capture: everything since the last checkpoint")
watcher.poll(show_changes)

step("3. Day 2 trades (B2 buys more MSFT, C3 is new) -> only the changes come back")
load_trades(spark, [(4, "B2", "Ben Li", "MSFT", 5, 410.0),
                    (5, "C3", "Chen Wu", "TSLA", 2, 250.0)])
build_positions(spark)
watcher.poll(show_changes)

step("4. A buggy job is blocked by Write-Audit-Publish; main does not move")
before = current_snapshot(spark, "curated.positions")
try:
    build_positions_buggy(spark)
except DataContractError as e:
    print("BLOCKED:", e)
print("main unchanged:", before == current_snapshot(spark, "curated.positions"))

step("5. Snapshot history (the table's 'screenshots')")
snapshot_history(spark, "curated.positions").show(truncate=False)

step("6. Time travel: positions as of day 1")
spark.sql(f"SELECT * FROM curated.positions VERSION AS OF {day1} ORDER BY 1, 2").show()

step("7. Lineage events")
for e in read_events():
    snap = lambda ds: [(d["name"], snapshot_of(d)) for d in ds]
    print(f'{e["eventType"]:8} {e["job"]["name"]:22} in={snap(e["inputs"])} '
          f'out={snap(e["outputs"])} {error_of(e)}')
