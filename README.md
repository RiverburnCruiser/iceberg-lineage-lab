# Iceberg Lineage Lab

A small, local PySpark + Apache Iceberg project that shows three things a
governed data platform needs:

1. **Governed job submission.** A `@governed_job` decorator is the only way a job
   writes data. It checks input contracts, writes to a private Iceberg branch,
   audits the output (no PII, keys not null and unique), and publishes to `main`
   only if every check passes (Write-Audit-Publish).
2. **Data lineage.** Every run emits OpenLineage-shaped `START` / `COMPLETE` /
   `FAIL` events that record the Iceberg **snapshot ID** of each input and output,
   so any run can be replayed with time travel.
3. **Change capture from snapshots.** A `SnapshotWatcher` keeps the last snapshot
   it processed and reads only the row-level changes since then (`INSERT`,
   `DELETE`, `UPDATE_BEFORE`, `UPDATE_AFTER`) through Iceberg's changelog view.

The example domain is trades rolled up into account positions.

## Run it

Requires Python 3.10+ and Java 17 or 21.

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
python demo.py        # end-to-end walkthrough, ~1 minute
pytest -q             # 5 tests
```

The first run downloads the Iceberg runtime jar (~47 MB) from Maven Central. On a
network that blocks Spark's downloader, fetch the jar yourself and point to it:

```bash
curl -fLo iceberg.jar https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-4.0_2.13/1.10.0/iceberg-spark-runtime-4.0_2.13-1.10.0.jar
ICEBERG_JAR=$PWD/iceberg.jar python demo.py
```

## Layout

| File | What it does |
| --- | --- |
| `lab/spark.py` | Spark session with a local Iceberg Hadoop catalog (`warehouse/`) |
| `lab/governance.py` | `@governed_job`: contracts, Write-Audit-Publish, lineage events |
| `lab/lineage.py` | OpenLineage-shaped events appended to `lineage/events.jsonl` |
| `lab/cdc.py` | Snapshot history, changelog reads, `SnapshotWatcher` with a checkpoint |
| `jobs/positions.py` | Example tables and two jobs: a correct `MERGE` and a buggy append |
| `demo.py` | Walkthrough: load, publish, capture changes, block a bad job, time travel |

## How the decorator works

```python
@governed_job(inputs={"raw.trades": ["account_id", "ticker", "qty", "price"]},
              output="curated.positions", keys=["account_id", "ticker"],
              pii=["client_name"])
def build_positions(spark):
    spark.sql("MERGE INTO curated.positions ...")
```

The job author writes only business logic. The decorator sets
`spark.wap.branch`, so every write in the job lands on branch `wap_<run>`. It
checks that branch, then runs `CALL system.fast_forward(table, 'main', branch)`.
If a check fails, the branch is dropped and `main` never moves.

## Things worth knowing

- `table.snapshots` lists **every** snapshot, including the rejected one on the
  WAP branch. `table.refs` and `table.history` show what `main` points to.
  Unreferenced snapshots are removed by `expire_snapshots`.
- The watcher writes its checkpoint only after the handler succeeds, so a crash
  replays changes instead of losing them (at-least-once).
- Incremental reads (`start-snapshot-id`) see appends only. Overwrites and
  `MERGE` need the changelog view, which is what the watcher uses.
- Schema changes (`ALTER TABLE`) are not branch-scoped in Iceberg, so WAP guards
  data, not schema.

## Next steps

- POST the lineage events to [Marquez](https://marquezproject.ai/) for a lineage graph UI.
- Add the OpenLineage Spark listener and compare its column-level lineage with these events.
- Schedule `rewrite_data_files` and `expire_snapshots` as an Airflow DAG.
