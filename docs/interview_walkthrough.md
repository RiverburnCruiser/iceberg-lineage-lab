# Interview walkthrough

Before the interview: `source .venv/bin/activate`, run `pytest -q` once (this warms
the Ivy cache), and keep one terminal open in the repo. Add `2>/dev/null` to hide
Spark's startup logs. Each demo takes roughly 15 to 40 seconds.

---

## Q1. Do you know Iceberg?

**Say:** Iceberg is a table format. A table is a tree of files: a `metadata.json`
points to snapshots, each snapshot points to manifests, and each manifest lists
data files with per-column min and max values. Because every commit just swaps that
pointer atomically, you get ACID writes, time travel, rollback, schema changes
without rewriting data, and hidden partitioning, where the table is partitioned by
`days(trade_ts)` but people simply filter on `trade_ts`.

**Run:** `python -m lab.show q1`

**Point at:**
- The `v3.metadata.json` path: the one file the catalog tracks.
- `snapshots`: one row per commit, with `parent_id` forming the history.
- `files`: `qty_min` / `qty_max`. These stats let the planner skip files.
- `read 1 of 2 data files`: real Iceberg scan metrics for `WHERE trade_ts >= ...`.
- Rollback: rows 6 → 5, and the bad snapshot in `history` with `is_current_ancestor = false`.
- `data files rewritten: 0, new snapshots: 0` after `ADD COLUMN`.

---

## Q2. Do you know data lineage?

**Say:** Lineage answers two questions: where did this data come from, and what
does it feed. The useful version is not just "table A feeds table B" but "this
version of B was built by this run, from this exact version of A". Because
Iceberg gives every version a snapshot ID, I record those IDs, which lets me trace,
rebuild, and do impact analysis.

**Run:** `python -m lab.show q2`

**Point at:**
- The event list: every run has START plus COMPLETE or FAIL, with `table@snapshot` on both sides.
- Provenance: job, run id, user, git SHA, and the input snapshot for the current positions.
- `replay == published: True`: re-running the SQL on the old input snapshot gives exactly the published table, even though `raw.trades` has moved on.
- Impact analysis: `['curated.positions', 'curated.trades']` is downstream of `raw.trades`.

---

## Q3. How would you implement data lineage?

**Say:** I put it in the platform, not in each job: the `@emit_lineage` decorator
emits OpenLineage events (START, COMPLETE, FAIL) carrying input and output snapshot
IDs, so job authors write no lineage code. The events go to an append-only store
(a JSONL file here, or Marquez or another OpenLineage backend through one
environment variable), and a graph is built from them. Lineage must never break a
job: if the backend is down, the job still runs and the event is still on disk.

**Run:** `python -m lab.show q3` (optional: `docker compose up -d` first and
prefix with `MARQUEZ_URL=http://localhost:5000`)

**Point at:**
- The COMPLETE event: standard OpenLineage shape plus an `icebergSnapshot` facet.
- The Mermaid graph: datasets as cylinders, jobs as boxes, snapshot IDs on edges, and the dashed `FAIL: not published` edge for the blocked job.
- `lineage/graph.md`: open it on GitHub or in VS Code preview to see the diagram rendered.
- Code: `lab/decorators.py:emit_lineage` (about 15 lines), `lab/lineage.py:post_to_marquez`.

---

## Q4. How would you ensure data integrity and compliance while automating Spark job submission?

**Say:** The scheduler can submit only governed jobs, and every governed run goes
through the same gates: input contracts before anything is written, writes to a
private branch that is checked before it is published to `main`, raw PII replaced
by a salted hash, and one append-only audit row with who ran it, the git SHA, and
the snapshot IDs. Reruns are safe because a run key that already succeeded is
skipped. For GDPR erasure, deleting rows is not enough on Iceberg, because old
snapshots can still be time-travelled, so I also expire those snapshots, which
physically removes the old files.

**Run:** `python -m lab.show q4`

**Point at:**
1. `REJECTED before any write: missing columns ['currency']`.
2. `BLOCKED: duplicate rows` and `main unchanged: True`.
3. `client_name_sha256`: the same client gets the same hash, so joins still work.
4. `curated.trades rows: 3 (still 3)` and the `SKIPPED` row in the audit log.
5. The audit table: `user`, `code_version` (git SHA), `output_snapshots`, `result`; `commit operations: ['append']`.
6. Erasure: rows on disk `(2, 2)` → `(0, 0)`, and time travel fails with `Cannot find snapshot`.

---

## Q5. Do you know decorators?

**Say:** A decorator is a function that takes a function and returns a new one,
so I can add behaviour around code without changing it. `functools.wraps` keeps
the original name, docstring and a `__wrapped__` link, which matters for logging,
lineage names and debugging. Stacking is function composition: `@a @b def f` is
`a(b(f))`, so `a` runs first and `b` sits closest to the function.

**Run:** `python -m lab.show q5` (no Spark, instant)

**Point at:**
- The enter/exit order of `logged` and `tagged('pricing')`.
- `__name__='market_value'` survives decoration.
- The real five-layer stack on `build_positions`, found by following `__wrapped__`.

---

## Q6. How would you use decorators to ensure data integrity and encapsulation for automation?

**Say:** Each concern is one small decorator (validate inputs, Write-Audit-Publish,
lineage, audit, retry), and `@governed_job` stacks them in a fixed order, so
every job gets the same guarantees and job authors write only business logic, with
no credentials, branch names, lineage or audit code. Order is a real design
decision: retry has to sit outside Write-Audit-Publish so that each attempt starts
on a clean branch. Retry also handles only transient errors, never a broken data
contract.

**Run:** `python -m lab.show q6`

**Point at:**
1. The job source: a decorator with declared contracts and no infrastructure code.
2. Retry inside WAP: the second attempt lands on the same branch as the first attempt's partial write, so the audit blocks it.
3. Retry outside WAP: a fresh branch, published, 3 rows, no duplicates.
4. `failed after 1 attempt(s)`: a `DataContractError` is not retried.

---

## Q7. How do you capture changes in a data pipeline?

**Say:** Iceberg already writes a "screenshot" of the table on every commit (a
snapshot), so I take periodic screenshots of each table's metadata into an audit
table (snapshot, operation, row and file counts, schema hash) and compare
consecutive ones to alert on schema drift, row-count jumps and snapshots that
never reached `main`. For row-level changes I use the changelog view between two
snapshots, because a plain incremental read only sees appended files and silently
misses MERGE updates and deletes. A watcher keeps the last processed snapshot as a
checkpoint, written only after success, so it is at-least-once.

**Run:** `python -m lab.show q7`

**Point at:**
- `audit.table_metadata_history`: three screenshots; the counts come from snapshot summaries, so no data is scanned.
- `ROW JUMP raw.trades: 3 -> 5` (positions 3 → 4 stays under the 50% threshold).
- `SCHEMA DRIFT ... (snapshot unchanged)`: `ALTER TABLE` makes no snapshot, which is why the schema is hashed.
- The unpublished snapshot: the blocked buggy job's branch commit.
- Incremental read rows `2` for append-only `raw.trades`, `0` for merged `curated.positions`, then the changelog with `UPDATE_BEFORE` / `UPDATE_AFTER` / `INSERT`.

---

## Likely follow-ups

- **"Isn't the audit row after the publish a gap?"** Yes, a small one. The WAP key check catches the rerun for keyed tables. A stricter design puts the run key in the snapshot summary, or uses `spark.wap.id` with `cherrypick_snapshot`, which rejects a duplicate WAP ID.
- **"Why not just check after writing?"** Readers would see bad data before the check fails. WAP makes publishing one atomic pointer move.
- **"What about schema changes inside a WAP job?"** They are not branch-scoped in Iceberg. They hit `main` immediately, so schema changes go through migrations, and the monitor detects drift.
- **"Erasure vs time travel?"** They conflict. Erasure expires older snapshots, so time travel before that point is lost on purpose. Other branches or tags must be dropped first; `erase` refuses otherwise.
- **"A bug you found?"** When a WAP write creates its branch implicitly, Iceberg 1.10 computes the snapshot's `total-records` from zero. I now create the branch from `main` explicitly. There is a test for it.
