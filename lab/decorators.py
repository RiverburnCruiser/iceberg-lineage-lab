"""Small, composable decorators for governed Spark jobs.

Each decorator does one job. `governed_job` stacks them in a safe order:

    @audited(...)              who, which code, which snapshots, result -> audit.job_runs
    @emit_lineage(...)         OpenLineage START / COMPLETE / FAIL events
    @retry(...)                optional; every attempt gets a fresh WAP branch
    @validate_inputs(...)      input contracts, checked before anything is written
    @write_audit_publish(...)  write to a private branch, check it, publish to main
    def job(spark, **params):  business logic only

A job is any function `job(spark, **params)`. The decorators share one `Run`
(run id, attempt) through a context variable, so the lineage events and the
audit row of a run carry the same run id.
"""
import contextvars
import functools
import logging
import time
import uuid
from collections.abc import Callable, Iterable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime

from pyspark.sql import SparkSession

from . import audit, lineage
from .contracts import check_inputs, check_output, current_snapshot

log = logging.getLogger(__name__)
Job = Callable[..., object]


class TransientError(Exception):
    """A failure worth retrying, such as a lost executor or a throttled object store."""


@dataclass
class Run:
    """State shared by the decorators of one job run."""
    job: str
    run_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    attempt: int = 1

    @property
    def branch(self) -> str:
        return f"wap_{self.run_id[:8]}_{self.attempt}"


_RUN: contextvars.ContextVar[Run | None] = contextvars.ContextVar("run", default=None)


@contextmanager
def run_scope(job: str) -> Iterator[Run]:
    """Join the run already in progress for `job`, or start a new one."""
    run = _RUN.get()
    if run is not None and run.job == job:
        yield run
        return
    token = _RUN.set(Run(job))
    try:
        yield _RUN.get()
    finally:
        _RUN.reset(token)


def _layer(name: str, fn: Job, wrapper: Job) -> Job:
    """functools.wraps keeps the job's name and docstring; `layer` names the decorator."""
    wrapper = functools.wraps(fn)(wrapper)
    wrapper.layer = name
    return wrapper


def layers(fn: Job) -> list[str]:
    """Decorator layers from outermost to the job itself, by following __wrapped__."""
    names = []
    while hasattr(fn, "__wrapped__"):
        names.append(fn.layer)
        fn = fn.__wrapped__
    return names + [f"{fn.__name__}()  <- the job"]


def retry(times: int = 3, on: tuple[type[BaseException], ...] = (TransientError,),
          delay_s: float = 0.0):
    """Call the function again on any exception in `on`, up to `times` attempts in total."""
    def decorator(fn: Job) -> Job:
        def wrapper(*args, **kwargs):
            for attempt in range(1, times + 1):
                if run := _RUN.get():
                    run.attempt = attempt
                try:
                    return fn(*args, **kwargs)
                except on as e:
                    if attempt == times:
                        raise
                    log.info("retry: attempt %d failed (%s), trying again", attempt, e)
                    time.sleep(delay_s)
        return _layer(f"retry(times={times}, on={[e.__name__ for e in on]})", fn, wrapper)
    return decorator


def validate_inputs(contracts: dict[str, list[str]]):
    """Reject the run before any write if an input lacks columns or is empty."""
    def decorator(fn: Job) -> Job:
        def wrapper(spark: SparkSession, **params):
            check_inputs(spark, contracts)
            return fn(spark, **params)
        return _layer(f"validate_inputs({list(contracts)})", fn, wrapper)
    return decorator


def write_audit_publish(output: str, keys: list[str], pii: Iterable[str] = ()):
    """Write-Audit-Publish: commits go to a private branch; `main` moves only if checks pass."""
    pii = list(pii)

    def decorator(fn: Job) -> Job:
        def wrapper(spark: SparkSession, **params):
            with run_scope(fn.__name__) as run:
                branch = run.branch
                if current_snapshot(spark, output) is not None:
                    # Create the branch from main ourselves. If the first WAP write creates
                    # it, Iceberg computes the snapshot's total-records / total-data-files
                    # from zero instead of from main (seen on 1.10), and the metadata
                    # monitor would report wrong row counts.
                    spark.sql(f"ALTER TABLE {output} CREATE BRANCH {branch}")
                spark.conf.set("spark.wap.branch", branch)          # write
                try:
                    result = fn(spark, **params)
                    spark.conf.unset("spark.wap.branch")
                    if current_snapshot(spark, output, branch) is None:
                        log.info("WAP: job wrote nothing, nothing to publish")
                        return result
                    try:                                            # audit
                        check_output(spark.table(f"{output}.branch_{branch}"), output, keys, pii)
                    except Exception as e:
                        log.info("WAP: audit of branch %s failed (%s); main unchanged", branch, e)
                        raise
                    spark.sql(f"CALL system.fast_forward('{output}', 'main', '{branch}')")
                    log.info("WAP: branch %s passed audit; main -> snapshot %s",  # publish
                             branch, current_snapshot(spark, output))
                    return result
                finally:
                    spark.conf.unset("spark.wap.branch")
                    spark.sql(f"ALTER TABLE {output} DROP BRANCH IF EXISTS {branch}")
        return _layer(f"write_audit_publish({output!r}, keys={keys})", fn, wrapper)
    return decorator


def emit_lineage(inputs: Iterable[str], output: str):
    """Emit START, then COMPLETE or FAIL, with the snapshot ID of every input and output."""
    inputs = list(inputs)

    def decorator(fn: Job) -> Job:
        def wrapper(spark: SparkSession, **params):
            with run_scope(fn.__name__) as run:
                ins = [lineage.dataset(t, current_snapshot(spark, t)) for t in inputs]
                outs = [lineage.dataset(output, current_snapshot(spark, output))]
                lineage.emit("START", run.run_id, fn.__name__, ins, outs, params=params)
                try:
                    result = fn(spark, **params)
                except Exception as e:
                    lineage.emit("FAIL", run.run_id, fn.__name__, ins, outs, error=str(e))
                    raise
                outs = [lineage.dataset(output, current_snapshot(spark, output))]
                lineage.emit("COMPLETE", run.run_id, fn.__name__, ins, outs)
                return result
        return _layer(f"emit_lineage({inputs} -> {output!r})", fn, wrapper)
    return decorator


def audited(inputs: Iterable[str] = (), outputs: Iterable[str] = ()):
    """Append one row per run to audit.job_runs. A `run_key` that already succeeded is skipped."""
    inputs, outputs = list(inputs), list(outputs)

    def decorator(fn: Job) -> Job:
        def wrapper(spark: SparkSession, **params):
            with run_scope(fn.__name__) as run:
                run_key, started = params.get("run_key"), datetime.now()
                snaps = lambda tables: {t: current_snapshot(spark, t) for t in tables}
                ins = snaps(inputs)
                record = functools.partial(audit.record, spark, run.run_id, fn.__name__,
                                           run_key, started, ins)
                if run_key and audit.already_succeeded(spark, fn.__name__, run_key):
                    log.info("audited: %s run_key=%s already succeeded; skipping",
                             fn.__name__, run_key)
                    record(snaps(outputs), "SKIPPED")
                    return None
                try:
                    result = fn(spark, **params)
                except Exception as e:
                    record(snaps(outputs), "FAILED", error=str(e))
                    raise
                record(snaps(outputs), "SUCCESS")
                return result
        return _layer("audited -> audit.job_runs", fn, wrapper)
    return decorator


def governed_job(*, inputs: dict[str, list[str]], output: str, keys: list[str],
                 pii: Iterable[str] = (), retries: int = 1,
                 retry_on: tuple[type[BaseException], ...] = (TransientError,)):
    """The standard stack. Same as writing the decorators above one under another."""
    def decorator(job: Job) -> Job:
        fn = write_audit_publish(output, keys, pii)(job)
        fn = validate_inputs(inputs)(fn)
        if retries > 1:
            fn = retry(retries, on=retry_on)(fn)
        fn = emit_lineage(inputs, output)(fn)
        return audited(inputs, [output])(fn)
    return decorator
