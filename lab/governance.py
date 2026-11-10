"""@governed_job: the single door every Spark job goes through.

Before the job:  input data contracts (required columns, not empty).
During the job:  writes land on a private Iceberg branch (Write-Audit-Publish).
After the job:   output checks (no PII, keys not null, keys unique), then
                 fast-forward `main` to the branch. Readers never see bad data.
Every run:       OpenLineage-style START / COMPLETE / FAIL events with the
                 Iceberg snapshot ID of every input and output, and one row in
                 the append-only audit log `audit.job_runs`.

The pieces live in `lab.contracts` (the rules) and `lab.decorators` (the small
decorators that `governed_job` is stacked from). This module keeps the
original import paths working.
"""
from .contracts import DataContractError, check_inputs, check_output, current_snapshot
from .decorators import governed_job

__all__ = ["DataContractError", "check_inputs", "check_output", "current_snapshot",
           "governed_job"]
