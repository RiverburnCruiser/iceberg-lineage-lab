"""OpenLineage-shaped run events, appended to a JSONL file.

Swap `emit` for an HTTP POST to Marquez (/api/v1/lineage) to get a lineage UI.
"""
import getpass
import json
import os
from datetime import datetime, timezone
from pathlib import Path

LINEAGE_FILE = Path(os.getenv("LINEAGE_FILE", "lineage/events.jsonl"))
NAMESPACE = "iceberg-lineage-lab"


def dataset(table: str, snapshot_id: int | None) -> dict:
    return {"namespace": "local", "name": table,
            "facets": {"icebergSnapshot": {"snapshotId": snapshot_id}}}


def emit(event_type: str, run_id: str, job: str, inputs: list[dict],
         outputs: list[dict], **facets) -> dict:
    event = {
        "eventType": event_type,  # START | COMPLETE | FAIL
        "eventTime": datetime.now(timezone.utc).isoformat(),
        "producer": NAMESPACE,
        "run": {"runId": run_id, "facets": {"user": getpass.getuser(), **facets}},
        "job": {"namespace": NAMESPACE, "name": job},
        "inputs": inputs,
        "outputs": outputs,
    }
    LINEAGE_FILE.parent.mkdir(parents=True, exist_ok=True)
    with LINEAGE_FILE.open("a") as f:
        f.write(json.dumps(event) + "\n")
    return event
