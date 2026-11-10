"""OpenLineage run events, appended to lineage/events.jsonl, plus a lineage graph built from them.

Set MARQUEZ_URL (for example http://localhost:5000) to also POST every event to
Marquez at /api/v1/lineage and browse the graph in its UI. Lineage is
observability, not a gate: if Marquez is down the job still runs and the event
is still in the JSONL file.
"""
import getpass
import json
import logging
import os
import re
import urllib.request
from collections import deque
from datetime import datetime, timezone
from pathlib import Path

from .audit import code_version

log = logging.getLogger(__name__)
LINEAGE_FILE = Path(os.getenv("LINEAGE_FILE", "lineage/events.jsonl"))
NAMESPACE = "iceberg-lineage-lab"
PRODUCER = "https://github.com/RiverburnCruiser/iceberg-lineage-lab"
SCHEMA_URL = "https://openlineage.io/spec/2-0-2/OpenLineage.json#/definitions/RunEvent"


def _facet(schema: str = f"{PRODUCER}/facets", **fields) -> dict:
    return {"_producer": PRODUCER, "_schemaURL": schema, **fields}


def dataset(table: str, snapshot_id: int | None) -> dict:
    """An OpenLineage dataset with the exact Iceberg snapshot that was read or written."""
    return {"namespace": "local", "name": table,
            "facets": {"icebergSnapshot": _facet(snapshotId=snapshot_id)}}


def emit(event_type: str, run_id: str, job: str, inputs: list[dict], outputs: list[dict],
         params: dict | None = None, error: str | None = None) -> dict:
    """Append one START / COMPLETE / FAIL event to the JSONL file (and Marquez, if set)."""
    facets = {"governance": _facet(user=getpass.getuser(), codeVersion=code_version(),
                                   params=params or {})}
    if error:
        facets["errorMessage"] = _facet(
            "https://openlineage.io/spec/facets/1-0-1/ErrorMessageRunFacet.json",
            message=error, programmingLanguage="python")
    event = {
        "eventType": event_type,
        "eventTime": datetime.now(timezone.utc).isoformat(),
        "producer": PRODUCER,
        "schemaURL": SCHEMA_URL,
        "run": {"runId": run_id, "facets": facets},
        "job": {"namespace": NAMESPACE, "name": job},
        "inputs": inputs,
        "outputs": outputs,
    }
    LINEAGE_FILE.parent.mkdir(parents=True, exist_ok=True)
    with LINEAGE_FILE.open("a") as f:
        f.write(json.dumps(event) + "\n")
    post_to_marquez(event)
    return event


def post_to_marquez(event: dict) -> bool:
    """POST the event to $MARQUEZ_URL/api/v1/lineage. Returns False if unset or unreachable."""
    if not (url := os.getenv("MARQUEZ_URL")):
        return False
    req = urllib.request.Request(f"{url.rstrip('/')}/api/v1/lineage", method="POST",
                                 data=json.dumps(event).encode(),
                                 headers={"Content-Type": "application/json"})
    try:
        urllib.request.urlopen(req, timeout=3).close()
        return True
    except OSError as e:
        log.warning("lineage: could not POST to %s: %s", url, e)
        return False


def read_events(path: Path | None = None) -> list[dict]:
    path = path or LINEAGE_FILE
    return [json.loads(line) for line in path.read_text().splitlines()] if path.exists() else []


def snapshot_of(ds: dict) -> int | None:
    return ds["facets"]["icebergSnapshot"]["snapshotId"]


def error_of(event: dict) -> str:
    return event["run"]["facets"].get("errorMessage", {}).get("message", "")


def provenance(events: list[dict], table: str, snapshot_id: int) -> dict | None:
    """Which run produced this snapshot of `table`, and from which input snapshots?"""
    for e in events:
        if e["eventType"] == "COMPLETE" and any(
                d["name"] == table and snapshot_of(d) == snapshot_id for d in e["outputs"]):
            gov = e["run"]["facets"]["governance"]
            return {"job": e["job"]["name"], "run_id": e["run"]["runId"], "time": e["eventTime"],
                    "user": gov["user"], "code_version": gov["codeVersion"],
                    "inputs": {d["name"]: snapshot_of(d) for d in e["inputs"]}}
    return None


def downstream(events: list[dict], table: str) -> list[str]:
    """Impact analysis: every dataset built, directly or not, from `table`."""
    reads: dict[str, set[str]] = {}
    for e in events:
        if e["eventType"] == "COMPLETE":
            for i in e["inputs"]:
                reads.setdefault(i["name"], set()).update(o["name"] for o in e["outputs"])
    seen, todo = [], deque([table])
    while todo:
        for out in sorted(reads.get(todo.popleft(), ())):
            if out not in seen:
                seen.append(out)
                todo.append(out)
    return seen


def to_mermaid(events: list[dict]) -> str:
    """Datasets and jobs as nodes; edges carry the snapshot IDs read or written.

    A failed run is drawn as a dashed edge: it never published, so it has no snapshot.
    """
    node = lambda kind, name: f"{kind}_{re.sub(r'\W', '_', name)}"
    nodes: dict[str, str] = {}
    edges: dict[tuple[str, str, str], list[str]] = {}
    for e in events:
        if e["eventType"] not in ("COMPLETE", "FAIL"):
            continue
        job = node("job", e["job"]["name"])
        nodes[job] = f'{job}[["{e["job"]["name"]}"]]'
        for d in e["inputs"] + e["outputs"]:
            nodes[node("ds", d["name"])] = f'{node("ds", d["name"])}[("{d["name"]}")]'
        for d in e["inputs"]:
            edges.setdefault((node("ds", d["name"]), job, "-->"), []).append(f"read {snapshot_of(d)}")
        for d in e["outputs"]:
            if e["eventType"] == "COMPLETE":
                edges.setdefault((job, node("ds", d["name"]), "-->"), []).append(
                    f"wrote {snapshot_of(d)}")
            else:
                edges.setdefault((job, node("ds", d["name"]), "-.->"), []).append("FAIL: not published")
    lines = ["flowchart LR", *(f"    {n}" for n in nodes.values())]
    for (src, dst, arrow), labels in edges.items():
        lines.append(f'    {src} {arrow}|"{"<br/>".join(dict.fromkeys(labels))}"| {dst}')
    return "\n".join(lines)


def write_graph(path: Path | None = None) -> Path:
    """Write the lineage graph of all events as a Mermaid diagram (GitHub renders it)."""
    path = path or LINEAGE_FILE.parent / "graph.md"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("# Lineage graph\n\nBuilt from `events.jsonl` by `lab.lineage.write_graph`.\n\n"
                    f"```mermaid\n{to_mermaid(read_events())}\n```\n")
    return path
