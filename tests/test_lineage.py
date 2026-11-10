"""Q2 / Q3: lineage events, graph, provenance, Marquez."""
import json
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest

import lab.lineage as lineage
from jobs.positions import build_positions, build_positions_buggy, load_trades
from lab.contracts import DataContractError, current_snapshot

DAY1 = [(1, "A1", "Asha Rao", "MSFT", 10, 400.0), (2, "B2", "Ben Li", "MSFT", 20, 400.0)]


def test_graph_provenance_and_impact(spark):
    load_trades(spark, DAY1)
    build_positions(spark)
    with pytest.raises(DataContractError):
        build_positions_buggy(spark)
    raw, pos = current_snapshot(spark, "raw.trades"), current_snapshot(spark, "curated.positions")
    events = lineage.read_events()

    p = lineage.provenance(events, "curated.positions", pos)
    assert p["job"] == "build_positions" and p["inputs"] == {"raw.trades": raw}
    assert lineage.downstream(events, "raw.trades") == ["curated.positions"]

    graph = lineage.write_graph().read_text()
    assert "```mermaid" in graph
    assert f'ds_raw_trades -->|"read {raw}"| job_build_positions' in graph
    assert f'job_build_positions -->|"wrote {pos}"| ds_curated_positions' in graph
    assert 'job_build_positions_buggy -.->|"FAIL: not published"| ds_curated_positions' in graph


def test_events_are_posted_to_marquez_when_configured(monkeypatch, tmp_path):
    received = []

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            received.append((self.path, json.loads(self.rfile.read(int(self.headers["Content-Length"])))))
            self.send_response(201)
            self.end_headers()

        def log_message(self, *args):
            pass

    server = HTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    try:
        monkeypatch.setenv("MARQUEZ_URL", f"http://127.0.0.1:{server.server_port}")
        lineage.emit("START", "0b6f7c1e-0000-4000-8000-000000000000", "job", [], [])
    finally:
        server.shutdown()
    path, event = received[0]
    assert path == "/api/v1/lineage"
    assert event["eventType"] == "START" and event["schemaURL"].startswith("https://openlineage.io")

    monkeypatch.setenv("MARQUEZ_URL", "http://127.0.0.1:9")      # nothing listens: no crash
    assert lineage.post_to_marquez(event) is False
    assert len(lineage.read_events()) == 1                       # still recorded locally
