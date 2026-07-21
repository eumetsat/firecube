# Copyright 2025-2026 EUMETSAT
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import annotations

import json

from firecube.core.controlplane.manager import ChunkManager
from firecube.core.controlplane.types import SpanCoverage
from tests.helpers.storage import make_test_binding


def test_control_plane_schema_version(tmp_path):
    """Verify that `.firecube/` WAL and snapshots carry schema_version='v2'."""
    workspace = tmp_path / "workspace"
    output_root = tmp_path / "repo"
    output_root.mkdir()

    cm = ChunkManager(binding=make_test_binding(output_root), workspace=workspace)

    cm.record_run_started(
        product="test-product",
        run_id="run-001",
        output_path="s3://bucket/test",
        output_format="zarr",
        size=100,
        meta={"env": "test"},
    )
    cm.record_span(
        product="test-product",
        run_id="run-001",
        batch_id="batch-001",
        group="group-A",
        status="active",
        coverage=SpanCoverage(
            group="group-A",
            arrays=["group-A/data"],
            time_index_ranges=[[0, 1]],
        ),
        meta={"env": "test"},
    )
    cm.record_run_terminal(
        product="test-product",
        run_id="run-001",
        output_path="s3://bucket/test",
        output_format="zarr",
        size=100,
        meta={"env": "test"},
        status="complete",
    )
    cm.rebuild_snapshot("test-product")
    cm.close()

    control_root = output_root / "test-product" / ".firecube"
    assert control_root.exists()

    run_dir = control_root / "runs" / "run-001"
    lines: list[dict] = []
    for events_file in sorted(run_dir.glob("events-*.jsonl")):
        lines.extend(
            json.loads(line)
            for line in events_file.read_text(encoding="utf-8").splitlines()
            if line.strip()
        )
    assert len(lines) >= 3
    for event in lines:
        assert event["schema_version"] == "v2"
        assert "event_type" in event
        assert event["record"]["schema_version"] == "v2"

    latest = json.loads((control_root / "LATEST.json").read_text(encoding="utf-8"))
    assert latest["schema_version"] == "v2"

    snapshot_path = control_root / "snapshots" / f"snapshot-{latest['generation']}.jsonl"
    snapshot_lines = [
        json.loads(line)
        for line in snapshot_path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    assert snapshot_lines
    assert all(record["schema_version"] == "v2" for record in snapshot_lines)
