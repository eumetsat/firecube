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
import logging
from pathlib import Path
from typing import Any

from firecube.core.controlplane import ChunkManager
from firecube.core.controlplane._event_processor import apply_events
from firecube.core.controlplane.types import SCHEMA_VERSION, build_span_entry
from tests.helpers.storage import make_test_binding


def _run_event(*, event_type: str, run_id: str, timestamp: float, status: str) -> dict[str, Any]:
    return {
        "event_id": f"{event_type}-{run_id}",
        "event_type": event_type,
        "product": "product",
        "run_id": run_id,
        "timestamp": timestamp,
        "record": {
            "key": f"run_{run_id}",
            "type": "run",
            "size": 0,
            "timestamp": timestamp,
            "status": status,
            "meta": {"run_id": run_id},
            "run": {"output_path": "/tmp/product", "output_format": "zarr"},
            "schema_version": SCHEMA_VERSION,
        },
    }


def _span_event(*, run_id: str, batch_id: str, group: str, timestamp: float) -> dict[str, Any]:
    record = build_span_entry(
        run_id=run_id,
        batch_id=batch_id,
        group=group,
        meta={"group": group},
        arrays=[f"{group}/FWI"],
        time_index_ranges=[[0, 1]],
        status="active",
    )
    record["timestamp"] = timestamp
    return {
        "event_id": f"span-{run_id}-{batch_id}-{group}",
        "event_type": "span_committed",
        "product": "product",
        "run_id": run_id,
        "timestamp": timestamp,
        "record": record,
    }


def _started_with_replacement_event(
    *, run_id: str, timestamp: float, replaces: list[str]
) -> dict[str, Any]:
    return {
        "event_id": f"replacement-start-{run_id}",
        "event_type": "run_started_with_replacement",
        "product": "product",
        "run_id": run_id,
        "timestamp": timestamp,
        "record": {
            "replaces": list(replaces),
            "schema_version": SCHEMA_VERSION,
        },
    }


def _replacement_committed_event(
    *, run_id: str, timestamp: float, replacing_run_id: str, replaced_span_keys: list[str]
) -> dict[str, Any]:
    return {
        "event_id": f"replacement-committed-{run_id}",
        "event_type": "replacement_committed",
        "product": "product",
        "run_id": run_id,
        "timestamp": timestamp,
        "record": {
            "replacing_run_id": replacing_run_id,
            "replaced_span_keys": list(replaced_span_keys),
            "schema_version": SCHEMA_VERSION,
        },
    }


def _chunk_manager(tmp_path: Path) -> ChunkManager:
    return ChunkManager(make_test_binding(tmp_path))


class _LocalSnapshotFS:
    def open(self, uri, mode: str = "r"):
        path = Path(getattr(uri, "path", uri))
        return path.open(mode)

    def exists(self, uri) -> bool:
        path = Path(getattr(uri, "path", uri))
        return path.exists()


def _seed_snapshot_state(
    manager: ChunkManager,
    *,
    product: str,
    meta: dict[str, Any],
) -> None:
    snapshot_meta_path = manager.workspace / f"{product}-snapshot.meta.json"
    snapshot_meta_path.write_text(json.dumps(meta))
    manager.repo._ensure_bound = lambda: None
    manager.repo._fs = _LocalSnapshotFS()
    manager.repo._read_latest_pointer = lambda product: {
        "schema_version": SCHEMA_VERSION,
        "completed_before": 1.0,
        "generation": 1,
        "snapshot_meta_path": str(snapshot_meta_path),
    }


def test_snapshot_status_reads_records_field_canonical(tmp_path):
    manager = _chunk_manager(tmp_path)
    _seed_snapshot_state(manager, product="product", meta={"records": 2})

    status = manager.snapshot_status("product")

    assert status["exists"] is True
    assert status["records"] == 2


def test_snapshot_status_falls_back_to_record_count_legacy(tmp_path):
    manager = _chunk_manager(tmp_path)
    _seed_snapshot_state(manager, product="product", meta={"record_count": 3})

    status = manager.snapshot_status("product")

    assert status["exists"] is True
    assert status["records"] == 3


def test_snapshot_status_returns_zero_when_neither_field(tmp_path):
    manager = _chunk_manager(tmp_path)
    _seed_snapshot_state(manager, product="product", meta={})

    status = manager.snapshot_status("product")

    assert status["exists"] is True
    assert status["records"] == 0


def test_replacement_at_terminal():
    log = logging.getLogger(__name__)
    span_a = _span_event(run_id="run-a", batch_id="batch-a", group="F024", timestamp=2.0)
    span_b = _span_event(run_id="run-b", batch_id="batch-b", group="F048", timestamp=4.0)
    events = [
        span_a,
        _started_with_replacement_event(run_id="run-b", timestamp=3.0, replaces=["run-a"]),
        span_b,
        _replacement_committed_event(
            run_id="run-b",
            timestamp=5.0,
            replacing_run_id="run-b",
            replaced_span_keys=[span_a["record"]["key"]],
        ),
    ]

    current: dict[str, dict[str, Any]] = {}
    apply_events(current, events[:3], log)

    span_a_key = str(span_a["record"]["key"])
    span_b_key = str(span_b["record"]["key"])
    assert current[span_a_key]["status"] == "active"

    apply_events(current, events[3:], log)

    assert current[span_a_key]["status"] == "replaced"
    assert current[span_a_key]["replaced_by"] == "run-b"
    assert current[span_a_key]["replaced_at"] == 5.0
    assert current[span_b_key]["status"] == "active"


def test_started_with_replacement_alone_does_not_replace():
    log = logging.getLogger(__name__)
    span_a = _span_event(run_id="run-a", batch_id="batch-a", group="F024", timestamp=2.0)
    current: dict[str, dict[str, Any]] = {}

    apply_events(
        current,
        [
            span_a,
            _started_with_replacement_event(run_id="run-b", timestamp=3.0, replaces=["run-a"]),
        ],
        log,
    )

    assert current[str(span_a["record"]["key"])]["status"] == "active"


def test_old_events_still_work():
    log = logging.getLogger(__name__)
    current: dict[str, dict[str, Any]] = {}
    span = _span_event(run_id="run-a", batch_id="batch-a", group="F024", timestamp=2.0)
    apply_events(
        current,
        [
            _run_event(event_type="run_started", run_id="run-a", timestamp=1.0, status="started"),
            span,
            _run_event(
                event_type="run_completed", run_id="run-a", timestamp=3.0, status="complete"
            ),
        ],
        log,
    )

    span_key = str(span["record"]["key"])
    assert current[span_key]["status"] == "active"
    assert current["run_run-a"]["status"] == "complete"
