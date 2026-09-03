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

"""Maintenance lifecycle WAL events."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from firecube.core.controlplane.repo import ManifestRepository
from firecube.core.controlplane.types import (
    EVENT_MAINTENANCE_COMPLETED,
    EVENT_MAINTENANCE_FAILED,
    EVENT_MAINTENANCE_STARTED,
    MAINTENANCE_KIND,
    MAINTENANCE_OP_ARCHIVE_RESTORE,
    MAINTENANCE_OP_DELETE,
    MAINTENANCE_OP_SCRUB,
)
from firecube.core.errors import ManifestError
from tests.helpers.storage import make_test_binding


def _read_run_wal_events(
    temp_workspace: Path, *, product: str, run_id: str
) -> list[dict[str, Any]]:
    run_dir = temp_workspace / product / ".firecube" / "runs" / run_id
    events: list[dict[str, Any]] = []
    for path in sorted(run_dir.glob("events-*.jsonl")):
        events.extend(
            json.loads(line)
            for line in path.read_text(encoding="utf-8").splitlines()
            if line.strip()
        )
    return events


def _read_run_meta(temp_workspace: Path, *, product: str, run_id: str) -> dict[str, Any]:
    return json.loads(
        (temp_workspace / product / ".firecube" / "runs" / run_id / "run.json").read_text(
            encoding="utf-8"
        )
    )


def test_maintenance_started_event_written(temp_workspace):
    product = "product.zarr"
    run_id = "maintenance-delete-001"
    repo = ManifestRepository(binding=make_test_binding(temp_workspace), workspace=temp_workspace)

    repo.record_maintenance_started(
        product=product,
        run_id=run_id,
        op=MAINTENANCE_OP_DELETE,
        scope_meta={"chunks_count": 3, "delete_storage": True},
    )

    events = _read_run_wal_events(temp_workspace, product=product, run_id=run_id)
    started = [e for e in events if e["event_type"] == EVENT_MAINTENANCE_STARTED]

    assert len(started) == 1
    record = next(iter(started))["record"]
    assert record["type"] == "run"
    assert record["status"] == "started"
    assert record["key"] == f"run_{run_id}"
    assert record["meta"]["kind"] == MAINTENANCE_KIND
    assert record["meta"]["op"] == MAINTENANCE_OP_DELETE
    assert record["meta"]["run_id"] == run_id
    assert record["meta"]["chunks_count"] == 3
    assert record["meta"]["delete_storage"] is True
    assert record["maintenance"]["op"] == MAINTENANCE_OP_DELETE
    assert record["maintenance"]["kind"] == MAINTENANCE_KIND
    assert record["maintenance"]["scope"]["chunks_count"] == 3


def test_maintenance_completed_finalizes_run_meta(temp_workspace):
    product = "product.zarr"
    run_id = "maintenance-delete-002"
    repo = ManifestRepository(binding=make_test_binding(temp_workspace), workspace=temp_workspace)

    repo.record_maintenance_started(
        product=product,
        run_id=run_id,
        op=MAINTENANCE_OP_DELETE,
        scope_meta={"chunks_count": 0},
    )
    repo.record_maintenance_completed(
        product=product,
        run_id=run_id,
        op=MAINTENANCE_OP_DELETE,
        scope_meta={"chunks_count": 0},
    )

    events = _read_run_wal_events(temp_workspace, product=product, run_id=run_id)
    types = [e["event_type"] for e in events]
    assert EVENT_MAINTENANCE_STARTED in types
    assert EVENT_MAINTENANCE_COMPLETED in types

    completed = next(e for e in events if e["event_type"] == EVENT_MAINTENANCE_COMPLETED)
    assert completed["record"]["status"] == "complete"
    assert completed["record"]["meta"]["op"] == MAINTENANCE_OP_DELETE

    meta = _read_run_meta(temp_workspace, product=product, run_id=run_id)
    assert meta["status"] == "complete"
    assert meta.get("completed_at")


def test_maintenance_failed_finalizes_run_meta_with_error(temp_workspace):
    product = "product.zarr"
    run_id = "maintenance-archive-restore-003"
    repo = ManifestRepository(binding=make_test_binding(temp_workspace), workspace=temp_workspace)

    repo.record_maintenance_started(
        product=product,
        run_id=run_id,
        op=MAINTENANCE_OP_ARCHIVE_RESTORE,
        scope_meta={"source_archive": "/tmp/archive.tgm"},
    )
    repo.record_maintenance_failed(
        product=product,
        run_id=run_id,
        op=MAINTENANCE_OP_ARCHIVE_RESTORE,
        scope_meta={"source_archive": "/tmp/archive.tgm"},
        error="disk full",
    )

    events = _read_run_wal_events(temp_workspace, product=product, run_id=run_id)
    failed = [e for e in events if e["event_type"] == EVENT_MAINTENANCE_FAILED]
    assert len(failed) == 1
    assert failed[0]["record"]["maintenance"]["error"] == "disk full"
    assert failed[0]["record"]["status"] == "failed"
    assert failed[0]["record"]["meta"]["op"] == MAINTENANCE_OP_ARCHIVE_RESTORE

    meta = _read_run_meta(temp_workspace, product=product, run_id=run_id)
    assert meta["status"] == "failed"
    assert meta.get("error") == "disk full"
    assert meta.get("completed_at")


def test_maintenance_op_validation_rejects_unknown_op(temp_workspace):
    product = "product.zarr"
    run_id = "maintenance-bogus"
    repo = ManifestRepository(binding=make_test_binding(temp_workspace), workspace=temp_workspace)

    with pytest.raises(ManifestError, match="Unsupported maintenance op"):
        repo.record_maintenance_started(
            product=product,
            run_id=run_id,
            op="bogus_op",
            scope_meta={},
        )

    with pytest.raises(ManifestError, match="Unsupported maintenance op"):
        repo.record_maintenance_completed(
            product=product,
            run_id=run_id,
            op="bogus_op",
        )

    with pytest.raises(ManifestError, match="Unsupported maintenance op"):
        repo.record_maintenance_failed(
            product=product,
            run_id=run_id,
            op="bogus_op",
            error="boom",
        )


def test_maintenance_run_visible_in_list_runs(temp_workspace):
    product = "product.zarr"
    run_id = "maintenance-scrub-004"
    repo = ManifestRepository(binding=make_test_binding(temp_workspace), workspace=temp_workspace)

    repo.record_maintenance_started(
        product=product,
        run_id=run_id,
        op=MAINTENANCE_OP_SCRUB,
        scope_meta={"group": "F024"},
    )
    repo.record_maintenance_completed(
        product=product,
        run_id=run_id,
        op=MAINTENANCE_OP_SCRUB,
        scope_meta={"group": "F024"},
    )

    runs = repo.list_runs(product=product)
    matching = [r for r in runs if r.run_id == run_id]
    assert len(matching) == 1
    assert matching[0].status == "complete"


def test_maintenance_facade_methods_on_chunk_manager(temp_workspace):
    from firecube.core.controlplane import ChunkManager

    product = "product.zarr"
    run_id = "maintenance-delete-facade-005"
    manager = ChunkManager(binding=make_test_binding(temp_workspace), workspace=temp_workspace)
    try:
        manager.record_maintenance_started(
            product=product,
            run_id=run_id,
            op=MAINTENANCE_OP_DELETE,
            scope_meta={"chunks_count": 1},
        )
        manager.record_maintenance_completed(
            product=product,
            run_id=run_id,
            op=MAINTENANCE_OP_DELETE,
            scope_meta={"chunks_count": 1},
        )

        events = _read_run_wal_events(temp_workspace, product=product, run_id=run_id)
        types = [e["event_type"] for e in events]
        assert EVENT_MAINTENANCE_STARTED in types
        assert EVENT_MAINTENANCE_COMPLETED in types

        runs = manager.list_runs(product=product)
        matching = [r for r in runs if r.run_id == run_id]
        assert len(matching) == 1
        assert matching[0].status == "complete"
    finally:
        manager.close()
