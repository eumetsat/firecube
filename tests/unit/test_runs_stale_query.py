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
import os
import time
from pathlib import Path

import pytest

from firecube.core.controlplane import ChunkManager
from firecube.core.storage.uri import StorageUri
from tests.helpers.storage import make_test_binding

pytestmark = pytest.mark.unit


def _manager(tmp_path: Path, *, product: str = "product.zarr") -> ChunkManager:
    workspace = tmp_path / "workspace"
    workspace.mkdir(exist_ok=True)
    return ChunkManager(binding=make_test_binding(tmp_path, product=product), workspace=workspace)


def _runs_dir(tmp_path: Path, product: str) -> Path:
    return tmp_path / product / ".firecube" / "runs"


def _run_dir(tmp_path: Path, product: str, run_id: str) -> Path:
    return _runs_dir(tmp_path, product) / run_id


def _write_run_json(
    tmp_path: Path,
    *,
    product: str,
    run_id: str,
    status: str,
    updated_at: float,
    started_at: float | None = None,
    completed_at: float | None = None,
    stale_threshold_s: int = 3600,
) -> Path:
    run_dir = _run_dir(tmp_path, product, run_id)
    run_dir.mkdir(parents=True, exist_ok=True)
    payload = {
        "schema_version": "v2",
        "product": product,
        "run_id": run_id,
        "status": status,
        "run_dir": str(run_dir),
        "run_uri": StorageUri.from_local_path(run_dir).to_str(),
        "started_at": started_at if started_at is not None else updated_at,
        "updated_at": updated_at,
        "completed_at": completed_at,
        "events": 1,
        "parts": 1,
        "run_stale_threshold_s": stale_threshold_s,
    }
    (run_dir / "run.json").write_text(json.dumps(payload), encoding="utf-8")
    return run_dir


def _write_orphan_run_segment(
    tmp_path: Path,
    *,
    product: str,
    run_id: str,
    modified_at: float,
) -> Path:
    run_dir = _run_dir(tmp_path, product, run_id)
    run_dir.mkdir(parents=True, exist_ok=True)
    segment = run_dir / "events-0001.jsonl"
    segment.write_text('{"schema_version": "v2"}\n', encoding="utf-8")
    os.utime(segment, (modified_at, modified_at))
    return run_dir


def test_list_stale_runs_returns_empty_list_when_runs_dir_is_empty(tmp_path: Path) -> None:
    manager = _manager(tmp_path)
    try:
        assert manager.list_stale_runs(product="product.zarr") == []
    finally:
        manager.close()


def test_list_stale_runs_returns_only_stale_non_terminal_runs(tmp_path: Path) -> None:
    manager = _manager(tmp_path)
    product = "product.zarr"
    now = time.time()

    _write_run_json(
        tmp_path,
        product=product,
        run_id="stale-a",
        status="started",
        updated_at=now - 7200,
    )
    _write_run_json(
        tmp_path,
        product=product,
        run_id="stale-b",
        status="started",
        updated_at=now - 7200,
    )
    _write_run_json(
        tmp_path,
        product=product,
        run_id="fresh",
        status="started",
        updated_at=now - 60,
    )
    _write_run_json(
        tmp_path,
        product=product,
        run_id="complete",
        status="complete",
        updated_at=now - 7200,
        completed_at=now - 7200,
    )
    _write_run_json(
        tmp_path,
        product=product,
        run_id="failed",
        status="failed",
        updated_at=now - 7200,
        completed_at=now - 7200,
    )

    try:
        stale_runs = manager.list_stale_runs(product=product)

        assert len(stale_runs) == 2
        assert {run.run_id for run in stale_runs} == {"stale-a", "stale-b"}
        assert all(run.status == "started" for run in stale_runs)
    finally:
        manager.close()


def test_list_stale_runs_excludes_boundary_run_at_threshold(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    manager = _manager(tmp_path)
    product = "product.zarr"
    now = 1_000_000.0
    boundary_updated_at = now - 3600.0
    monkeypatch.setattr("firecube.core.controlplane.types.time.time", lambda: now)

    _write_run_json(
        tmp_path,
        product=product,
        run_id="boundary",
        status="started",
        updated_at=boundary_updated_at,
    )

    try:
        assert manager.list_stale_runs(product=product) == []
    finally:
        manager.close()


def test_list_stale_runs_includes_orphan_run_with_segments(tmp_path: Path) -> None:
    manager = _manager(tmp_path)
    product = "product.zarr"
    now = time.time()

    _write_orphan_run_segment(
        tmp_path,
        product=product,
        run_id="orphan-run",
        modified_at=now - 7200,
    )

    try:
        stale_runs = manager.list_stale_runs(product=product)

        assert len(stale_runs) == 1
        run = stale_runs[0]
        assert run.product == product
        assert run.run_id == "orphan-run"
        assert run.status == "orphaned"
        assert (
            run.run_dir
            == StorageUri.from_local_path(_run_dir(tmp_path, product, "orphan-run")).to_str()
        )
        assert (
            run.run_uri
            == StorageUri.from_local_path(_run_dir(tmp_path, product, "orphan-run")).to_str()
        )
        assert run.started_at == pytest.approx(now - 7200)
        assert run.updated_at == pytest.approx(now - 7200)
        assert run.completed_at is None
        assert run.events == 0
        assert run.parts == 1
        assert run.error == "missing_run_meta"
    finally:
        manager.close()
