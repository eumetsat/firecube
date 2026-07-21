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
from pathlib import Path

import pytest

from firecube.core.controlplane import ChunkManager
from tests.helpers.storage import make_test_binding

pytestmark = pytest.mark.unit


def _make_manager(tmp_path: Path, product: str = "product.zarr") -> ChunkManager:
    return ChunkManager(binding=make_test_binding(tmp_path, product=product), workspace=tmp_path)


def _write_run_meta(tmp_path: Path, *, product: str, run_id: str) -> None:
    run_dir = tmp_path / product / ".firecube" / "runs" / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    payload: dict[str, object] = {
        "schema_version": "v2",
        "product": product,
        "run_id": run_id,
        "status": "started",
        "parts": 0,
        "events": 0,
        "started_at": 1.0,
        "updated_at": 1.0,
        "run_uri": str(run_dir),
        "run_stale_threshold_s": 3600,
    }
    (run_dir / "run.json").write_text(json.dumps(payload), encoding="utf-8")


def _write_orphan_segment(tmp_path: Path, *, product: str, run_id: str) -> None:
    run_dir = tmp_path / product / ".firecube" / "runs" / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    event = {
        "schema_version": "v2",
        "event_type": "run_started",
        "record": {"key": f"run_{run_id}"},
        "timestamp": 1.0,
        "meta": {},
    }
    (run_dir / "events-000.jsonl").write_text(json.dumps(event) + "\n", encoding="utf-8")


def test_read_run_with_simple_id(tmp_path: Path) -> None:
    product = "product.zarr"
    run_id = "abc__slot=0-100"
    manager = _make_manager(tmp_path, product=product)
    try:
        _write_run_meta(tmp_path, product=product, run_id=run_id)

        runs = manager.list_runs(product=product)

        assert [run.run_id for run in runs] == [run_id]
    finally:
        manager.close()


def test_read_run_with_encoded_group_id(tmp_path: Path) -> None:
    product = "product.zarr"
    run_id = "abc__group=multires%2F0.5deg__slot=0-100"
    manager = _make_manager(tmp_path, product=product)
    try:
        _write_orphan_segment(tmp_path, product=product, run_id=run_id)

        runs = manager.list_runs(product=product)

        assert [run.run_id for run in runs] == [run_id]
        assert runs[0].status == "orphaned"
    finally:
        manager.close()


def test_legacy_phase3_run_id_still_reads(tmp_path: Path) -> None:
    product = "product.zarr"
    run_id = "base__slot=0-100"
    manager = _make_manager(tmp_path, product=product)
    try:
        _write_run_meta(tmp_path, product=product, run_id=run_id)

        runs = manager.list_runs(product=product)

        assert [run.run_id for run in runs] == [run_id]
    finally:
        manager.close()


def test_list_runs_returns_encoded_ids_intact(tmp_path: Path) -> None:
    product = "product.zarr"
    encoded_run_id = "abc__group=grp%2Fsub__slot=0-100"
    other_run_id = "abc__slot=100-200"
    manager = _make_manager(tmp_path, product=product)
    try:
        _write_run_meta(tmp_path, product=product, run_id=encoded_run_id)
        _write_run_meta(tmp_path, product=product, run_id=other_run_id)

        run_ids = {run.run_id for run in manager.list_runs(product=product)}

        assert run_ids == {encoded_run_id, other_run_id}
    finally:
        manager.close()
