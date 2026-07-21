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
from firecube.ingestor.runtime.parallel_run_id import derive_pod_run_id
from tests.helpers.storage import make_test_binding

pytestmark = pytest.mark.integration


def _make_manager(tmp_path: Path, product: str = "product.zarr") -> ChunkManager:
    return ChunkManager(binding=make_test_binding(tmp_path, product=product), workspace=tmp_path)


def _write_run_meta(
    tmp_path: Path,
    *,
    product: str,
    run_id: str,
    slot_range: tuple[int, int] | None = None,
    slot_group: str | None = None,
) -> None:
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
    if slot_range is not None:
        payload["slot_range"] = [slot_range[0], slot_range[1]]
    if slot_group is not None:
        payload["slot_group"] = slot_group
    (run_dir / "run.json").write_text(json.dumps(payload), encoding="utf-8")


def test_two_group_pods_distinct_run_records(tmp_path: Path) -> None:
    product = "product.zarr"
    manager = _make_manager(tmp_path, product=product)
    try:
        run_a = derive_pod_run_id("run", 0, 100, "group_a")
        run_b = derive_pod_run_id("run", 0, 100, "group_b")
        _write_run_meta(
            tmp_path, product=product, run_id=run_a, slot_range=(0, 100), slot_group="group_a"
        )
        _write_run_meta(
            tmp_path, product=product, run_id=run_b, slot_range=(0, 100), slot_group="group_b"
        )

        runs = manager.list_runs(product=product)
        assert {run.run_id for run in runs} == {run_a, run_b}
        assert len(runs) == 2
        assert {run.slot_group for run in runs} == {"group_a", "group_b"}
    finally:
        manager.close()


def test_existing_phase3_run_id_still_lists(tmp_path: Path) -> None:
    product = "product.zarr"
    manager = _make_manager(tmp_path, product=product)
    try:
        run_id = "run__slot=0-100"
        _write_run_meta(tmp_path, product=product, run_id=run_id, slot_range=(0, 100))

        runs = manager.list_runs(product=product)
        assert [run.run_id for run in runs] == [run_id]
        assert runs[0].slot_group is None
    finally:
        manager.close()
