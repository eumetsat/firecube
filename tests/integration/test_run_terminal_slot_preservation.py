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

"""Verify that slot_range and slot_group survive run-terminal recording.

T4: extend ``record_run_terminal`` across the three control-plane facade layers
so explicit slot kwargs are threaded down to ``_writer`` / ``_build_run_record``
and end up in the rewritten ``run.json``. Prior to this fix, the terminal record
relied entirely on resumed metadata; callers that override slot fields (or
recover from a torn ``run.json``) silently lost them.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from firecube.core.controlplane.manager import ChunkManager
from firecube.core.controlplane.repo import ManifestRepository
from firecube.core.product.identity import ProductIdentity
from firecube.core.storage.binding import StorageBinding
from firecube.core.storage.driver_config import StorageDriverConfig
from firecube.core.storage.uri import StorageUri


def _make_binding(tmp_path: Path) -> tuple[StorageBinding, str]:
    product = "product"
    product_uri = StorageUri.from_local_path(tmp_path / product)
    control_uri = product_uri.join(".firecube")
    binding = StorageBinding(
        identity=ProductIdentity(
            product_uri=product_uri,
            product_name=product,
            format="zarr",
            control_root_uri=control_uri,
        ),
        driver=StorageDriverConfig(driver="fsspec"),
    )
    return binding, product


def _run_meta_path(tmp_path: Path, product: str, run_id: str) -> Path:
    return tmp_path / product / ".firecube" / "runs" / run_id / "run.json"


def test_record_run_terminal_preserves_explicit_slot_kwargs(tmp_path: Path) -> None:
    """Terminal record retains slot_range/slot_group when explicitly passed."""
    binding, product = _make_binding(tmp_path)
    repo = ManifestRepository(binding=binding, workspace=tmp_path)
    try:
        run_id = "run-terminal-explicit"
        slot_range = (526080, 526128)
        slot_group = "SEVIRI_L15"

        repo.record_run_started(
            product=product,
            run_id=run_id,
            output_path=str(tmp_path / product),
            output_format="zarr",
            size=0,
            meta={"plugin": "test"},
            slot_range=slot_range,
            slot_group=slot_group,
        )

        repo.record_run_terminal(
            product=product,
            run_id=run_id,
            output_path=str(tmp_path / product),
            output_format="zarr",
            size=1,
            meta={"plugin": "test"},
            status="complete",
            slot_range=slot_range,
            slot_group=slot_group,
        )

        run_meta = json.loads(_run_meta_path(tmp_path, product, run_id).read_text(encoding="utf-8"))
        assert run_meta["slot_range"] == [526080, 526128]
        assert run_meta["slot_group"] == "SEVIRI_L15"
        assert run_meta["status"] == "complete"

        runs = repo.list_runs(product=product)
        target = next(r for r in runs if r.run_id == run_id)
        assert target.slot_range == (526080, 526128)
        assert target.slot_group == "SEVIRI_L15"
        assert target.status == "complete"
    finally:
        repo.close()


def test_chunk_manager_record_run_terminal_forwards_slot_kwargs(tmp_path: Path) -> None:
    """ChunkManager facade also accepts and threads slot_range/slot_group."""
    binding, product = _make_binding(tmp_path)
    manager = ChunkManager(binding=binding, workspace=tmp_path)
    try:
        run_id = "run-terminal-manager"
        slot_range = (0, 96)
        slot_group = "FCI_L1C"

        manager.record_run_started(
            product=product,
            run_id=run_id,
            output_path=str(tmp_path / product),
            output_format="zarr",
            size=0,
            meta={"plugin": "test"},
            slot_range=slot_range,
            slot_group=slot_group,
        )

        manager.record_run_terminal(
            product=product,
            run_id=run_id,
            output_path=str(tmp_path / product),
            output_format="zarr",
            size=1,
            meta={"plugin": "test"},
            status="complete",
            slot_range=slot_range,
            slot_group=slot_group,
        )

        runs = manager.list_runs(product=product)
        target = next(r for r in runs if r.run_id == run_id)
        assert target.slot_range == (0, 96)
        assert target.slot_group == "FCI_L1C"
    finally:
        manager.close()


def test_record_run_terminal_recovers_slot_fields_after_torn_run_json(
    tmp_path: Path,
) -> None:
    """Explicit slot kwargs win even when run.json is missing on resume.

    Simulates a torn write / race: ``run.json`` is deleted between the start
    and terminal records. The terminal call must still stamp the new
    ``run.json`` with the explicit slot fields, proving the kwarg path does
    not rely on the on-disk resume_meta carrying them.
    """
    binding, product = _make_binding(tmp_path)
    repo = ManifestRepository(binding=binding, workspace=tmp_path)
    try:
        run_id = "run-terminal-torn"
        slot_range = (526080, 526128)
        slot_group = "SEVIRI_L15"

        repo.record_run_started(
            product=product,
            run_id=run_id,
            output_path=str(tmp_path / product),
            output_format="zarr",
            size=0,
            meta={"plugin": "test"},
            slot_range=slot_range,
            slot_group=slot_group,
        )
        # Drop the cached writer so the terminal call rebuilds from disk.
        repo._writers.pop((product, run_id), None)

        # Simulate torn write: delete the run.json that was just stamped.
        run_meta_path = _run_meta_path(tmp_path, product, run_id)
        assert run_meta_path.exists(), "precondition: run.json was written"
        run_meta_path.unlink()

        repo.record_run_terminal(
            product=product,
            run_id=run_id,
            output_path=str(tmp_path / product),
            output_format="zarr",
            size=1,
            meta={"plugin": "test"},
            status="complete",
            slot_range=slot_range,
            slot_group=slot_group,
        )

        rewritten = json.loads(run_meta_path.read_text(encoding="utf-8"))
        assert rewritten["slot_range"] == [526080, 526128]
        assert rewritten["slot_group"] == "SEVIRI_L15"
    finally:
        repo.close()


@pytest.mark.parametrize("status", ["complete", "failed", "abandoned"])
def test_record_run_terminal_accepts_slot_kwargs_for_all_terminal_statuses(
    tmp_path: Path, status: str
) -> None:
    """All three terminal statuses accept and persist slot kwargs."""
    binding, product = _make_binding(tmp_path)
    repo = ManifestRepository(binding=binding, workspace=tmp_path)
    try:
        run_id = f"run-terminal-{status}"
        slot_range = (1000, 1064)
        slot_group = "MTG_FCI"

        repo.record_run_started(
            product=product,
            run_id=run_id,
            output_path=str(tmp_path / product),
            output_format="zarr",
            size=0,
            meta={"plugin": "test"},
            slot_range=slot_range,
            slot_group=slot_group,
        )

        repo.record_run_terminal(
            product=product,
            run_id=run_id,
            output_path=str(tmp_path / product),
            output_format="zarr",
            size=1,
            meta={"plugin": "test"},
            status=status,
            error="boom" if status != "complete" else None,
            slot_range=slot_range,
            slot_group=slot_group,
        )

        run_meta = json.loads(_run_meta_path(tmp_path, product, run_id).read_text(encoding="utf-8"))
        assert run_meta["slot_range"] == [1000, 1064]
        assert run_meta["slot_group"] == "MTG_FCI"
        assert run_meta["status"] == status
    finally:
        repo.close()
