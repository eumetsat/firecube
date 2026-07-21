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

"""W4.3: maintenance ops acquire write claims before mutating product state."""

from __future__ import annotations

import time
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pytest
import xarray as xr
from click.testing import CliRunner

from firecube.cli.main import cli
from firecube.core.controlplane import ChunkInfo, ChunkManager
from firecube.core.controlplane.types import DeletionPlan, WriteDomain
from firecube.core.errors import ManifestError
from firecube.core.storage.uri import StorageUri
from firecube.core.tensogram.converter import zarr_to_tgm
from firecube.core.zarr.scrub import run_scrub
from tests.helpers.storage import make_test_binding, make_test_session

pytestmark = pytest.mark.integration


def _local_env(tmp_path: Path) -> dict[str, str]:
    return {"FIRECUBE_STORAGE_TYPE": "local", "FIRECUBE_TARGET_PATH": str(tmp_path)}


def _claim_domain(product: str, group: str = "F024") -> WriteDomain:
    return WriteDomain(product=product, category="zarr_append", name=group)


def _claiming_manager(tmp_path: Path, product: str = "product.zarr") -> ChunkManager:
    workspace = tmp_path / "workspace"
    workspace.mkdir(exist_ok=True)
    binding = make_test_binding(tmp_path, product=product)
    return ChunkManager(binding=binding, workspace=workspace)


def _sample_chunk(product: str) -> ChunkInfo:
    return ChunkInfo(
        key="F024/FWI/c/0/0/0",
        product=product,
        chunk_type="chunk",
        size=1,
        timestamp=time.time(),
        manifest_path="",
    )


def _sample_span(product: str) -> ChunkInfo:
    return ChunkInfo(
        key="span-run-1-batch-1-F024",
        product=product,
        chunk_type="span",
        size=0,
        timestamp=time.time(),
        manifest_path="",
        meta={"group": "F024", "run_id": "run-1", "batch_id": "batch-1"},
        record={
            "span": {
                "arrays": ["F024/FWI"],
                "time_index_ranges": [[0, 0]],
                "aligned": True,
            }
        },
    )


def _make_archive(source_zarr: Path, archive_path: Path) -> None:
    dataset = xr.Dataset(
        {
            "FWI": (
                ["timestamp", "lat", "lon"],
                np.random.default_rng(7).random((2, 2, 2)).astype("float32"),
            )
        },
        coords={
            "timestamp": np.arange(2),
            "lat": np.linspace(-1.0, 1.0, 2, dtype="float32"),
            "lon": np.linspace(0.0, 1.0, 2, dtype="float32"),
        },
    )
    dataset.to_zarr(source_zarr, group="F024")
    zarr_to_tgm(str(source_zarr), str(archive_path), group="F024")


def test_execute_deletion_aborts_when_product_has_active_writer_claim(tmp_path: Path) -> None:
    product = "product.zarr"
    manager = _claiming_manager(tmp_path)
    claim = manager.acquire_claim(
        product=product,
        domain=_claim_domain(product),
        owner_id="running-ingest:F024",
    )
    try:
        plan = DeletionPlan(
            chunks=[_sample_chunk(product)],
            total_size=1,
            products_affected={product},
            manifest_files=set(),
        )

        with pytest.raises(ManifestError, match="firecube chunks runs abandon"):
            manager.execute_deletion(
                plan,
                delete_storage=True,
                delete_manifest=False,
                dry_run=False,
            )

        claims = manager.list_claims(product=product)
        assert [info.domain for info in claims] == [_claim_domain(product).identifier]
    finally:
        claim.release()
        manager.close()


def test_delete_spans_aborts_when_product_has_active_writer_claim(tmp_path: Path) -> None:
    product = "product.zarr"
    manager = _claiming_manager(tmp_path)
    claim = manager.acquire_claim(
        product=product,
        domain=_claim_domain(product),
        owner_id="running-ingest:F024",
    )
    try:
        with pytest.raises(ManifestError, match="firecube chunks runs abandon"):
            manager.delete_spans([_sample_span(product)], dry_run=False)

        claims = manager.list_claims(product=product)
        assert [info.domain for info in claims] == [_claim_domain(product).identifier]
    finally:
        claim.release()
        manager.close()


def test_scrub_aborts_when_product_has_active_writer_claim(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    product = "product.zarr"
    manager = _claiming_manager(tmp_path, product=product)
    claim = manager.acquire_claim(
        product=product,
        domain=_claim_domain(product),
        owner_id="running-ingest:F024",
    )
    session = make_test_session(tmp_path, product=product)

    # T3.4 migrated scrub to validate_group_with_fs (fs-aware seam) — mock that.
    monkeypatch.setattr(
        "firecube.core.zarr.scrub.validate_group_with_fs",
        lambda *args, **kwargs: SimpleNamespace(extra_chunks=[f"{product}/F024/FWI/c/0/0/0"]),
    )
    monkeypatch.setattr(
        ChunkManager,
        "list_chunks",
        lambda self, *args, **kwargs: [_sample_chunk(product)],
    )

    try:
        with pytest.raises(ManifestError, match="firecube chunks runs abandon"):
            run_scrub(session, "F024")
    finally:
        claim.release()
        manager.close()


def test_archive_restore_aborts_when_product_has_active_writer_claim(tmp_path: Path) -> None:
    runner = CliRunner()
    env = _local_env(tmp_path)
    source_zarr = tmp_path / "source.zarr"
    archive_path = tmp_path / "archive.tgm"
    restored = tmp_path / "restored.zarr"
    _make_archive(source_zarr, archive_path)

    manager = _claiming_manager(tmp_path)
    claim = manager.acquire_claim(
        product="restored.zarr",
        domain=_claim_domain("restored.zarr"),
        owner_id="running-ingest:F024",
    )
    try:
        result = runner.invoke(
            cli,
            [
                "archive",
                "restore",
                "--archive",
                StorageUri.from_local_path(archive_path).to_str(),
                "--target",
                StorageUri.from_local_path(restored).to_str(),
                "--storage-type",
                "local",
                "--storage-driver",
                "fsspec",
            ],
            env=env,
        )
        assert result.exit_code != 0
        assert "firecube chunks runs abandon" in result.output
    finally:
        claim.release()
        manager.close()
