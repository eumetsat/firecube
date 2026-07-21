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

"""Time-dimension resolution for span deletion.

Covers the per-span resolution chain in ``DeletionEngine.delete_spans``:
span-recorded ``time_dim_name`` > discovery from the 1-D timestamp-state
array > explicit caller-supplied name > engine default — plus the
conflict guard that refuses to delete along a contradicted axis and the
WAL round-trip of the recorded dim name.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import numpy as np
import pytest

from firecube.core.controlplane import ChunkManager, SpanCoverage
from firecube.core.controlplane.deletion import DeletionEngine
from firecube.core.controlplane.repo import ManifestRepository
from firecube.core.controlplane.types import ChunkInfo
from firecube.core.product.identity import ProductIdentity
from firecube.core.storage.binding import StorageBinding
from firecube.core.storage.driver_config import StorageDriverConfig
from firecube.core.storage.uri import StorageUri
from tests.helpers.storage import make_test_binding

pytestmark = pytest.mark.unit

PRODUCT = "product.zarr"


def _make_store(
    tmp_path: Path,
    *,
    dim: str = "time",
    with_state_array: bool = False,
) -> Path:
    """Create a real Zarr v3 store with one data array along ``dim``."""
    import zarr

    store_root = tmp_path / PRODUCT
    root = zarr.open_group(store=str(store_root), mode="w", zarr_format=3)
    grp = root.require_group("data")
    arr = grp.create_array(
        "counts",
        shape=(2, 2),
        chunks=(1, 2),
        dtype="f4",
        dimension_names=(dim, "x"),
        overwrite=True,
    )
    arr[:] = np.ones((2, 2), dtype=np.float32)
    if with_state_array:
        state = grp.create_array(
            "firecube_timestamp_state",
            shape=(2,),
            chunks=(2,),
            dtype="u1",
            dimension_names=(dim,),
            overwrite=True,
        )
        state[:] = np.ones((2,), dtype=np.uint8)
    return store_root


def _engine(tmp_path: Path) -> DeletionEngine:
    repo = ManifestRepository(
        binding=make_test_binding(tmp_path, product=PRODUCT),
        workspace=tmp_path,
    )
    return DeletionEngine(repo)


def _span(
    *,
    time_dim_name: str | None = None,
    state_array: str | None = None,
) -> ChunkInfo:
    spec: dict[str, Any] = {
        "arrays": ["data/counts"],
        "time_index_ranges": [[0, 0]],
        "aligned": True,
    }
    if time_dim_name is not None:
        spec["time_dim_name"] = time_dim_name
    if state_array is not None:
        spec["state_array"] = state_array
    return ChunkInfo(
        key="span_run1_b1_data",
        product=PRODUCT,
        chunk_type="span",
        size=0,
        timestamp=1.0,
        manifest_path="",
        record={"span": spec},
    )


def _delete(engine: DeletionEngine, span: ChunkInfo, **kwargs: Any) -> dict[str, Any]:
    return engine.delete_spans(
        [span],
        dry_run=False,
        update_manifest=False,
        update_state=False,
        **kwargs,
    )


def _chunk_path(tmp_path: Path) -> Path:
    return tmp_path / PRODUCT / "data" / "counts" / "c" / "0" / "0"


class TestSpanTimeDimResolution:
    def test_recorded_dim_resolves_custom_cube(self, tmp_path: Path) -> None:
        _make_store(tmp_path, dim="time")
        result = _delete(_engine(tmp_path), _span(time_dim_name="time"))
        assert result["errors"] == []
        assert result["deleted_keys"] == 1
        assert not _chunk_path(tmp_path).exists()

    def test_state_array_discovery_resolves_custom_cube(self, tmp_path: Path) -> None:
        _make_store(tmp_path, dim="time", with_state_array=True)
        span = _span(state_array="data/firecube_timestamp_state")
        result = _delete(_engine(tmp_path), span)
        assert result["errors"] == []
        assert result["deleted_keys"] == 1
        assert not _chunk_path(tmp_path).exists()

    def test_explicit_name_resolves_when_no_authority(self, tmp_path: Path) -> None:
        _make_store(tmp_path, dim="time")
        result = _delete(_engine(tmp_path), _span(), time_dim_name="time")
        assert result["errors"] == []
        assert result["deleted_keys"] == 1

    def test_no_authority_fails_loudly_with_remediation(self, tmp_path: Path) -> None:
        _make_store(tmp_path, dim="time")
        with pytest.raises(ValueError, match="--time-dim"):
            _delete(_engine(tmp_path), _span())
        assert _chunk_path(tmp_path).exists()

    def test_explicit_conflict_with_recorded_aborts_before_deletion(self, tmp_path: Path) -> None:
        _make_store(tmp_path, dim="time")
        with pytest.raises(ValueError, match="contradicts"):
            _delete(_engine(tmp_path), _span(time_dim_name="time"), time_dim_name="timestamp")
        assert _chunk_path(tmp_path).exists()

    def test_explicit_conflict_with_discovered_aborts_before_deletion(self, tmp_path: Path) -> None:
        _make_store(tmp_path, dim="time", with_state_array=True)
        span = _span(state_array="data/firecube_timestamp_state")
        with pytest.raises(ValueError, match="discovered"):
            _delete(_engine(tmp_path), span, time_dim_name="timestamp")
        assert _chunk_path(tmp_path).exists()

    def test_explicit_matching_recorded_is_accepted(self, tmp_path: Path) -> None:
        _make_store(tmp_path, dim="time")
        result = _delete(_engine(tmp_path), _span(time_dim_name="time"), time_dim_name="time")
        assert result["errors"] == []
        assert result["deleted_keys"] == 1

    def test_default_timestamp_cube_unchanged(self, tmp_path: Path) -> None:
        _make_store(tmp_path, dim="timestamp")
        result = _delete(_engine(tmp_path), _span())
        assert result["errors"] == []
        assert result["deleted_keys"] == 1
        assert not _chunk_path(tmp_path).exists()


class TestTimeDimNameWalRoundTrip:
    @staticmethod
    def _manager(workspace: Path) -> ChunkManager:
        product_uri = StorageUri.from_local_path(workspace / "__firecube_controlplane__")
        binding = StorageBinding(
            identity=ProductIdentity.from_uri(product_uri, "zarr", product_name="test_product"),
            driver=StorageDriverConfig(),
        )
        return ChunkManager(binding=binding, workspace=workspace)

    def _record_span(self, manager: ChunkManager, coverage: SpanCoverage) -> dict[str, Any]:
        manager.record_run_started(
            product="prod",
            run_id="run-1",
            output_path=str(manager.workspace / "prod"),
            output_format="zarr",
            size=0,
            meta={"plugin": "test"},
        )
        manager.record_span(
            product="prod",
            run_id="run-1",
            batch_id="b1",
            group="data",
            status="active",
            coverage=coverage,
            meta={"plugin": "test", "group": "data"},
        )
        manager.record_run_terminal(
            product="prod",
            run_id="run-1",
            output_path=str(manager.workspace / "prod"),
            output_format="zarr",
            size=1,
            meta={"plugin": "test"},
            status="complete",
        )
        spans = manager.list_chunks(product="prod", chunk_type="span")
        assert len(spans) == 1
        record = spans[0].record
        assert isinstance(record, dict)
        spec = record.get("span")
        assert isinstance(spec, dict)
        return spec

    def test_recorded_time_dim_round_trips(self, tmp_path: Path) -> None:
        spec = self._record_span(
            self._manager(tmp_path),
            SpanCoverage(
                group="data",
                arrays=["data/counts"],
                time_index_ranges=[[0, 1]],
                time_dim_name="time",
            ),
        )
        assert spec["time_dim_name"] == "time"

    def test_legacy_coverage_omits_time_dim_key(self, tmp_path: Path) -> None:
        spec = self._record_span(
            self._manager(tmp_path),
            SpanCoverage(
                group="data",
                arrays=["data/counts"],
                time_index_ranges=[[0, 1]],
            ),
        )
        assert "time_dim_name" not in spec
