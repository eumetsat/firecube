# Copyright 2025-2026 EUMETSAT
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Deletion alignment uses stored chunk lengths rather than requested lengths.

When re-ingesting into an existing Zarr store, alignment decisions must use
the STORED chunk length (from the existing Zarr metadata) rather than the
REQUESTED chunk_shape (from config). Zarr never changes an array's chunk
grid after creation, so the requested value is advisory only. The stored
value also propagates into ``SpanCoverage.chunk_len_used`` so downstream
deletion planning can measure alignment against the store's own chunk grid.
"""

from __future__ import annotations

import logging
import warnings
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
import xarray as xr

from firecube.core.controlplane.types import SpanCoverage, build_span_entry
from firecube.ingestor.runtime.zarr.append_services import (
    AppendCoverageBuilder,
    AppendResumeService,
    AppendTimestampState,
)
from firecube.ingestor.runtime.zarr.resume_cache import clear_resume_cache


@pytest.fixture(autouse=True)
def _clear_cache():
    clear_resume_cache()
    yield
    clear_resume_cache()


def _make_ds(
    n: int = 2, nlat: int = 2, nlon: int = 3, start: str = "2024-01-01T05:00"
) -> xr.Dataset:
    ts = pd.date_range(start, periods=n, freq="h")
    data = np.zeros((n, nlat, nlon), dtype=np.float32)
    return xr.Dataset(
        {"FWI": (("timestamp", "lat", "lon"), data)},
        coords={"timestamp": ts, "lat": np.arange(nlat), "lon": np.arange(nlon)},
    )


def _write_initial_store_with_chunk(
    store_path: Path,
    group: str,
    n_timestamps: int,
    chunk_len: int,
    nlat: int = 2,
    nlon: int = 3,
) -> None:
    ts = pd.date_range("2024-01-01", periods=n_timestamps, freq="h")
    ds = xr.Dataset(
        {
            "FWI": (
                ("timestamp", "lat", "lon"),
                np.zeros((n_timestamps, nlat, nlon), dtype=np.float32),
            )
        },
        coords={"timestamp": ts, "lat": np.arange(nlat), "lon": np.arange(nlon)},
    )
    with warnings.catch_warnings():
        warnings.filterwarnings(
            "ignore",
            message="Consolidated metadata is currently not part in the Zarr format 3 specification",
        )
        ds.to_zarr(
            str(store_path),
            group=group,
            mode="w",
            zarr_format=3,
            safe_chunks=False,
            encoding={"FWI": {"chunks": (chunk_len, nlat, nlon)}},
        )


def _svc(
    store_uri: str | None = None,
    *,
    resume_existing: bool = False,
    chunk_shape: dict[str, int] | None = None,
) -> AppendResumeService:
    return AppendResumeService(
        read_source_uri=store_uri,
        read_storage_options=None,
        resume_existing=resume_existing,
        append_dim="timestamp",
        chunk_shape=chunk_shape,
        shard_shape=None,
        sharding=False,
        logger=logging.getLogger("test.deletion_alignment"),
        state_var_name="firecube_timestamp_state",
    )


def _prepare_write(svc: AppendResumeService, ds: xr.Dataset, group: str, store: str) -> None:
    ts_state = AppendTimestampState("firecube_timestamp_state", time_dim_name="timestamp")
    ds = ts_state.attach(ds, append_dim="timestamp")
    svc.prepare_write(
        ds=ds,
        group=group,
        store=store,
        write_target_uri=store,
        arrays_for_group=None,
        ts_state=ts_state,
    )


@pytest.mark.unit
class TestStoredChunkLenPrecedence:
    def test_stored_chunk_len_takes_precedence_over_requested(self, tmp_path: Path) -> None:
        store_path = tmp_path / "existing_chunked.zarr"
        _write_initial_store_with_chunk(store_path, "G1", n_timestamps=5, chunk_len=5)
        store = str(store_path)

        svc = _svc(
            store_uri=store,
            resume_existing=True,
            chunk_shape={"timestamp": 10, "lat": 2, "lon": 3},
        )
        _prepare_write(svc, _make_ds(2), "G1", store)

        assert svc.chunk_len == 5, (
            "Stored chunk length must win over requested chunk_shape for existing groups. "
            f"Expected 5 (stored), got {svc.chunk_len} (which suggests requested=10 was used)."
        )

    def test_stored_chunk_len_used_when_no_requested_configured(self, tmp_path: Path) -> None:
        store_path = tmp_path / "existing_no_config.zarr"
        _write_initial_store_with_chunk(store_path, "G1", n_timestamps=6, chunk_len=3)
        store = str(store_path)

        svc = _svc(store_uri=store, resume_existing=True, chunk_shape=None)
        _prepare_write(svc, _make_ds(2, start="2024-01-01T06:00"), "G1", store)

        assert svc.chunk_len == 3

    def test_configured_chunk_len_used_for_new_group(self, tmp_path: Path) -> None:
        store = str(tmp_path / "new_group.zarr")
        svc = _svc(
            store_uri=store,
            chunk_shape={"timestamp": 7, "lat": 2, "lon": 3},
        )
        _prepare_write(svc, _make_ds(2), "G1", store)

        assert svc.chunk_len == 7


@pytest.mark.unit
class TestChunkLenUsedInCoverage:
    def test_chunk_len_used_recorded_in_coverage_from_stored(self, tmp_path: Path) -> None:
        store_path = tmp_path / "cov_chunked.zarr"
        _write_initial_store_with_chunk(store_path, "G1", n_timestamps=5, chunk_len=5)
        store = str(store_path)

        svc = _svc(
            store_uri=store,
            resume_existing=True,
            chunk_shape={"timestamp": 10, "lat": 2, "lon": 3},
        )
        ds = _make_ds(2)
        _prepare_write(svc, ds, "G1", store)
        svc.advance_cursor(2)

        cov = AppendCoverageBuilder(time_dim_name="timestamp")
        cov.record_batch(start_i=5, count=2, ds=ds, append_dim="timestamp", aligned=False)
        entry = cov.build_entry(
            group="G1",
            coverage_arrays=svc.coverage_arrays,
            state_var_name="firecube_timestamp_state",
            state_deleted_value=2,
            chunk_len_used=svc.chunk_len,
        )

        assert entry is not None
        assert entry["chunk_len_used"] == 5

    def test_chunk_len_used_absent_when_none(self) -> None:
        cov = AppendCoverageBuilder(time_dim_name="timestamp")
        ds = _make_ds(2)
        cov.record_batch(start_i=0, count=2, ds=ds, append_dim="timestamp", aligned=True)
        entry = cov.build_entry(
            group="G1",
            coverage_arrays=["G1/FWI"],
            state_var_name="firecube_timestamp_state",
            state_deleted_value=2,
            chunk_len_used=None,
        )

        assert entry is not None
        assert "chunk_len_used" not in entry, (
            "chunk_len_used must be omitted from the coverage entry when None "
            "so pre-Option-B WAL records remain byte-identical."
        )

    def test_span_coverage_roundtrips_chunk_len_used(self, tmp_path: Path) -> None:
        store_path = tmp_path / "roundtrip.zarr"
        _write_initial_store_with_chunk(store_path, "G1", n_timestamps=4, chunk_len=4)
        store = str(store_path)

        svc = _svc(
            store_uri=store,
            resume_existing=True,
            chunk_shape={"timestamp": 8, "lat": 2, "lon": 3},
        )
        ds = _make_ds(2)
        _prepare_write(svc, ds, "G1", store)
        svc.advance_cursor(2)

        cov = AppendCoverageBuilder(time_dim_name="timestamp")
        cov.record_batch(start_i=4, count=2, ds=ds, append_dim="timestamp", aligned=False)
        entry = cov.build_entry(
            group="G1",
            coverage_arrays=svc.coverage_arrays,
            state_var_name="firecube_timestamp_state",
            state_deleted_value=2,
            chunk_len_used=svc.chunk_len,
        )
        assert entry is not None

        span = SpanCoverage(
            group=entry["group"],
            arrays=entry["arrays"],
            time_index_ranges=entry["time_index_ranges"],
            aligned=entry["aligned"],
            state_array=entry["state_array"],
            state_deleted_value=entry["state_deleted_value"],
            time_min=entry.get("time_min"),
            time_max=entry.get("time_max"),
            time_dim_name=entry.get("time_dim_name"),
            chunk_len_used=entry.get("chunk_len_used"),
        )

        assert span.chunk_len_used == 4


@pytest.mark.unit
class TestBuildSpanEntryChunkLenUsed:
    def test_chunk_len_used_emitted_when_set(self) -> None:
        entry = build_span_entry(
            run_id="run-001",
            batch_id="b001",
            group="G1",
            meta={},
            arrays=["G1/FWI"],
            time_index_ranges=[[0, 4]],
            chunk_len_used=5,
        )

        assert entry["span"]["chunk_len_used"] == 5

    def test_chunk_len_used_omitted_when_none(self) -> None:
        entry = build_span_entry(
            run_id="run-001",
            batch_id="b001",
            group="G1",
            meta={},
            arrays=["G1/FWI"],
            time_index_ranges=[[0, 4]],
            chunk_len_used=None,
        )

        assert "chunk_len_used" not in entry["span"], (
            "chunk_len_used must be omitted from the WAL span payload when None "
            "so pre-Option-B WAL records remain byte-identical."
        )


@pytest.mark.unit
class TestDeletionMeasuresAlignmentFromStore:
    """Defect : `DeletionEngine.delete_spans` must guard on alignment
    measured from the STORED chunk grid per data array, not on the
    WAL-recorded `aligned` flag (which can be wrong when the requested
    chunk size disagreed with the stored one — defect ).
    """

    def test_deletion_uses_measured_alignment_not_wal(self, tmp_path: Path) -> None:
        import zarr

        from firecube.core.controlplane.deletion import DeletionEngine
        from firecube.core.controlplane.repo import ManifestRepository
        from firecube.core.controlplane.types import ChunkInfo
        from tests.helpers.storage import make_test_binding

        product = "product.zarr"
        store_root = tmp_path / product
        store_root.mkdir(parents=True, exist_ok=True)

        root = zarr.open_group(store=str(store_root), mode="w", zarr_format=3)
        grp = root.require_group("data")
        arr = grp.create_array(
            "counts",
            shape=(4, 2),
            chunks=(2, 2),
            dtype="f4",
            dimension_names=("timestamp", "x"),
            overwrite=True,
        )
        arr[:] = np.ones((4, 2), dtype=np.float32)

        repo = ManifestRepository(
            binding=make_test_binding(tmp_path, product=product),
            workspace=tmp_path,
        )
        engine = DeletionEngine(repo)

        span = ChunkInfo(
            key="span_run1_b1_data",
            product=product,
            chunk_type="span",
            size=0,
            timestamp=1.0,
            manifest_path=f"{repo.base_uri.rstrip('/')}/{product}/.firecube",
            meta={"group": "data"},
            record={
                "span": {
                    "arrays": ["data/counts"],
                    "time_index_ranges": [[0, 1]],
                    "aligned": False,
                }
            },
        )

        result = engine.delete_spans([span], dry_run=False, force=False, update_manifest=False)

        assert result["errors"] == [], (
            "WAL aligned=False must not block deletion when the stored chunk "
            f"grid measures aligned; errors={result['errors']}"
        )
        assert result["deleted_keys"] > 0, (
            "Deletion should have proceeded because range [0,1] aligns with "
            "the stored chunk_len=2 on the time dimension"
        )
        assert result["deleted_spans"] == 1

    def test_deletion_blocks_when_stored_grid_misaligns_despite_wal(self, tmp_path: Path) -> None:
        import zarr

        from firecube.core.controlplane.deletion import DeletionEngine
        from firecube.core.controlplane.repo import ManifestRepository
        from firecube.core.controlplane.types import ChunkInfo
        from tests.helpers.storage import make_test_binding

        product = "product.zarr"
        store_root = tmp_path / product
        store_root.mkdir(parents=True, exist_ok=True)

        root = zarr.open_group(store=str(store_root), mode="w", zarr_format=3)
        grp = root.require_group("data")
        arr = grp.create_array(
            "counts",
            shape=(6, 2),
            chunks=(3, 2),
            dtype="f4",
            dimension_names=("timestamp", "x"),
            overwrite=True,
        )
        arr[:] = np.ones((6, 2), dtype=np.float32)

        repo = ManifestRepository(
            binding=make_test_binding(tmp_path, product=product),
            workspace=tmp_path,
        )
        engine = DeletionEngine(repo)

        span = ChunkInfo(
            key="span_run1_b1_data",
            product=product,
            chunk_type="span",
            size=0,
            timestamp=1.0,
            manifest_path=f"{repo.base_uri.rstrip('/')}/{product}/.firecube",
            meta={"group": "data"},
            record={
                "span": {
                    "arrays": ["data/counts"],
                    "time_index_ranges": [[0, 1]],
                    "aligned": True,
                }
            },
        )

        result = engine.delete_spans([span], dry_run=False, force=False, update_manifest=False)

        assert result["deleted_keys"] == 0, (
            "Misaligned deletion (range [0,1] vs chunk_len=3) must not proceed "
            "even when WAL claims aligned=True"
        )
        assert any("not time-chunk aligned" in msg for msg in result["errors"]), (
            "Expected alignment error when stored grid disagrees with WAL flag; "
            f"errors={result['errors']}"
        )
