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


def _directzarr_shaped_store_and_span(tmp_path: Path, time_marks: dict | None = None):
    """Arrays under ``data/``; a span recorded the DirectZarr way (bare names, group in meta).

    ``time_marks`` adds a 300-slot ``time`` coordinate (chunk 256) carrying those attrs,
    a ``counts`` of time chunk 1, and a span of the single slot 5 that lists both.
    """
    import zarr

    from firecube.core.controlplane.deletion import DeletionEngine
    from firecube.core.controlplane.repo import ManifestRepository
    from firecube.core.controlplane.types import ChunkInfo
    from tests.helpers.storage import make_test_binding

    product = "product.zarr"
    store_root = tmp_path / product
    store_root.mkdir(parents=True, exist_ok=True)
    root = zarr.open_group(store=str(store_root), mode="w", zarr_format=3)
    arr = root.require_group("data").create_array(
        "counts",
        shape=(8, 2),
        chunks=(4, 2),
        dtype="f4",
        dimension_names=("timestamp", "x"),
        overwrite=True,
    )
    arr[:] = np.ones((8, 2), dtype=np.float32)
    span_record = {"arrays": ["counts"], "time_index_ranges": [[0, 3]], "aligned": True}
    if time_marks is not None:
        data = root["data"]
        counts = data.create_array(
            "counts",
            shape=(300, 2),
            chunks=(1, 2),
            dtype="f4",
            dimension_names=("time", "x"),
            overwrite=True,
        )
        counts[:] = np.ones((300, 2), dtype=np.float32)
        coord = data.create_array(
            "time", shape=(300,), chunks=(256,), dtype="i8", dimension_names=("time",)
        )
        coord[:] = np.arange(300)
        coord.attrs.update(time_marks)
        span_record = {
            "arrays": ["counts", "time"],
            "time_index_ranges": [[5, 5]],
            "aligned": True,
            "state_array": "data/firecube_timestamp_state",
            "time_dim_name": "time",
        }

    repo = ManifestRepository(
        binding=make_test_binding(tmp_path, product=product), workspace=tmp_path
    )
    span = ChunkInfo(
        key="span_run1_b1_data",
        product=product,
        chunk_type="span",
        size=0,
        timestamp=1.0,
        manifest_path=f"{repo.base_uri.rstrip('/')}/{product}/.firecube",
        meta={"group": "data"},
        record={"span": span_record},
    )
    return DeletionEngine(repo), span, store_root


def test_repro_span_bare_names_alignment(tmp_path: Path) -> None:
    engine, span, _store_root = _directzarr_shaped_store_and_span(tmp_path)

    result = engine.delete_spans([span], dry_run=True, force=False, update_manifest=False)

    assert result["errors"] == [], (
        f"bare array names must resolve under the span's group; errors={result['errors']}"
    )
    assert result["deleted_keys"] == 1, "chunk 0 of data/counts must be planned for deletion"


def test_repro_span_bare_names_delete(tmp_path: Path) -> None:
    engine, span, store_root = _directzarr_shaped_store_and_span(tmp_path)
    chunk_dir = store_root / "data" / "counts" / "c"

    result = engine.delete_spans([span], dry_run=False, force=False, update_manifest=False)

    assert result["errors"] == [], f"errors={result['errors']}"
    assert result["deleted_keys"] == 1
    assert not (chunk_dir / "0" / "0").exists(), "the span's chunk must be removed"
    assert (chunk_dir / "1" / "0").exists(), "the chunk of the other slots must stay"


@pytest.mark.unit
@pytest.mark.parametrize(
    ("present", "recorded", "removed"),
    [
        (["g/sub/x", "sub/x"], "sub/x", "g/sub/x"),
        (["g/g/x"], "g/x", "g/g/x"),
        (["g/x", "g/g/x"], "g/x", None),
    ],
    ids=["root-decoy-kept", "group-named-prefix", "both-present-refused"],
)
def test_delete_span_resolves_recorded_name_against_the_store(
    tmp_path: Path, present: list[str], recorded: str, removed: str | None
) -> None:
    import zarr

    from firecube.core.controlplane.deletion import DeletionEngine
    from firecube.core.controlplane.repo import ManifestRepository
    from firecube.core.controlplane.types import ChunkInfo
    from tests.helpers.storage import make_test_binding

    store_root = tmp_path / "product.zarr"
    root = zarr.open_group(store=str(store_root), mode="w", zarr_format=3)
    for path in present:
        arr = root.create_array(
            path, shape=(2, 2), chunks=(1, 2), dtype="f4", dimension_names=("timestamp", "x")
        )
        arr[:] = np.ones((2, 2), dtype=np.float32)
    repo = ManifestRepository(
        binding=make_test_binding(tmp_path, product="product.zarr"), workspace=tmp_path
    )
    span = ChunkInfo(
        key="span_run1_b1_g",
        product="product.zarr",
        chunk_type="span",
        size=0,
        timestamp=1.0,
        manifest_path=f"{repo.base_uri.rstrip('/')}/product.zarr/.firecube",
        meta={"group": "g"},
        record={"span": {"arrays": [recorded], "time_index_ranges": [[0, 0]], "aligned": True}},
    )

    result = DeletionEngine(repo).delete_spans(
        [span], dry_run=False, force=False, update_manifest=False
    )

    assert {p: (store_root / p / "c/0/0").exists() for p in present} == {
        p: p != removed for p in present
    }, "slot 0 chunk still stored, per array path"
    if removed is None:
        assert len(result["errors"]) == 1 and all(p in result["errors"][0] for p in present)
        assert result["deleted_spans"] == 0, "a refused span stays active"
    else:
        assert result["errors"] == []


@pytest.mark.unit
@pytest.mark.parametrize(
    ("marks", "outcome"),
    [
        ({"firecube_coord_managed": True}, "kept"),
        ({"firecube_preallocated": True}, "kept"),
        ({}, "misaligned"),
        ({"firecube_coord_managed": True, "firecube_preallocated": True}, "drift"),
    ],
    ids=["managed", "preallocated", "unmarked", "both-markers"],
)
def test_delete_span_keeps_managed_time_coordinate(
    tmp_path: Path, marks: dict, outcome: str
) -> None:
    from firecube.core.errors import SchemaDriftError

    engine, span, store_root = _directzarr_shaped_store_and_span(tmp_path, time_marks=marks)
    counts, time_dir = store_root / "data/counts/c", store_root / "data/time/c"
    time_objects = sorted(p.name for p in time_dir.iterdir())

    if outcome == "drift":
        with pytest.raises(SchemaDriftError):
            engine.delete_spans([span], dry_run=False, force=False, update_manifest=False)
        result = None
    else:
        result = engine.delete_spans([span], dry_run=False, force=False, update_manifest=False)

    if outcome == "kept":
        assert result is not None
        assert result["errors"] == [], f"a managed time coordinate is not measured; {result}"
        assert result["deleted_keys"] == 1
        assert not (counts / "5" / "0").exists(), "the span's counts chunk must be removed"
        assert (counts / "4" / "0").exists() and (counts / "6" / "0").exists()
    else:
        assert (counts / "5" / "0").exists(), "a refused deletion removes nothing"
        if result is not None:
            assert result["deleted_keys"] == 0
            assert any("not time-chunk aligned" in m for m in result["errors"]), result["errors"]
    assert sorted(p.name for p in time_dir.iterdir()) == time_objects, "time chunks must stay"
    import zarr

    values = zarr.open_group(str(store_root), mode="r", use_consolidated=False)["data/time"]
    assert list(values[:]) == list(range(300))
