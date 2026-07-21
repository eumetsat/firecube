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

import logging
import warnings

import numpy as np
import pandas as pd
import pytest
import xarray as xr

from firecube.ingestor.errors import ResumeConflictError
from firecube.ingestor.runtime.zarr.append_services import (
    AppendResumeService,
    AppendTimestampState,
)
from firecube.ingestor.runtime.zarr.resume_cache import (
    ResumeCacheEntry,
    clear_resume_cache,
    get_resume_cache_entry,
    put_resume_cache_entry,
)


@pytest.fixture(autouse=True)
def _clear_cache():
    clear_resume_cache()
    yield
    clear_resume_cache()


def _make_ds(n=3, nlat=2, nlon=3, var_name="FWI"):
    ts = pd.date_range("2024-01-01", periods=n, freq="h")
    data = np.zeros((n, nlat, nlon), dtype=np.float32)
    return xr.Dataset(
        {var_name: (("timestamp", "lat", "lon"), data)},
        coords={"timestamp": ts, "lat": np.arange(nlat), "lon": np.arange(nlon)},
    )


def _write_initial_store(store_path, group, n_timestamps, nlat=2, nlon=3):
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
        ds.to_zarr(str(store_path), group=group, mode="w", zarr_format=3, safe_chunks=False)


def _svc(store_uri=None, resume_existing=False, chunk_shape=None, shard_shape=None):
    return AppendResumeService(
        read_source_uri=store_uri,
        read_storage_options=None,
        resume_existing=resume_existing,
        append_dim="timestamp",
        chunk_shape=chunk_shape,
        shard_shape=shard_shape,
        sharding=False,
        logger=logging.getLogger("test"),
    )


@pytest.mark.unit
class TestPrepareWriteNewGroup:
    def test_new_group_sets_write_mode(self, tmp_path):
        store = str(tmp_path / "new.zarr")
        svc = _svc(store_uri=store)
        ds = _make_ds(3)
        ts_state = AppendTimestampState("firecube_timestamp_state", time_dim_name="timestamp")
        ds = ts_state.attach(ds, append_dim="timestamp")

        result = svc.prepare_write(
            ds=ds,
            group="G1",
            store=store,
            write_target_uri=store,
            arrays_for_group=None,
            ts_state=ts_state,
        )

        assert result is True
        assert svc.mode == "w"
        assert svc.write_cursor == 0
        assert svc.preexisting_values == frozenset()

    def test_new_group_infers_chunk_len_from_chunk_shape(self, tmp_path):
        store = str(tmp_path / "chunk.zarr")
        svc = _svc(store_uri=store, chunk_shape={"timestamp": 5, "lat": 2, "lon": 3})
        ds = _make_ds(3)
        ts_state = AppendTimestampState("firecube_timestamp_state", time_dim_name="timestamp")
        ds = ts_state.attach(ds, append_dim="timestamp")

        svc.prepare_write(
            ds=ds,
            group="G1",
            store=store,
            write_target_uri=store,
            arrays_for_group=None,
            ts_state=ts_state,
        )

        assert svc.chunk_len == 5

    def test_coverage_arrays_from_callback(self, tmp_path):
        store = str(tmp_path / "cov.zarr")
        svc = _svc(store_uri=store)
        ds = _make_ds(2)
        ts_state = AppendTimestampState("firecube_timestamp_state", time_dim_name="timestamp")
        ds = ts_state.attach(ds, append_dim="timestamp")

        svc.prepare_write(
            ds=ds,
            group="G1",
            store=store,
            write_target_uri=store,
            arrays_for_group=lambda g: [f"{g}/custom_arr"],
            ts_state=ts_state,
        )

        assert svc.coverage_arrays == ["G1/custom_arr"]


@pytest.mark.unit
class TestPrepareWriteExistingGroup:
    def test_existing_group_sets_append_mode(self, tmp_path):
        store_path = tmp_path / "existing.zarr"
        _write_initial_store(store_path, "G1", 5)
        store = str(store_path)
        svc = _svc(store_uri=store, resume_existing=True)
        ds = _make_ds(2)
        ds = ds.assign_coords(timestamp=pd.date_range("2024-01-01T05:00", periods=2, freq="h"))
        ts_state = AppendTimestampState("firecube_timestamp_state", time_dim_name="timestamp")
        ds = ts_state.attach(ds, append_dim="timestamp")

        svc.prepare_write(
            ds=ds,
            group="G1",
            store=store,
            write_target_uri=store,
            arrays_for_group=None,
            ts_state=ts_state,
        )

        assert svc.mode == "a"
        assert svc.write_cursor == 5

    def test_cursor_from_cache(self, tmp_path):
        store_path = tmp_path / "cached.zarr"
        _write_initial_store(store_path, "G1", 5)
        store = str(store_path)
        cache_key = (store, "G1", "timestamp")
        put_resume_cache_entry(
            cache_key,
            ResumeCacheEntry(cursor=10, chunk_len=4, state_initialized=True),
        )
        svc = _svc(store_uri=store)
        ds = _make_ds(2)
        ts_state = AppendTimestampState("firecube_timestamp_state", time_dim_name="timestamp")
        ds = ts_state.attach(ds, append_dim="timestamp")

        svc.prepare_write(
            ds=ds,
            group="G1",
            store=store,
            write_target_uri=store,
            arrays_for_group=None,
            ts_state=ts_state,
        )

        assert svc.write_cursor == 10
        assert svc.chunk_len == 4


@pytest.mark.unit
class TestOverlapDetection:
    def test_overlap_raises_resume_conflict(self, tmp_path):
        store_path = tmp_path / "overlap.zarr"
        _write_initial_store(store_path, "G1", 4)
        store = str(store_path)
        svc = _svc(store_uri=store, resume_existing=True)
        overlap_ts = pd.date_range("2024-01-01T02:00", periods=2, freq="h")
        ds = _make_ds(2)
        ds = ds.assign_coords(timestamp=overlap_ts)
        ts_state = AppendTimestampState("firecube_timestamp_state", time_dim_name="timestamp")
        ds = ts_state.attach(ds, append_dim="timestamp")

        with pytest.raises(ResumeConflictError, match="overlapping resume append"):
            svc.prepare_write(
                ds=ds,
                group="G1",
                store=store,
                write_target_uri=store,
                arrays_for_group=None,
                ts_state=ts_state,
            )


@pytest.mark.unit
class TestAdvanceAndCache:
    def test_advance_cursor_returns_start(self):
        svc = _svc()
        svc.write_cursor = 10
        start = svc.advance_cursor(5)
        assert start == 10
        assert svc.write_cursor == 15

    def test_update_cache_creates_entry(self, tmp_path):
        store = str(tmp_path / "cache.zarr")
        svc = _svc(store_uri=store)
        svc.resume_cache_key = (store, "G1", "timestamp")
        svc.write_cursor = 7
        svc.chunk_len = 3

        svc.update_cache_after_write()

        entry = get_resume_cache_entry(svc.resume_cache_key)
        assert entry is not None
        assert entry.cursor == 7
        assert entry.chunk_len == 3

    def test_update_cache_updates_existing(self, tmp_path):
        store = str(tmp_path / "cache2.zarr")
        cache_key = (store, "G1", "timestamp")
        put_resume_cache_entry(
            cache_key,
            ResumeCacheEntry(cursor=5, chunk_len=2, state_initialized=False),
        )
        svc = _svc(store_uri=store)
        svc.resume_cache_key = cache_key
        svc.write_cursor = 12
        svc.chunk_len = 4

        svc.update_cache_after_write()

        entry = get_resume_cache_entry(cache_key)
        assert entry is not None
        assert entry.cursor == 12
        assert entry.chunk_len == 4
