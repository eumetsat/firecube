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

import warnings

import numpy as np
import pandas as pd
import pytest
import xarray as xr

from firecube.ingestor.runtime.zarr.append_services import AppendTimestampState
from firecube.ingestor.runtime.zarr.resume_cache import (
    ResumeCacheEntry,
    clear_resume_cache,
    get_resume_cache_entry,
)


@pytest.fixture(autouse=True)
def _clear_cache():
    clear_resume_cache()
    yield
    clear_resume_cache()


def _make_ds(n=3, nlat=2, nlon=3):
    ts = pd.date_range("2024-01-01", periods=n, freq="h")
    data = np.zeros((n, nlat, nlon), dtype=np.float32)
    return xr.Dataset(
        {"FWI": (("timestamp", "lat", "lon"), data)},
        coords={"timestamp": ts, "lat": np.arange(nlat), "lon": np.arange(nlon)},
    )


def _write_initial_store(store_path, group, n_timestamps):
    ts = pd.date_range("2024-01-01", periods=n_timestamps, freq="h")
    ds = xr.Dataset(
        {"FWI": (("timestamp", "lat", "lon"), np.zeros((n_timestamps, 2, 3), dtype=np.float32))},
        coords={"timestamp": ts, "lat": np.arange(2), "lon": np.arange(3)},
    )
    with warnings.catch_warnings():
        warnings.filterwarnings(
            "ignore",
            message="Consolidated metadata is currently not part in the Zarr format 3 specification",
        )
        ds.to_zarr(str(store_path), group=group, mode="w", zarr_format=3, safe_chunks=False)


@pytest.mark.unit
class TestAppendTimestampStateAttach:
    def test_attach_adds_state_var(self):
        svc = AppendTimestampState("firecube_timestamp_state", time_dim_name="timestamp")
        ds = _make_ds(3)
        result = svc.attach(ds, append_dim="timestamp")
        assert "firecube_timestamp_state" in result.data_vars

    def test_attach_custom_var_name(self):
        svc = AppendTimestampState("my_state", time_dim_name="timestamp")
        ds = _make_ds(2)
        result = svc.attach(ds, append_dim="timestamp")
        assert "my_state" in result.data_vars

    def test_attach_preserves_original_vars(self):
        svc = AppendTimestampState("firecube_timestamp_state", time_dim_name="timestamp")
        ds = _make_ds(3)
        result = svc.attach(ds, append_dim="timestamp")
        assert "FWI" in result.data_vars
        assert result.sizes["timestamp"] == 3


@pytest.mark.unit
class TestAppendTimestampStateEnsureExisting:
    def test_ensure_creates_state_array_for_legacy_store(self, tmp_path):
        store_path = tmp_path / "legacy.zarr"
        _write_initial_store(store_path, "G1", 4)
        svc = AppendTimestampState("firecube_timestamp_state", time_dim_name="timestamp")

        svc.ensure_existing(
            store_uri=str(store_path),
            group="G1",
            existing_time=4,
            chunk_len=2,
            cached=None,
            resume_cache_key=None,
            preexisting_values=None,
        )

        ds = xr.open_zarr(str(store_path), group="G1", consolidated=False)
        assert "firecube_timestamp_state" in ds

    def test_ensure_skips_when_already_initialized(self, tmp_path):
        store_path = tmp_path / "init.zarr"
        _write_initial_store(store_path, "G1", 3)
        svc = AppendTimestampState("firecube_timestamp_state", time_dim_name="timestamp")
        cached = ResumeCacheEntry(cursor=3, chunk_len=2, state_initialized=True)

        svc.ensure_existing(
            store_uri=str(store_path),
            group="G1",
            existing_time=3,
            chunk_len=2,
            cached=cached,
            resume_cache_key=None,
            preexisting_values=None,
        )

        ds = xr.open_zarr(str(store_path), group="G1", consolidated=False)
        assert "firecube_timestamp_state" not in ds

    def test_ensure_updates_resume_cache(self, tmp_path):
        store_path = tmp_path / "cache_update.zarr"
        _write_initial_store(store_path, "G1", 5)
        svc = AppendTimestampState("firecube_timestamp_state", time_dim_name="timestamp")
        cache_key = ("uri", "G1", "timestamp")

        svc.ensure_existing(
            store_uri=str(store_path),
            group="G1",
            existing_time=5,
            chunk_len=3,
            cached=None,
            resume_cache_key=cache_key,
            preexisting_values=frozenset(),
        )

        entry = get_resume_cache_entry(cache_key)
        assert entry is not None
        assert entry.state_initialized is True
        assert entry.cursor == 5

    def test_ensure_skips_when_no_store_uri(self):
        svc = AppendTimestampState("firecube_timestamp_state", time_dim_name="timestamp")
        svc.ensure_existing(
            store_uri=None,
            group="G1",
            existing_time=5,
            chunk_len=2,
            cached=None,
            resume_cache_key=None,
            preexisting_values=None,
        )
