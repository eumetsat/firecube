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
from unittest.mock import patch

import numpy as np
import pandas as pd
import pytest
import xarray as xr

from firecube.core.filesystem.store_factory import ZarrStoreHandle
from firecube.ingestor.runtime.zarr.append_services import AppendWriteExecutor
from tests.helpers.storage import local_zarr_handle


def _make_ds(n=3, nlat=2, nlon=3):
    ts = pd.date_range("2024-01-01", periods=n, freq="h")
    data = np.zeros((n, nlat, nlon), dtype=np.float32)
    return xr.Dataset(
        {"FWI": (("timestamp", "lat", "lon"), data)},
        coords={"timestamp": ts, "lat": np.arange(nlat), "lon": np.arange(nlon)},
    )


def _executor(store, logger=None):
    handle = (
        local_zarr_handle(store)
        if store != "dummy"
        else ZarrStoreHandle(store="dummy", storage_options=None, target_uri="dummy")
    )
    return AppendWriteExecutor(
        zarr_store=handle,
        chunk_shape={"timestamp": 2, "lat": 2, "lon": 3},
        shard_shape=None,
        sharding=False,
        compression=False,
        append_dim="timestamp",
        logger=logger or logging.getLogger("test"),
    )


@pytest.mark.unit
class TestExecute:
    def test_write_creates_zarr_group(self, tmp_path):
        store = str(tmp_path / "exec.zarr")
        writer = _executor(store)
        ds = _make_ds(3)

        writer.execute(ds=ds, group="G1", mode="w")

        result = xr.open_zarr(store, group="G1", consolidated=False)
        assert result.sizes["timestamp"] == 3

    def test_append_mode_extends_store(self, tmp_path):
        store = str(tmp_path / "append.zarr")
        writer = _executor(store)
        ds1 = _make_ds(2)
        ds2 = _make_ds(2)
        ds2 = ds2.assign_coords(timestamp=pd.date_range("2024-01-01T02:00", periods=2, freq="h"))

        writer.execute(ds=ds1, group="G1", mode="w")
        writer.execute(ds=ds2, group="G1", mode="a")

        result = xr.open_zarr(store, group="G1", consolidated=False)
        assert result.sizes["timestamp"] == 4


@pytest.mark.unit
class TestCheckAlignment:
    def test_aligned_returns_true(self):
        writer = _executor("dummy")
        assert writer.check_alignment(start_i=0, count=2, chunk_len=2, group="G1") is True
        assert writer.check_alignment(start_i=4, count=2, chunk_len=2, group="G1") is True

    def test_unaligned_returns_false_and_warns(self):
        logger = logging.getLogger("test.alignment")
        writer = _executor("dummy", logger=logger)

        with patch.object(logger, "warning") as mock_warn:
            result = writer.check_alignment(start_i=0, count=3, chunk_len=2, group="G1")

        assert result is False
        assert mock_warn.call_count == 1
        assert "unaligned" in mock_warn.call_args[0][0].lower()

    def test_no_chunk_len_returns_true(self):
        writer = _executor("dummy")
        assert writer.check_alignment(start_i=1, count=3, chunk_len=None, group="G1") is True
        assert writer.check_alignment(start_i=1, count=3, chunk_len=0, group="G1") is True
