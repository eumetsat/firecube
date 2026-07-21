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
from typing import Any, cast

import numpy as np
import pandas as pd
import pytest
import xarray as xr

from firecube.ingestor.runtime.zarr.append_services import (
    AppendCoverageBuilder,
    AppendResumeService,
    AppendTimestampState,
    AppendWriteExecutor,
)


def _make_ds(dim_name: str = "time", n: int = 3, nlat: int = 2, nlon: int = 3) -> xr.Dataset:
    ts = pd.date_range("2024-01-01", periods=n, freq="h")
    data = np.zeros((n, nlat, nlon), dtype=np.float32)
    return xr.Dataset(
        {"FWI": ((dim_name, "lat", "lon"), data)},
        coords={dim_name: ts, "lat": np.arange(nlat), "lon": np.arange(nlon)},
    )


@pytest.mark.unit
class TestAppendServicesCustomDim:
    def test_timestamp_state_uses_constructor_time_dim(self):
        svc = AppendTimestampState("firecube_timestamp_state", time_dim_name="time")
        result = svc.attach(_make_ds())

        assert result["firecube_timestamp_state"].dims == ("time",)

    def test_coverage_builder_uses_constructor_time_dim(self):
        svc = AppendCoverageBuilder(time_dim_name="time")
        ds = _make_ds()

        svc.record_batch(start_i=0, count=3, ds=ds, aligned=True)

        entry = svc.build_entry(
            group="G1",
            coverage_arrays=["G1/FWI"],
            state_var_name="firecube_timestamp_state",
            state_deleted_value=2,
        )

        assert entry is not None
        assert pd.Timestamp(entry["time_min"]) == ds["time"].values[0]
        assert pd.Timestamp(entry["time_max"]) == ds["time"].values[-1]

    def test_write_executor_uses_constructor_time_dim(self):
        captured: dict[str, object] = {}

        def fake_write(ds, **kwargs):
            captured.update(kwargs)

        svc = AppendWriteExecutor(
            zarr_store=cast(Any, object()),
            chunk_shape=None,
            shard_shape=None,
            sharding=False,
            compression=False,
            append_dim="timestamp",
            logger=logging.getLogger(__name__),
            write_fn=fake_write,
            time_dim_name="time",
        )

        svc.execute(ds=_make_ds(), group="G1", mode="a")

        assert captured["append_dim"] == "time"

    def test_resume_service_uses_constructor_time_dim(self, monkeypatch):
        svc = AppendResumeService(
            read_source_uri="source.zarr",
            read_storage_options=None,
            resume_existing=False,
            append_dim="timestamp",
            chunk_shape=None,
            shard_shape=None,
            sharding=False,
            logger=logging.getLogger(__name__),
            time_dim_name="time",
        )
        monkeypatch.setattr(
            svc, "_read_metadata", lambda *args, **kwargs: (False, None, None, None)
        )

        ds = _make_ds()
        ts_state = AppendTimestampState("firecube_timestamp_state", time_dim_name="time")

        svc.prepare_write(
            ds=ds,
            group="G1",
            store=object(),
            write_target_uri=None,
            arrays_for_group=None,
            ts_state=ts_state,
        )

        assert svc.resume_cache_key == ("source.zarr", "G1", "time")
