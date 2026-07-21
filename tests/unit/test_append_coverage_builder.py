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

import numpy as np
import pandas as pd
import pytest
import xarray as xr

from firecube.ingestor.runtime.zarr.append_services import AppendCoverageBuilder


def _make_ds(timestamps, nlat=2, nlon=3):
    ts = pd.to_datetime(list(timestamps))
    data = np.zeros((len(ts), nlat, nlon), dtype=np.float32)
    return xr.Dataset(
        {"FWI": (("timestamp", "lat", "lon"), data)},
        coords={"timestamp": ts, "lat": np.arange(nlat), "lon": np.arange(nlon)},
    )


def _require_entry(entry):
    assert entry is not None
    return entry


@pytest.mark.unit
class TestRecordBatch:
    def test_single_batch_range(self):
        cov = AppendCoverageBuilder(time_dim_name="timestamp")
        ds = _make_ds(pd.date_range("2024-01-01", periods=5, freq="h"))
        cov.record_batch(start_i=0, count=5, ds=ds, append_dim="timestamp", aligned=True)

        entry = _require_entry(
            cov.build_entry(
                group="G1",
                coverage_arrays=["G1/FWI"],
                state_var_name="firecube_timestamp_state",
                state_deleted_value=2,
            )
        )
        assert entry["time_index_ranges"] == [[0, 4]]

    def test_multiple_batches_produce_separate_ranges(self):
        cov = AppendCoverageBuilder(time_dim_name="timestamp")
        ts1 = pd.date_range("2024-01-01", periods=3, freq="h")
        ts2 = pd.date_range("2024-01-01T03:00", periods=3, freq="h")

        cov.record_batch(start_i=0, count=3, ds=_make_ds(ts1), append_dim="timestamp", aligned=True)
        cov.record_batch(start_i=3, count=3, ds=_make_ds(ts2), append_dim="timestamp", aligned=True)

        entry = _require_entry(
            cov.build_entry(
                group="G1",
                coverage_arrays=["G1/FWI"],
                state_var_name="firecube_timestamp_state",
                state_deleted_value=2,
            )
        )
        assert entry["time_index_ranges"] == [[0, 2], [3, 5]]

    def test_time_bounds_tracked(self):
        cov = AppendCoverageBuilder(time_dim_name="timestamp")
        ts = pd.date_range("2024-06-15T08:00", periods=4, freq="h")
        cov.record_batch(start_i=0, count=4, ds=_make_ds(ts), append_dim="timestamp", aligned=True)

        entry = _require_entry(
            cov.build_entry(
                group="G1",
                coverage_arrays=["G1/FWI"],
                state_var_name="firecube_timestamp_state",
                state_deleted_value=2,
            )
        )
        assert pd.Timestamp(entry["time_min"]) == ts[0]
        assert pd.Timestamp(entry["time_max"]) == ts[-1]


@pytest.mark.unit
class TestBuildEntry:
    def test_returns_none_when_no_ranges(self):
        cov = AppendCoverageBuilder(time_dim_name="timestamp")
        entry = cov.build_entry(
            group="G1",
            coverage_arrays=["G1/FWI"],
            state_var_name="firecube_timestamp_state",
            state_deleted_value=2,
        )
        assert entry is None

    def test_aligned_field_true_when_all_aligned(self):
        cov = AppendCoverageBuilder(time_dim_name="timestamp")
        ds = _make_ds(pd.date_range("2024-01-01", periods=2, freq="h"))
        cov.record_batch(start_i=0, count=2, ds=ds, append_dim="timestamp", aligned=True)
        cov.record_batch(start_i=2, count=2, ds=ds, append_dim="timestamp", aligned=True)

        entry = _require_entry(
            cov.build_entry(
                group="G1",
                coverage_arrays=["G1/FWI"],
                state_var_name="firecube_timestamp_state",
                state_deleted_value=2,
            )
        )
        assert entry["aligned"] is True

    def test_aligned_field_false_when_any_unaligned(self):
        cov = AppendCoverageBuilder(time_dim_name="timestamp")
        ds = _make_ds(pd.date_range("2024-01-01", periods=3, freq="h"))
        cov.record_batch(start_i=0, count=3, ds=ds, append_dim="timestamp", aligned=True)
        cov.record_batch(start_i=3, count=3, ds=ds, append_dim="timestamp", aligned=False)

        entry = _require_entry(
            cov.build_entry(
                group="G1",
                coverage_arrays=["G1/FWI"],
                state_var_name="firecube_timestamp_state",
                state_deleted_value=2,
            )
        )
        assert entry["aligned"] is False

    def test_state_array_path(self):
        cov = AppendCoverageBuilder(time_dim_name="timestamp")
        ds = _make_ds(pd.date_range("2024-01-01", periods=2, freq="h"))
        cov.record_batch(start_i=0, count=2, ds=ds, append_dim="timestamp", aligned=True)

        entry = _require_entry(
            cov.build_entry(
                group="mygroup",
                coverage_arrays=["mygroup/FWI"],
                state_var_name="custom_state",
                state_deleted_value=3,
            )
        )
        assert entry["state_array"] == "mygroup/custom_state"
        assert entry["state_deleted_value"] == 3
