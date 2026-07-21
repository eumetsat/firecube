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

"""Tests for AppendCoverageBuilder.record_batch time decoding (1970 bug fix)."""

from __future__ import annotations

import numpy as np
import pytest
import xarray as xr

from firecube.ingestor.runtime.zarr.append_services import AppendCoverageBuilder


def _make_ds_float64_cf(n: int = 3) -> xr.Dataset:
    times = np.arange(n, dtype=np.float64)
    return xr.Dataset(
        coords={
            "time": xr.DataArray(
                times,
                dims=["time"],
                attrs={"units": "days since 2000-01-01", "calendar": "standard"},
            )
        }
    )


def _make_ds_datetime64() -> xr.Dataset:
    times = np.array(["2023-12-01", "2023-12-02", "2023-12-03"], dtype="datetime64[s]")
    return xr.Dataset(coords={"time": xr.DataArray(times, dims=["time"])})


def test_float64_cf_units_gives_correct_bounds() -> None:
    builder = AppendCoverageBuilder(time_dim_name="time")
    ds = _make_ds_float64_cf(n=10)  # 0..9 days since 2000-01-01 => 2000-01-01..2000-01-10
    builder.record_batch(start_i=0, count=10, ds=ds, aligned=True)
    entry = builder.build_entry(
        group="test",
        coverage_arrays=["time"],
        state_var_name="state",
        state_deleted_value=0,
    )
    assert entry is not None
    assert entry["time_min"] is not None
    assert "1970" not in str(entry["time_min"]), f"Got 1970: {entry['time_min']}"
    assert str(entry["time_min"]).startswith("2000-01-01"), (
        f"Expected 2000-01-01, got {entry['time_min']}"
    )
    assert str(entry["time_max"]).startswith("2000-01-10"), (
        f"Expected 2000-01-10, got {entry['time_max']}"
    )


def test_datetime64_still_works() -> None:
    builder = AppendCoverageBuilder(time_dim_name="time")
    ds = _make_ds_datetime64()
    builder.record_batch(start_i=0, count=3, ds=ds, aligned=True)
    entry = builder.build_entry(
        group="test",
        coverage_arrays=["time"],
        state_var_name="state",
        state_deleted_value=0,
    )
    assert entry is not None
    assert "2023-12-01" in str(entry["time_min"])
    assert "2023-12-03" in str(entry["time_max"])


def test_numeric_without_units_skips_time_bounds() -> None:
    builder = AppendCoverageBuilder(time_dim_name="time")
    ds = xr.Dataset(coords={"time": xr.DataArray(np.array([1.0, 2.0, 3.0]), dims=["time"])})
    builder.record_batch(start_i=0, count=3, ds=ds, aligned=True)
    entry = builder.build_entry(
        group="test", coverage_arrays=["time"], state_var_name="state", state_deleted_value=0
    )
    assert entry is not None
    assert entry["time_min"] is None


def test_malformed_cf_units_fails_loudly() -> None:
    """A malformed units/calendar must surface, not be swallowed.

    Silent except-swallowing in this builder previously hid the 1970-epoch
    coverage bug; DESIGN.md "Risks To Avoid" records that the bare-except was
    removed on purpose. Bad time metadata must fail loudly so the operator
    fixes it, rather than degrade coverage bounds silently.
    """
    builder = AppendCoverageBuilder(time_dim_name="time")
    ds = xr.Dataset(
        coords={
            "time": xr.DataArray(
                np.array([1.0, 2.0, 3.0]),
                dims=["time"],
                attrs={"units": "days since not-a-real-date", "calendar": "standard"},
            )
        }
    )
    with pytest.raises(ValueError, match="not-a-real-date"):
        builder.record_batch(start_i=0, count=3, ds=ds, aligned=True)


def test_subsecond_bounds_not_collapsed() -> None:
    """Distinct sub-second timestamps must yield distinct coverage bounds."""
    builder = AppendCoverageBuilder(time_dim_name="time")
    times = np.array(
        ["2023-12-01T00:00:00.100", "2023-12-01T00:00:00.900"],
        dtype="datetime64[ns]",
    )
    ds = xr.Dataset(coords={"time": xr.DataArray(times, dims=["time"])})
    builder.record_batch(start_i=0, count=2, ds=ds, aligned=True)
    entry = builder.build_entry(
        group="test", coverage_arrays=["time"], state_var_name="state", state_deleted_value=0
    )
    assert entry is not None
    assert entry["time_min"] != entry["time_max"]
    assert ".1" in str(entry["time_min"]) or "00.100" in str(entry["time_min"])
