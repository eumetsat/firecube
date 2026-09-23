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

"""``AppendCoverageBuilder.record_batch`` for calendar-valued (cftime) time coords.

Before this fix, ``record_batch`` only updated ``time_min``/``time_max`` when
the decoded array's dtype was ``datetime64`` (``decoded.dtype.kind == "M"``);
a non-standard calendar decodes to an object array and was silently skipped,
so every run's coverage reported ``time_min: null, time_max: null`` even
though the batch held real dates. These tests use real ``xr.Dataset``
batches with an in-memory object-dtype ``cftime`` coordinate (the exact shape
a plugin's ``build_dataset()`` hands the engine before CF-encoding at write
time) -- no mocks.
"""

from __future__ import annotations

import numpy as np
import pytest
import xarray as xr

from firecube.core.zarr.time_decode import decode_time_array
from firecube.ingestor.runtime.zarr.append_services import AppendCoverageBuilder

pytestmark = pytest.mark.unit

_UNITS = "seconds since 2049-01-01"
_CALENDAR = "360_day"


def _calendar_values(day_offsets: list[int]) -> np.ndarray:
    encoded = np.asarray([offset * 86400 for offset in day_offsets], dtype="int64")
    return decode_time_array(encoded, {"units": _UNITS, "calendar": _CALENDAR})


def _ds(values: np.ndarray, *, dim: str = "time") -> xr.Dataset:
    return xr.Dataset(coords={dim: xr.DataArray(values, dims=[dim])})


def test_record_batch_calendar_object_array_records_bounds() -> None:
    """A calendar-valued batch (Jan 1 .. Feb 30, 360_day) reports real, non-null bounds."""
    values = _calendar_values([0, 1, 59])  # Jan 1, Jan 2, Feb 30
    builder = AppendCoverageBuilder(time_dim_name="time")

    builder.record_batch(start_i=0, count=3, ds=_ds(values), aligned=True)
    entry = builder.build_entry(
        group="default",
        coverage_arrays=["default/value"],
        state_var_name="firecube_timestamp_state",
        state_deleted_value=2,
    )

    assert entry is not None
    assert entry["time_min"] == "2049-01-01T00:00:00"
    assert entry["time_max"] == "2049-02-30T00:00:00"


def test_record_batch_calendar_object_array_merges_across_batches() -> None:
    """Two batches on the same calendar dimension merge into one min/max, like datetime64."""
    builder = AppendCoverageBuilder(time_dim_name="time")
    builder.record_batch(start_i=0, count=2, ds=_ds(_calendar_values([0, 1])), aligned=True)
    builder.record_batch(start_i=2, count=2, ds=_ds(_calendar_values([29, 30])), aligned=True)

    entry = builder.build_entry(
        group="default",
        coverage_arrays=["default/value"],
        state_var_name="firecube_timestamp_state",
        state_deleted_value=2,
    )

    assert entry is not None
    assert entry["time_min"] == "2049-01-01T00:00:00"
    assert entry["time_max"] == "2049-02-01T00:00:00"


def test_record_batch_calendar_missing_value_excluded_from_bounds() -> None:
    """A ``None`` slot in the calendar coordinate is excluded, not treated as a bound."""
    real_values = _calendar_values([0, 5])
    values = np.array([real_values[0], None, real_values[1]], dtype=object)
    builder = AppendCoverageBuilder(time_dim_name="time")

    builder.record_batch(start_i=0, count=3, ds=_ds(values), aligned=True)
    entry = builder.build_entry(
        group="default",
        coverage_arrays=["default/value"],
        state_var_name="firecube_timestamp_state",
        state_deleted_value=2,
    )

    assert entry is not None
    assert entry["time_min"] == "2049-01-01T00:00:00"
    assert entry["time_max"] == "2049-01-06T00:00:00"


def test_record_batch_calendar_all_missing_batch_records_nothing() -> None:
    """A batch whose calendar coordinate is entirely missing leaves bounds unset (defensive)."""
    values = np.array([None, float("nan")], dtype=object)
    builder = AppendCoverageBuilder(time_dim_name="time")

    builder.record_batch(start_i=0, count=2, ds=_ds(values), aligned=True)
    entry = builder.build_entry(
        group="default",
        coverage_arrays=["default/value"],
        state_var_name="firecube_timestamp_state",
        state_deleted_value=2,
    )

    assert entry is not None
    assert entry["time_min"] is None
    assert entry["time_max"] is None


def test_record_batch_mixes_datetime64_then_calendar_raises_value_error() -> None:
    """Mixing decoded kinds across batches for one dimension is refused loudly, not compared."""
    builder = AppendCoverageBuilder(time_dim_name="time")
    dt_values = np.array(["2024-01-01"], dtype="datetime64[s]")
    builder.record_batch(start_i=0, count=1, ds=_ds(dt_values), aligned=True)

    with pytest.raises(ValueError, match="mixes"):
        builder.record_batch(start_i=1, count=2, ds=_ds(_calendar_values([0, 1])), aligned=True)


def test_record_batch_non_calendar_object_array_is_legitimate_passthrough() -> None:
    """An object-dtype coordinate that is NOT calendar-valued stays silently skipped.

    Matches ``decode_or_passthrough``'s documented "Other dtypes -> passthrough"
    contract (a non-time append dimension may legitimately be object-dtype);
    only the calendar-valued case changed from silently-skipped to recorded.
    """
    values = np.array(["a", "b", "c"], dtype=object)
    builder = AppendCoverageBuilder(time_dim_name="time")

    builder.record_batch(start_i=0, count=3, ds=_ds(values), aligned=True)
    entry = builder.build_entry(
        group="default",
        coverage_arrays=["default/value"],
        state_var_name="firecube_timestamp_state",
        state_deleted_value=2,
    )

    assert entry is not None
    assert entry["time_min"] is None
    assert entry["time_max"] is None


def test_record_batch_numeric_without_units_still_skips_time_bounds() -> None:
    """Pins the existing passthrough contract for a non-time integer append dim (unchanged)."""
    builder = AppendCoverageBuilder(time_dim_name="time")
    ds = xr.Dataset(
        coords={"time": xr.DataArray(np.array([1, 2, 3], dtype="int64"), dims=["time"])}
    )

    builder.record_batch(start_i=0, count=3, ds=ds, aligned=True)
    entry = builder.build_entry(
        group="default",
        coverage_arrays=["default/value"],
        state_var_name="firecube_timestamp_state",
        state_deleted_value=2,
    )

    assert entry is not None
    assert entry["time_min"] is None
    assert entry["time_max"] is None
