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

"""``AppendOrder`` behavior against a real on-disk calendar-valued time coordinate.

Before this fix, ``AppendOrder._maximum`` unconditionally used
``np.isnan(values)`` for any non-datetime64 decoded array. A stored time
coordinate on a non-standard calendar (e.g. ``360_day``) decodes to an object
array of ``cftime`` scalars, and ``np.isnan`` raises ``TypeError`` for object
dtype — this is the "second resume_existing ingest always crashes" defect.
These tests build a real local Zarr v3 store with an int64 time array carrying
``units``/``calendar`` attrs (the actual on-disk shape firecube writes for a
calendar time coordinate) and drive ``AppendOrder`` against it directly, no
mocks.
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pytest
import xarray as xr
import zarr

from firecube.core.filesystem.store_factory import ZarrStoreHandle
from firecube.ingestor.errors import AppendOverwriteRefused, InsertRefusedError
from firecube.ingestor.runtime.zarr.append_order import AppendOrder

pytestmark = pytest.mark.unit

_UNITS = "seconds since 2049-01-01"
_CALENDAR = "360_day"


def _make_store(tmp_path: Path, group: str, encoded: np.ndarray) -> ZarrStoreHandle:
    """Write a real int64 time array with units/calendar attrs under ``group/time``."""
    store_dir = tmp_path / "store.zarr"
    root = zarr.open_group(store=str(store_dir), mode="w", zarr_format=3)
    grp = root.create_group(group)
    array = grp.create_array("time", shape=encoded.shape, dtype="int64", chunks=encoded.shape)
    array[:] = encoded
    array.attrs["units"] = _UNITS
    array.attrs["calendar"] = _CALENDAR
    return ZarrStoreHandle(
        store=str(store_dir), storage_options=None, target_uri=f"file://{store_dir}"
    )


def _calendar_dataset(day_offsets: list[int], *, var_name: str = "time") -> xr.Dataset:
    """Build an in-memory dataset whose time coord is real cftime scalars (360_day)."""
    from firecube.core.zarr.time_decode import decode_time_array

    encoded = np.asarray([offset * 86400 for offset in day_offsets], dtype="int64")
    values = decode_time_array(encoded, {"units": _UNITS, "calendar": _CALENDAR})
    return xr.Dataset(coords={var_name: (var_name, values)})


def test_maximum_reads_sorted_calendar_coordinate(tmp_path: Path) -> None:
    """A sorted on-disk calendar coordinate decodes cleanly; maximum is the last cftime scalar."""
    encoded = np.array([0, 86400, 30 * 86400], dtype="int64")  # Jan 1, Jan 2, Feb 1 (360_day)
    handle = _make_store(tmp_path, "grp", encoded)

    boundary = AppendOrder()._maximum(handle, "grp", "time")

    assert boundary is not None
    assert boundary.length == 3
    assert boundary.maximum.calendar == "360_day"
    assert boundary.maximum.isoformat() == "2049-02-01T00:00:00"


def test_maximum_refuses_unsorted_calendar_coordinate_same_reason_as_datetime64(
    tmp_path: Path,
) -> None:
    """An unsorted calendar coordinate is refused with the SAME reason as an unsorted datetime64 one."""
    encoded = np.array([2 * 86400, 86400, 0], dtype="int64")  # descending
    handle = _make_store(tmp_path, "grp", encoded)

    with pytest.raises(AppendOverwriteRefused) as excinfo:
        AppendOrder()._maximum(handle, "grp", "time")

    assert excinfo.value.reason == "unsorted_existing_coord"


def test_maximum_refuses_datetime64_unsorted_coordinate_same_reason(tmp_path: Path) -> None:
    """Datetime64 control case: proves the calendar test above asserts the SAME error type/reason."""
    store_dir = tmp_path / "dt_store.zarr"
    root = zarr.open_group(store=str(store_dir), mode="w", zarr_format=3)
    grp = root.create_group("grp")
    array = grp.create_array("time", shape=(3,), dtype="int64", chunks=(3,))
    array[:] = np.array(
        [
            np.datetime64("2024-01-03").astype("datetime64[s]").astype("int64"),
            np.datetime64("2024-01-02").astype("datetime64[s]").astype("int64"),
            np.datetime64("2024-01-01").astype("datetime64[s]").astype("int64"),
        ],
        dtype="int64",
    )
    array.attrs["units"] = "seconds since 1970-01-01"
    array.attrs["calendar"] = "standard"
    handle = ZarrStoreHandle(
        store=str(store_dir), storage_options=None, target_uri=f"file://{store_dir}"
    )

    with pytest.raises(AppendOverwriteRefused) as excinfo:
        AppendOrder()._maximum(handle, "grp", "time")

    assert excinfo.value.reason == "unsorted_existing_coord"


def test_assert_append_accepts_incoming_after_existing_calendar_maximum(tmp_path: Path) -> None:
    """The exact second-ingest defect scenario: append after an existing calendar coordinate.

    Before the fix, resolving the existing store's boundary via ``_maximum``
    crashed with ``TypeError: ufunc 'isnan' not supported for the input
    types`` for any non-datetime64 decoded array. This must now succeed
    silently (no exception) when the incoming batch sorts strictly after the
    stored maximum.
    """
    encoded = np.array([0, 86400], dtype="int64")  # Jan 1, Jan 2 (360_day)
    handle = _make_store(tmp_path, "grp", encoded)
    incoming = _calendar_dataset([30, 31])  # Feb 1, Feb 2 -- strictly after Jan 2

    AppendOrder().assert_append(
        incoming, group="grp", time_dim="time", write_store=handle, resume_store=None
    )


def test_assert_append_refuses_insert_before_existing_calendar_maximum(tmp_path: Path) -> None:
    """An incoming calendar value at/before the stored maximum is refused as an insert."""
    encoded = np.array([0, 86400, 172800], dtype="int64")  # Jan 1..3 (360_day)
    handle = _make_store(tmp_path, "grp", encoded)
    incoming = _calendar_dataset([1])  # Jan 2 -- already covered by the stored maximum (Jan 3)

    with pytest.raises(InsertRefusedError) as excinfo:
        AppendOrder().assert_append(
            incoming, group="grp", time_dim="time", write_store=handle, resume_store=None
        )

    assert excinfo.value.reason == "insert"
