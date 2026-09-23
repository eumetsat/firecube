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

"""``decode_time_array``/``missing_time_mask`` behavior for non-standard calendars.

A CF time array on a non-Gregorian calendar (``360_day``, ``noleap``, ...), or a
Gregorian calendar whose values fall outside the range ``datetime64`` can
represent, decodes to an object array of calendar-valued (``cftime``) scalars
rather than ``datetime64``. Downstream consumers (``AppendOrder``,
``AppendCoverageBuilder``) rely on ``missing_time_mask`` to detect missing
slots in every shape ``decode_or_passthrough`` can return, since numpy's
``isnan``/``isnat`` ufuncs reject object dtype outright
(``TypeError: ufunc 'isnan' not supported for the input types``).
"""

from __future__ import annotations

import numpy as np
import pytest

from firecube.core.zarr.time_decode import decode_time_array, missing_time_mask

pytestmark = pytest.mark.unit


def test_decode_time_array_360_day_returns_object_array_of_calendar_values() -> None:
    """A 360_day-encoded array decodes to an object array, not datetime64."""
    values = np.array([0, 86400, 30 * 86400 + 29 * 86400], dtype="int64")
    attrs = {"units": "seconds since 2049-01-01", "calendar": "360_day"}

    decoded = decode_time_array(values, attrs)

    assert decoded.dtype.kind == "O"
    assert str(decoded[0]) == "2049-01-01 00:00:00"
    # index 2 is day 59 (0-indexed) of a 360_day calendar: month 2 (30 days), day 30.
    assert decoded[2].month == 2
    assert decoded[2].day == 30
    assert decoded[2].calendar == "360_day"
    assert decoded[2].isoformat() == "2049-02-30T00:00:00"


def test_decode_time_array_out_of_range_gregorian_returns_object_array() -> None:
    """A standard-calendar value outside datetime64's range also decodes to an object array.

    xarray's CF decoder falls back to cftime for pre-1582 Gregorian dates
    (before datetime64[ns] can represent them), so "calendar-valued object
    array" is not exclusive to non-standard calendars.
    """
    values = np.array([0, 100_000], dtype="int64")
    attrs = {"units": "days since 1000-01-01", "calendar": "standard"}

    decoded = decode_time_array(values, attrs)

    assert decoded.dtype.kind == "O"
    assert decoded[0].calendar == "standard"
    assert callable(decoded[0].isoformat)


def test_decode_time_array_standard_in_range_still_returns_datetime64() -> None:
    """A representable standard-calendar value keeps the datetime64 fast path (no regression)."""
    values = np.array([0, 86400], dtype="int64")
    attrs = {"units": "seconds since 2049-01-01", "calendar": "standard"}

    decoded = decode_time_array(values, attrs)

    assert decoded.dtype.kind == "M"


def test_missing_time_mask_handles_datetime64_nat() -> None:
    values = np.array(["2024-01-01", "NaT", "2024-01-03"], dtype="datetime64[s]")

    mask = missing_time_mask(values)

    assert mask.tolist() == [False, True, False]


def test_missing_time_mask_handles_object_none_and_float_nan() -> None:
    """An object array's missing slot is ``None`` or a float ``NaN`` element."""
    calendar_values = decode_time_array(
        np.array([0, 86400, 172800, 259200], dtype="int64"),
        {"units": "seconds since 2049-01-01", "calendar": "360_day"},
    )
    values = np.array(
        [calendar_values[0], None, float("nan"), calendar_values[3]],
        dtype=object,
    )

    mask = missing_time_mask(values)

    assert mask.tolist() == [False, True, True, False]


def test_missing_time_mask_object_array_all_present_reports_no_missing() -> None:
    calendar_values = decode_time_array(
        np.array([0, 86400], dtype="int64"),
        {"units": "seconds since 2049-01-01", "calendar": "noleap"},
    )

    mask = missing_time_mask(calendar_values)

    assert mask.tolist() == [False, False]


def test_missing_time_mask_numeric_passthrough_reports_no_missing() -> None:
    """A bare numeric counter (no CF units, e.g. a non-time append dim) has no missing concept."""
    values = np.array([1, 2, 3], dtype="int64")

    mask = missing_time_mask(values)

    assert mask.tolist() == [False, False, False]
