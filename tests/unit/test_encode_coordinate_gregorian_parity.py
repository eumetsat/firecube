"""Parity tests: `encode_coordinate` Gregorian-family values vs
`coerce_to_epoch_s` and xarray's `encode_cf_datetime`.

Covers the acceptance table for calendar in (proleptic_gregorian, standard,
gregorian) and mode in (floor, exact), across str/datetime/Timestamp/
np.datetime64/cftime Gregorian objects, plus the documented np.datetime64
exact-mode floor quirk and the cross-calendar-mismatch guard.
"""

from __future__ import annotations

import datetime as dt

import cftime
import numpy as np
import pandas as pd
import pytest
from xarray.coding.times import encode_cf_datetime

from firecube.core.encoded_time import encode_coordinate
from firecube.core.index_resolve import coerce_to_epoch_s
from firecube.core.slot_index import iso_to_epoch_s

pytestmark = pytest.mark.unit

UNITS = "seconds since 2024-01-01 00:00:00"
EPOCH_S = iso_to_epoch_s("2024-01-01T00:00:00Z")

GREGORIAN_CALENDARS = ("proleptic_gregorian", "standard", "gregorian")
MODES = ("floor", "exact")

NON_FRACTIONAL_VALUES = [
    "2024-06-15T12:30:00Z",
    "2024-06-15T12:30:00+00:00",
    pd.Timestamp("2024-06-15T12:30:00", tz="UTC"),
    dt.datetime(2024, 6, 15, 12, 30, 0),
    np.datetime64("2024-06-15T12:30:00", "s"),
    np.datetime64("2024-06-15T12:30:00.000", "ms"),
    np.datetime64("2024-06-15T12:30:00.000000000", "ns"),
    "1500-01-01T00:00:00Z",
]


def _as_datetime64(value: object) -> np.datetime64:
    """Normalize a str/datetime/Timestamp/np.datetime64 to np.datetime64."""
    if isinstance(value, np.datetime64):
        return value
    if isinstance(value, str):
        text = value[:-1] if value.endswith("Z") else value
        # Second resolution: "ns" overflows silently for years outside
        # about 1677 to 2262 (1500-01-01 would wrap to 2084).
        return np.datetime64(text, "s")
    if isinstance(value, pd.Timestamp):
        return value.tz_convert("UTC").tz_localize(None).to_datetime64()
    if isinstance(value, dt.datetime):
        return np.datetime64(value.replace(tzinfo=None), "ns")
    raise TypeError(f"unsupported value type: {type(value).__name__}")


@pytest.mark.parametrize("calendar", GREGORIAN_CALENDARS)
@pytest.mark.parametrize("mode", MODES)
@pytest.mark.parametrize("value", NON_FRACTIONAL_VALUES)
def test_encode_coordinate_matches_coerce_to_epoch_s(value, mode, calendar):
    result = encode_coordinate(value, units=UNITS, calendar=calendar, mode=mode)
    expected = coerce_to_epoch_s(value, mode=mode) - EPOCH_S
    assert result == expected


@pytest.mark.parametrize("calendar", GREGORIAN_CALENDARS)
@pytest.mark.parametrize("mode", MODES)
@pytest.mark.parametrize("value", NON_FRACTIONAL_VALUES)
def test_encode_coordinate_matches_xarray_encode_cf_datetime(value, mode, calendar):
    result = encode_coordinate(value, units=UNITS, calendar=calendar, mode=mode)
    dt64 = _as_datetime64(value)
    encoded_arr, _, _ = encode_cf_datetime(
        np.array([dt64]), units=UNITS, calendar="proleptic_gregorian"
    )
    assert result == encoded_arr[0]


CFTIME_VALUES = [
    cftime.DatetimeGregorian(2024, 6, 15, 12, 30, 0),
    cftime.DatetimeProlepticGregorian(2024, 6, 15, 12, 30, 0),
]


@pytest.mark.parametrize("calendar", GREGORIAN_CALENDARS)
@pytest.mark.parametrize("mode", MODES)
@pytest.mark.parametrize("value", CFTIME_VALUES)
def test_encode_coordinate_matches_xarray_for_cftime_gregorian_objects(value, mode, calendar):
    result = encode_coordinate(value, units=UNITS, calendar=calendar, mode=mode)
    encoded_arr, _, _ = encode_cf_datetime(
        np.array([value], dtype=object), units=UNITS, calendar="proleptic_gregorian"
    )
    assert result == encoded_arr[0]


FRACTIONAL_VALUES = [
    "2024-06-15T12:30:00.750Z",
    pd.Timestamp("2024-06-15T12:30:00.750", tz="UTC"),
    dt.datetime(2024, 6, 15, 12, 30, 0, 750000),
]
# Only these two types are checked for sub-second precision in exact mode;
# a str (like a numpy.datetime64) is truncated to whole seconds in both modes,
# as coerce_to_epoch_s has always done.
FRACTIONAL_VALUES_REJECTED_IN_EXACT_MODE = FRACTIONAL_VALUES[1:]


@pytest.mark.parametrize("calendar", GREGORIAN_CALENDARS)
@pytest.mark.parametrize("value", FRACTIONAL_VALUES)
def test_encode_coordinate_floor_mode_truncates_sub_second(value, calendar):
    result = encode_coordinate(value, units=UNITS, calendar=calendar, mode="floor")
    expected = coerce_to_epoch_s(value, mode="floor") - EPOCH_S
    assert result == expected


@pytest.mark.parametrize("calendar", GREGORIAN_CALENDARS)
@pytest.mark.parametrize("value", FRACTIONAL_VALUES_REJECTED_IN_EXACT_MODE)
def test_encode_coordinate_exact_mode_raises_on_sub_second(value, calendar):
    with pytest.raises(ValueError):
        encode_coordinate(value, units=UNITS, calendar=calendar, mode="exact")


@pytest.mark.parametrize("calendar", GREGORIAN_CALENDARS)
def test_encode_coordinate_exact_mode_iso_string_truncates_like_coerce(calendar):
    value = "2024-06-15T12:30:00.750Z"
    exact = encode_coordinate(value, units=UNITS, calendar=calendar, mode="exact")
    assert exact == coerce_to_epoch_s(value, mode="exact") - EPOCH_S


@pytest.mark.parametrize("calendar", GREGORIAN_CALENDARS)
def test_encode_coordinate_exact_mode_np_datetime64_still_floors(calendar):
    # Documented quirk: coerce_to_epoch_s's np.datetime64 branch has no mode
    # check and always floors to seconds via `.astype("datetime64[s]")`, so
    # mode="exact" does NOT raise for np.datetime64 inputs even though it
    # raises for the equivalent str/datetime/Timestamp value.
    value = np.datetime64("2024-06-15T12:30:00.750", "ms")
    floored = encode_coordinate(value, units=UNITS, calendar=calendar, mode="floor")
    exact = encode_coordinate(value, units=UNITS, calendar=calendar, mode="exact")
    assert exact == floored


def test_gregorian_value_on_non_gregorian_axis_still_raises_typeerror():
    with pytest.raises(TypeError):
        encode_coordinate(
            "2024-06-15T12:30:00Z",
            units="days since 1850-02-28",
            calendar="360_day",
            mode="exact",
        )
