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

"""Behavioral tests for the ``CoordinateEncoding`` storage-encoding seam.

Covers `coordinate_encoding_for` (built from a time axis, at materialization
time) and `coordinate_encoding_from_array` (built from an opened array's
dtype/attrs, at write time): both dispatch on the same Gregorian-vs-encoded
rule and must agree, since one materializes a coordinate array and the other
reads it back.
"""

from __future__ import annotations

import numpy as np
import pytest

from firecube.core.index_spec import IrregularTimeAxis, RegularTimeAxis
from firecube.core.zarr.coord_materialization import (
    coordinate_encoding_for,
    coordinate_encoding_from_array,
)

pytestmark = pytest.mark.unit


def _gregorian_axis() -> RegularTimeAxis:
    return RegularTimeAxis(
        coordinate="time",
        epoch="2024-01-01T00:00:00Z",
        cadence_s=600,
        slot_count=10,
    )


def _360_day_axis() -> RegularTimeAxis:
    return RegularTimeAxis(
        coordinate="time",
        epoch="2049-01-01T00:00:00Z",
        cadence_s=86400,
        slot_count=90,
        calendar="360_day",
    )


def _float_units_irregular_axis() -> IrregularTimeAxis:
    return IrregularTimeAxis(
        coordinate="time",
        values=[71640.5, 71699.5, 71729.5],
        calendar="360_day",
        units="days since 1850-01-01",
    )


class TestCoordinateEncodingForGregorianAxis:
    def test_dtype_defaults_to_datetime64_ns_with_no_extra_attrs(self) -> None:
        encoding = coordinate_encoding_for(_gregorian_axis(), None)
        assert encoding.dtype == np.dtype("datetime64[ns]")
        assert encoding.extra_attrs == {}

    def test_fill_value_is_nat(self) -> None:
        encoding = coordinate_encoding_for(_gregorian_axis(), None)
        assert np.isnat(encoding.fill_value)

    def test_encode_values_converts_iso_strings(self) -> None:
        encoding = coordinate_encoding_for(_gregorian_axis(), None)
        result = encoding.encode_values(["2024-01-01T00:00:00Z", "2024-01-01T00:10:00Z"])
        assert result.dtype == np.dtype("datetime64[ns]")
        assert result[0] == np.datetime64("2024-01-01T00:00:00", "ns")
        assert result[1] == np.datetime64("2024-01-01T00:10:00", "ns")

    def test_encode_scalar_matches_encode_values(self) -> None:
        encoding = coordinate_encoding_for(_gregorian_axis(), None)
        scalar = encoding.encode_scalar("2024-01-01T00:10:00Z")
        assert scalar == np.datetime64("2024-01-01T00:10:00", "ns")

    def test_is_fill_is_an_elementwise_nat_mask(self) -> None:
        encoding = coordinate_encoding_for(_gregorian_axis(), None)
        arr = np.array(["2024-01-01", "NaT"], dtype="datetime64[ns]")
        mask = encoding.is_fill(arr)
        assert list(mask) == [False, True]

    def test_scalar_is_fill_detects_nat(self) -> None:
        encoding = coordinate_encoding_for(_gregorian_axis(), None)
        assert encoding.scalar_is_fill(np.datetime64("NaT", "ns")) is True
        assert encoding.scalar_is_fill(np.datetime64("2024-01-01", "ns")) is False

    def test_values_equal_treats_nat_as_equal_to_nat(self) -> None:
        encoding = coordinate_encoding_for(_gregorian_axis(), None)
        nat = np.datetime64("NaT", "ns")
        real = np.datetime64("2024-01-01T00:00:00", "ns")
        assert encoding.values_equal(nat, nat) is True
        assert encoding.values_equal(real, real) is True
        assert encoding.values_equal(real, nat) is False


class TestCoordinateEncodingFor360DayAxis:
    def test_dtype_and_extra_attrs(self) -> None:
        encoding = coordinate_encoding_for(_360_day_axis(), None)
        assert encoding.dtype == np.dtype("int64")
        assert encoding.extra_attrs == {
            "units": "seconds since 2049-01-01 00:00:00",
            "calendar": "360_day",
        }

    def test_fill_value_is_int64_min(self) -> None:
        encoding = coordinate_encoding_for(_360_day_axis(), None)
        assert encoding.fill_value == np.iinfo(np.int64).min

    def test_encode_values_is_dtype_cast(self) -> None:
        encoding = coordinate_encoding_for(_360_day_axis(), None)
        grid = np.arange(90, dtype=np.int64) * 86400
        result = encoding.encode_values(grid)
        assert result.dtype == np.dtype("int64")
        assert np.array_equal(result, grid)

    def test_encode_scalar_passes_through_already_encoded_numbers(self) -> None:
        encoding = coordinate_encoding_for(_360_day_axis(), None)
        assert encoding.encode_scalar(86400) == 86400

    def test_is_fill_is_an_elementwise_int64_min_mask(self) -> None:
        encoding = coordinate_encoding_for(_360_day_axis(), None)
        fill = np.iinfo(np.int64).min
        arr = np.array([0, fill, 86400], dtype=np.int64)
        mask = encoding.is_fill(arr)
        assert list(mask) == [False, True, False]

    def test_scalar_is_fill_detects_int64_min(self) -> None:
        encoding = coordinate_encoding_for(_360_day_axis(), None)
        fill = np.iinfo(np.int64).min
        assert encoding.scalar_is_fill(fill) is True
        assert encoding.scalar_is_fill(0) is False

    def test_values_equal_is_plain_equality(self) -> None:
        encoding = coordinate_encoding_for(_360_day_axis(), None)
        assert encoding.values_equal(86400, 86400) is True
        assert encoding.values_equal(86400, 0) is False


class TestCoordinateEncodingForFloatUnitsIrregularAxis:
    def test_dtype_and_extra_attrs(self) -> None:
        encoding = coordinate_encoding_for(_float_units_irregular_axis(), None)
        assert encoding.dtype == np.dtype("float64")
        assert encoding.extra_attrs == {
            "units": "days since 1850-01-01",
            "calendar": "360_day",
        }

    def test_fill_value_is_nan(self) -> None:
        encoding = coordinate_encoding_for(_float_units_irregular_axis(), None)
        assert np.isnan(encoding.fill_value)

    def test_encode_values_passes_through_floats(self) -> None:
        encoding = coordinate_encoding_for(_float_units_irregular_axis(), None)
        result = encoding.encode_values([71640.5, 71699.5])
        assert result.dtype == np.dtype("float64")
        assert np.array_equal(result, np.array([71640.5, 71699.5]))

    def test_is_fill_is_an_elementwise_nan_mask(self) -> None:
        encoding = coordinate_encoding_for(_float_units_irregular_axis(), None)
        arr = np.array([71640.5, np.nan])
        mask = encoding.is_fill(arr)
        assert list(mask) == [False, True]

    def test_scalar_is_fill_detects_nan(self) -> None:
        encoding = coordinate_encoding_for(_float_units_irregular_axis(), None)
        assert encoding.scalar_is_fill(float("nan")) is True
        assert encoding.scalar_is_fill(71640.5) is False


class TestCoordinateEncodingFromArrayRoundTrip:
    """``coordinate_encoding_from_array`` must agree with `coordinate_encoding_for`
    for the same coordinate: the materializer builds the array with the
    former, the region writer reads it back with the latter.
    """

    def test_gregorian_axis_round_trips(self) -> None:
        built = coordinate_encoding_for(_gregorian_axis(), None)
        round_tripped = coordinate_encoding_from_array(built.dtype, built.extra_attrs)
        assert round_tripped.dtype == built.dtype
        assert round_tripped.extra_attrs == built.extra_attrs
        assert np.isnat(round_tripped.fill_value)
        assert round_tripped.encode_scalar("2024-01-01T00:10:00Z") == built.encode_scalar(
            "2024-01-01T00:10:00Z"
        )

    def test_360_day_axis_round_trips(self) -> None:
        built = coordinate_encoding_for(_360_day_axis(), None)
        round_tripped = coordinate_encoding_from_array(built.dtype, built.extra_attrs)
        assert round_tripped.dtype == built.dtype
        assert round_tripped.extra_attrs == built.extra_attrs
        assert round_tripped.fill_value == built.fill_value
        assert round_tripped.encode_scalar(86400) == built.encode_scalar(86400)

    def test_float_units_irregular_axis_round_trips(self) -> None:
        built = coordinate_encoding_for(_float_units_irregular_axis(), None)
        round_tripped = coordinate_encoding_from_array(built.dtype, built.extra_attrs)
        assert round_tripped.dtype == built.dtype
        assert round_tripped.extra_attrs == built.extra_attrs
        assert np.isnan(round_tripped.fill_value)
        assert round_tripped.encode_scalar(71640.5) == built.encode_scalar(71640.5)

    def test_datetime64_dtype_is_always_gregorian_regardless_of_attrs(self) -> None:
        # A ``datetime64`` array is never an encoded coordinate, even if it
        # somehow carried units/calendar-shaped attrs: dtype.kind == "M" is
        # the first branch in `coordinate_encoding_from_array`.
        encoding = coordinate_encoding_from_array(
            np.dtype("datetime64[ns]"), {"units": "irrelevant", "calendar": "irrelevant"}
        )
        assert encoding.extra_attrs == {}
        assert np.isnat(encoding.fill_value)
