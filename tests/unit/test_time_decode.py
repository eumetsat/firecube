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

"""Tests for firecube.core.zarr.time_decode."""

# pyright: reportMissingImports=false

import numpy as np
import pytest

from firecube.core.zarr.time_decode import decode_or_passthrough, decode_time_array

pytestmark = pytest.mark.unit


def test_datetime64_passthrough_preserves_resolution() -> None:
    values = np.array(["2023-12-01"], dtype="datetime64[ns]")

    out = decode_time_array(values, {})

    # Native resolution is preserved rather than coarsened to seconds.
    assert out.dtype == np.dtype("datetime64[ns]")
    assert out[0] == np.datetime64("2023-12-01", "ns")


def test_datetime64_subsecond_not_truncated() -> None:
    """Distinct sub-second timestamps must survive decode (no second-floor).

    Coverage bounds and dedup keys are derived from this output; collapsing
    sub-second values to the same second would corrupt dedup decisions.
    """
    values = np.array(
        ["2023-12-01T00:00:00.100", "2023-12-01T00:00:00.200"],
        dtype="datetime64[ns]",
    )

    out = decode_time_array(values, {})

    assert out[0] != out[1]
    assert out[0] == np.datetime64("2023-12-01T00:00:00.100", "ns")


def test_float64_seconds_since_epoch() -> None:
    values = np.array([1701388800.0])

    out = decode_time_array(values, {"units": "seconds since 1970-01-01"})

    assert str(out[0])[:10] == "2023-12-01"


def test_float64_days_since_2000() -> None:
    values = np.array([0.0, 1.0, 9.0])

    out = decode_time_array(
        values,
        {"units": "days since 2000-01-01", "calendar": "standard"},
    )

    assert str(out[0])[:10] == "2000-01-01"
    assert str(out[2])[:10] == "2000-01-10"


def test_int64_seconds_since_epoch() -> None:
    values = np.array([1701388800], dtype="int64")

    out = decode_time_array(values, {"units": "seconds since 1970-01-01"})

    assert str(out[0])[:10] == "2023-12-01"


def test_calendar_proleptic_gregorian() -> None:
    values = np.array([0.0, 1.0])

    out = decode_time_array(
        values,
        {"units": "days since 2000-01-01", "calendar": "proleptic_gregorian"},
    )

    # decode_cf_datetime selects a range-aware datetime64 resolution; assert the
    # kind rather than a fixed unit, which varies by xarray version.
    assert out.dtype.kind == "M"
    assert out[0] == np.datetime64("2000-01-01")


def test_no_units_raises_valueerror() -> None:
    values = np.array([1.0, 2.0])

    with pytest.raises(ValueError, match="units"):
        decode_time_array(values, {})


def test_units_without_since_raises() -> None:
    values = np.array([1.0, 2.0])

    with pytest.raises(ValueError, match="since"):
        decode_time_array(values, {"units": "kelvin"})


def test_unsupported_dtype_raises() -> None:
    values = np.array([1.0 + 2.0j], dtype=np.complex64)

    with pytest.raises(ValueError, match=r"complex64"):
        decode_time_array(values, {})


class TestDecodeOrPassthrough:
    """Six permutations of (dtype, attrs.units) covering the dispatch table."""

    def test_datetime64_no_units(self) -> None:
        values = np.array(["2023-12-01"], dtype="datetime64[ns]")
        out = decode_or_passthrough(values, {})
        assert out.dtype.kind == "M"
        assert out[0] == np.datetime64("2023-12-01", "ns")

    def test_datetime64_units_without_since(self) -> None:
        values = np.array(["2023-12-01"], dtype="datetime64[ns]")
        out = decode_or_passthrough(values, {"units": "kelvin"})
        assert out.dtype.kind == "M"

    def test_datetime64_units_with_since(self) -> None:
        values = np.array(["2023-12-01"], dtype="datetime64[ns]")
        out = decode_or_passthrough(values, {"units": "seconds since 1970-01-01"})
        assert out.dtype.kind == "M"
        assert out[0] == np.datetime64("2023-12-01", "ns")

    def test_numeric_no_units_passthrough(self) -> None:
        values = np.array([1.0, 2.0, 3.0], dtype="float64")
        out = decode_or_passthrough(values, {})
        assert out.dtype.kind == "f"
        assert out[0] == 1.0

    def test_numeric_units_without_since_raises(self) -> None:
        values = np.array([1.0, 2.0, 3.0], dtype="float64")
        with pytest.raises(ValueError, match="since"):
            decode_or_passthrough(values, {"units": "kelvin"})

    def test_numeric_units_with_since_decodes(self) -> None:
        values = np.array([0.0, 1.0], dtype="float64")
        out = decode_or_passthrough(
            values, {"units": "days since 2000-01-01", "calendar": "standard"}
        )
        assert out.dtype.kind == "M"
        assert str(out[0])[:10] == "2000-01-01"

    def test_numeric_units_with_since_but_malformed_raises(self) -> None:
        values = np.array([1.0, 2.0], dtype="float64")
        with pytest.raises(ValueError):
            decode_or_passthrough(values, {"units": "days since not-a-real-date"})
