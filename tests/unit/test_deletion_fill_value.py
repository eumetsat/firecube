# Copyright 2025-2026 EUMETSAT
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""The value span deletion writes into a slot must read back as missing.

A CF-encoded time array (numeric dtype, ``units`` containing ``since``) keeps
its zarr ``fill_value`` at ``0`` for integer dtypes, and ``0`` decodes to the
reference epoch -- a valid date. Deletion must instead write a sentinel that
CF readers decode to ``NaT``, or refuse when the dtype cannot carry one.
Each representable sentinel is proven by writing it into a real Zarr array and
decoding it through ``xr.open_zarr``.
"""

from __future__ import annotations

import base64
import struct
from pathlib import Path
from typing import Any, cast

import numpy as np
import pytest
import xarray as xr
import zarr

from firecube.core.controlplane.deletion import _fill_value_for_array_write

pytestmark = pytest.mark.unit

CALENDARS = ["standard", "gregorian", "proleptic_gregorian"]
UNITS = "seconds since 2000-01-01"
ARRAY_NAME = "t"
# Seconds after the reference epoch; none of them is a fill sentinel.
VALID_VALUES = [60, 120, 180]


def _store(tmp_path: Path) -> Path:
    return tmp_path / "fill.zarr"


def _create_array(
    tmp_path: Path,
    *,
    dtype: str,
    attrs: dict[str, Any],
    fill_value: Any = None,
) -> Any:
    root = zarr.open_group(str(_store(tmp_path)), mode="w", zarr_format=3)
    array = cast(
        Any,
        root.create_array(
            ARRAY_NAME,
            shape=(3,),
            chunks=(3,),
            dtype=dtype,
            fill_value=fill_value,
            dimension_names=("time",),
        ),
    )
    array.attrs.update(attrs)
    array[:] = np.array(VALID_VALUES, dtype=dtype)
    return array


def _write_slot_and_decode(tmp_path: Path, array: Any, value: Any) -> np.ndarray:
    """Write *value* into slot 1 the way deletion does and decode via xarray."""
    array[1:2] = value
    dataset = xr.open_zarr(str(_store(tmp_path)), consolidated=False)
    try:
        decoded = np.asarray(dataset[ARRAY_NAME].values)
    finally:
        dataset.close()
    return decoded


def _assert_only_slot_1_is_nat(decoded: np.ndarray) -> None:
    assert decoded.dtype.kind == "M", decoded.dtype
    assert np.isnat(decoded).tolist() == [False, True, False], decoded


def _b64_double(value: float) -> str:
    return base64.standard_b64encode(struct.pack("<d", value)).decode("ascii")


@pytest.mark.parametrize("calendar", CALENDARS)
def test_cf_int64_without_declared_fill_writes_int64_min_that_decodes_to_nat(
    tmp_path: Path, calendar: str
) -> None:
    """zarr fill 0 is a valid date; deletion must write a value CF decodes as NaT."""
    array = _create_array(
        tmp_path, dtype="int64", fill_value=0, attrs={"units": UNITS, "calendar": calendar}
    )

    value = _fill_value_for_array_write(array)

    assert np.asarray(value).dtype == np.dtype("int64")
    _assert_only_slot_1_is_nat(_write_slot_and_decode(tmp_path, array, value))


@pytest.mark.parametrize("calendar", CALENDARS)
def test_cf_int64_declared_fill_attr_wins(tmp_path: Path, calendar: str) -> None:
    """A declared ``_FillValue`` is the CF mask value, so it is the one written."""
    array = _create_array(
        tmp_path,
        dtype="int64",
        fill_value=0,
        attrs={"units": UNITS, "calendar": calendar, "_FillValue": -1},
    )

    value = _fill_value_for_array_write(array)

    _assert_only_slot_1_is_nat(_write_slot_and_decode(tmp_path, array, value))
    assert int(np.asarray(array[1])) == array.attrs["_FillValue"]


@pytest.mark.parametrize("calendar", CALENDARS)
def test_cf_float64_without_declared_fill_writes_nan(tmp_path: Path, calendar: str) -> None:
    """Float CF time with no declared ``_FillValue`` takes NaN, which decodes to NaT."""
    array = _create_array(tmp_path, dtype="float64", attrs={"units": UNITS, "calendar": calendar})

    value = _fill_value_for_array_write(array)

    _assert_only_slot_1_is_nat(_write_slot_and_decode(tmp_path, array, value))
    assert np.isnan(np.asarray(array[1]))


@pytest.mark.parametrize("calendar", CALENDARS)
@pytest.mark.parametrize("dtype", ["float64", "float32"])
def test_cf_float_declared_fill_attr_wins(tmp_path: Path, calendar: str, dtype: str) -> None:
    """A declared float ``_FillValue`` (base64 little-endian double) is the value written."""
    declared = -999.0
    array = _create_array(
        tmp_path,
        dtype=dtype,
        attrs={"units": UNITS, "calendar": calendar, "_FillValue": _b64_double(declared)},
    )

    value = _fill_value_for_array_write(array)

    _assert_only_slot_1_is_nat(_write_slot_and_decode(tmp_path, array, value))
    assert float(np.asarray(array[1])) == declared


@pytest.mark.parametrize(
    "declared",
    [-999.0, "not base64!", base64.standard_b64encode(struct.pack("<f", -999.0)).decode()],
    ids=["bare-number", "not-base64", "four-byte-payload"],
)
def test_cf_float_malformed_declared_fill_is_refused(tmp_path: Path, declared: Any) -> None:
    """A float ``_FillValue`` that is not a base64 little-endian double is refused."""
    array = _create_array(
        tmp_path,
        dtype="float64",
        attrs={"units": UNITS, "calendar": "standard", "_FillValue": declared},
    )

    with pytest.raises(ValueError) as excinfo:
        _fill_value_for_array_write(array)

    assert ARRAY_NAME in str(excinfo.value)
    assert "_FillValue" in str(excinfo.value)
    assert np.asarray(array[:]).tolist() == VALID_VALUES


@pytest.mark.parametrize("calendar", CALENDARS)
@pytest.mark.parametrize("dtype", ["int32", "uint32"])
def test_cf_time_dtype_without_nat_sentinel_is_refused(
    tmp_path: Path, calendar: str, dtype: str
) -> None:
    """No safe NaT sentinel exists for these dtypes without a declared ``_FillValue``."""
    array = _create_array(
        tmp_path, dtype=dtype, fill_value=0, attrs={"units": UNITS, "calendar": calendar}
    )

    with pytest.raises(ValueError) as excinfo:
        _fill_value_for_array_write(array)

    message = str(excinfo.value)
    assert ARRAY_NAME in message
    assert dtype in message
    assert UNITS in message
    assert "cannot represent NaT; declare _FillValue" in message
    assert np.asarray(array[:]).tolist() == VALID_VALUES


@pytest.mark.parametrize("attrs", [{}, {"units": "mm"}], ids=["no-units", "non-time-units"])
def test_non_cf_int_keeps_zarr_fill_value(tmp_path: Path, attrs: dict[str, Any]) -> None:
    """Without CF time units, the array's own zarr fill value is written."""
    array = _create_array(tmp_path, dtype="int32", fill_value=-9999, attrs=attrs)

    value = _fill_value_for_array_write(array)
    array[1:2] = value

    assert np.asarray(array[:]).tolist() == [60, array.fill_value, 180]
    assert int(np.asarray(array[1])) == -9999


def test_float_with_nan_fill_writes_nan(tmp_path: Path) -> None:
    """A NaN-declared float data array is filled with NaN."""
    array = _create_array(tmp_path, dtype="float32", fill_value=np.nan, attrs={})

    value = _fill_value_for_array_write(array)
    array[1:2] = value

    assert np.isnan(np.asarray(array[:])).tolist() == [False, True, False]


def test_datetime64_writes_nat(tmp_path: Path) -> None:
    """A native datetime64 array is filled with NaT."""
    array = _create_array(tmp_path, dtype="datetime64[s]", attrs={})

    value = _fill_value_for_array_write(array)

    _assert_only_slot_1_is_nat(_write_slot_and_decode(tmp_path, array, value))
    assert np.isnat(np.asarray(array[:])).tolist() == [False, True, False]
