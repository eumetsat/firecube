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

"""Tests for `_array_is_all_fill`.

Bytes/string and object dtypes are out of contract for this helper, so these
tests only exercise the numeric and datetime/timedelta fill semantics.
"""

from __future__ import annotations

import numpy as np
import pytest

from firecube.core.zarr.region_writer import _array_is_all_fill

pytestmark = pytest.mark.unit


def test_array_is_all_fill_float_nan_true() -> None:
    arr = np.full((5, 5), np.nan, dtype=np.float32)
    assert _array_is_all_fill(arr, np.float32("nan")) is True


def test_array_is_all_fill_float_nan_false_when_value_present() -> None:
    arr = np.full((5, 5), np.nan, dtype=np.float32)
    arr[0, 0] = 1.0
    assert _array_is_all_fill(arr, np.float32("nan")) is False


def test_array_is_all_fill_complex_nan_true() -> None:
    arr = np.full((3, 3), complex(np.nan, np.nan), dtype=np.complex128)
    assert _array_is_all_fill(arr, complex(float("nan"), float("nan"))) is True


def test_array_is_all_fill_datetime64_nat_true() -> None:
    arr = np.full((4,), np.datetime64("NaT", "s"), dtype="datetime64[s]")
    assert _array_is_all_fill(arr, np.datetime64("NaT", "s")) is True


def test_array_is_all_fill_timedelta64_nat_true() -> None:
    arr = np.full((4,), np.timedelta64("NaT", "s"), dtype="timedelta64[s]")
    assert _array_is_all_fill(arr, np.timedelta64("NaT", "s")) is True


def test_array_is_all_fill_integer_zero_true() -> None:
    arr = np.zeros((4, 4), np.int32)
    assert _array_is_all_fill(arr, 0) is True


def test_array_is_all_fill_integer_zero_false_when_value_present() -> None:
    arr = np.zeros((4, 4), np.int32)
    arr[0, 0] = 7
    assert _array_is_all_fill(arr, 0) is False


def test_array_is_all_fill_bool_false_true() -> None:
    arr = np.zeros((3,), np.bool_)
    assert _array_is_all_fill(arr, False) is True


def test_array_is_all_fill_empty_array_is_true() -> None:
    arr = np.zeros((0,), np.float32)
    assert _array_is_all_fill(arr, np.float32("nan")) is True


def test_array_is_all_fill_nan_fill_mixed_with_real_data_false() -> None:
    arr = np.array([1.0, np.nan, 3.0], np.float32)
    assert _array_is_all_fill(arr, np.float32("nan")) is False
