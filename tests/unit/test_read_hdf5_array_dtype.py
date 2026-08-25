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

from pathlib import Path
from typing import Any

import h5py
import numpy as np
import pytest

from firecube.core.formats.hdf5 import read_hdf5_array

pytestmark = pytest.mark.unit


def _write_dataset(path: Path, values: np.ndarray, *, variable: str = "data") -> None:
    with h5py.File(path, "w") as handle:
        handle.create_dataset(variable, data=values)


def test_reads_int_dataset_preserves_int_dtype(tmp_path: Path) -> None:
    path = tmp_path / "int32.h5"
    _write_dataset(path, np.array([1, 2, 3], dtype=np.int32))

    result = read_hdf5_array(path, variable="data")

    assert result.dtype == np.dtype("int32")
    np.testing.assert_array_equal(result, np.array([1, 2, 3], dtype=np.int32))


def test_reads_float_dataset_preserves_float_dtype(tmp_path: Path) -> None:
    path = tmp_path / "float64.h5"
    _write_dataset(path, np.array([1.25, 2.5], dtype=np.float64))

    result = read_hdf5_array(path, variable="data")

    assert result.dtype == np.dtype("float64")


def test_explicit_dtype_kwarg_casts(tmp_path: Path) -> None:
    path = tmp_path / "cast.h5"
    _write_dataset(path, np.array([1, 2, 3], dtype=np.int32))

    result = read_hdf5_array(path, variable="data", dtype="float32")

    assert result.dtype == np.dtype("float32")
    np.testing.assert_array_equal(result, np.array([1, 2, 3], dtype=np.float32))


def test_xarray_fallback_preserves_dtype(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    path = tmp_path / "fallback.h5"
    _write_dataset(path, np.array([4, 5, 6], dtype=np.int32))
    original_file = h5py.File
    calls = 0

    def fail_first_h5py_open(*args: Any, **kwargs: Any) -> Any:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise OSError("forced h5py failure")
        return original_file(*args, **kwargs)

    monkeypatch.setattr(h5py, "File", fail_first_h5py_open)

    result = read_hdf5_array(path, variable="data")

    assert result.dtype == np.dtype("int32")
    np.testing.assert_array_equal(result, np.array([4, 5, 6], dtype=np.int32))


def test_reads_uint8_dataset_preserves_uint8(tmp_path: Path) -> None:
    path = tmp_path / "uint8.h5"
    _write_dataset(path, np.array([0, 255], dtype=np.uint8))

    result = read_hdf5_array(path, variable="data")

    assert result.dtype == np.dtype("uint8")


def test_reads_bool_dataset_preserves_bool(tmp_path: Path) -> None:
    path = tmp_path / "bool.h5"
    _write_dataset(path, np.array([True, False], dtype=np.bool_))

    result = read_hdf5_array(path, variable="data")

    assert result.dtype == np.dtype("bool")


def test_missing_variable_raises_keyerror(tmp_path: Path) -> None:
    path = tmp_path / "missing.h5"
    _write_dataset(path, np.array([1], dtype=np.int32), variable="other")

    with pytest.raises(KeyError, match="data"):
        read_hdf5_array(path, variable="data")
