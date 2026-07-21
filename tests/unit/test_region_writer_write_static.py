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

"""Tests for RegionZarrWriter.write_static."""

from __future__ import annotations

import tempfile
from typing import Any, cast

import numpy as np
import pytest
import zarr
from zarr.storage import LocalStore

from firecube.core.zarr.region_writer import RegionZarrWriter


def _make_writer(store_path: str) -> RegionZarrWriter:
    return RegionZarrWriter(f"file://{store_path}")


def _preallocate(
    store_path: str,
    group: str,
    name: str,
    shape: tuple[int, ...],
    dtype: str = "float64",
) -> None:
    """Create an array in the store for test setup."""
    store = LocalStore(store_path)
    root = zarr.open_group(store=store, mode="a", zarr_format=3)
    if f"{group}/{name}" not in root:
        root.require_group(group).create_array(name, shape=shape, dtype=dtype)


def test_static_2d_write() -> None:
    """write_static writes a 2D array correctly."""
    with tempfile.TemporaryDirectory() as tmp:
        _preallocate(tmp, "g", "lat", (4, 5))
        writer = _make_writer(tmp)
        data = np.arange(20, dtype="float64").reshape(4, 5)
        writer.write_static("g", "lat", data)
        root = zarr.open_group(store=LocalStore(tmp), mode="r", zarr_format=3)
        stored = np.asarray(cast(Any, root["g/lat"])[...])
        np.testing.assert_array_equal(stored, data)


def test_shape_mismatch_raises() -> None:
    """write_static raises ValueError on shape mismatch."""
    with tempfile.TemporaryDirectory() as tmp:
        _preallocate(tmp, "g", "lat", (4, 5))
        writer = _make_writer(tmp)
        data = np.zeros((3, 5), dtype="float64")
        with pytest.raises(ValueError, match="shape mismatch"):
            writer.write_static("g", "lat", data)


def test_idempotent_rewrite() -> None:
    """write_static can overwrite with identical data."""
    with tempfile.TemporaryDirectory() as tmp:
        _preallocate(tmp, "g", "lat", (4, 5))
        writer = _make_writer(tmp)
        data = np.ones((4, 5), dtype="float64")
        writer.write_static("g", "lat", data)
        writer.write_static("g", "lat", data)
        root = zarr.open_group(store=LocalStore(tmp), mode="r", zarr_format=3)
        stored = np.asarray(cast(Any, root["g/lat"])[...])
        np.testing.assert_array_equal(stored, data)
