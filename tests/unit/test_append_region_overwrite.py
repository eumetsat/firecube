#!/usr/bin/env python
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

import numpy as np
import pytest
import xarray as xr
import zarr

from firecube.core.config import StorageConfig
from firecube.core.filesystem.store_factory import create_zarr_store
from firecube.ingestor.errors import AppendOverwriteRefused
from firecube.ingestor.runtime.zarr.write import write_dataset_to_zarr

pytestmark = pytest.mark.unit


def _local_handle(store_path: Path, mode: str = "a"):
    return create_zarr_store(
        uri=str(store_path),
        storage_config=StorageConfig(storage_type="local", storage_driver="fsspec"),
        mode=mode,
    )


def _times(periods: int = 30, start: str = "2024-01-01") -> np.ndarray:
    offsets = np.arange(periods).astype("timedelta64[D]")
    return np.datetime64(start, "ns") + offsets


def _state(length: int, value: int = 1) -> np.ndarray:
    return np.full((length,), value, dtype=np.uint8)


def _dataset(
    values: np.ndarray,
    *,
    times: np.ndarray,
    state_value: int = 1,
) -> xr.Dataset:
    return xr.Dataset(
        {
            "value": (("timestamp",), values),
            "firecube_timestamp_state": (("timestamp",), _state(len(times), state_value)),
        },
        coords={"timestamp": times},
    )


def _grid_dataset(
    values: np.ndarray,
    *,
    times: np.ndarray,
    lat: np.ndarray | None = None,
    lon: np.ndarray | None = None,
) -> xr.Dataset:
    return xr.Dataset(
        {
            "value": (("timestamp", "y"), values),
            "lat": (("y",), np.asarray([45.0, 46.0], dtype=np.float32) if lat is None else lat),
            "lon": (("y",), np.asarray([7.0, 8.0], dtype=np.float32) if lon is None else lon),
            "firecube_timestamp_state": (("timestamp",), _state(len(times))),
        },
        coords={"timestamp": times},
    )


def _write_initial(
    store_path: Path, ds: xr.Dataset, chunk_shape: dict[str, int] | None = None
) -> None:
    write_dataset_to_zarr(
        ds,
        zarr_store=_local_handle(store_path, mode="w"),
        group="G",
        mode="w",
        chunk_shape=chunk_shape or {"timestamp": 15},
    )


def _region_write(store_path: Path, ds: xr.Dataset, region: slice) -> None:
    write_dataset_to_zarr(
        ds,
        zarr_store=_local_handle(store_path, mode="a"),
        group="G",
        region=region,
        time_dim="timestamp",
    )


def _open_group(store_path: Path):
    return zarr.open_group(store=str(store_path), mode="r", zarr_format=3)["G"]


def test_region_overwrite_values_correct(tmp_path: Path) -> None:
    store_path = tmp_path / "region-values.zarr"
    times = _times()
    initial_values = np.arange(30, dtype=np.int32)
    _write_initial(store_path, _dataset(initial_values, times=times))

    region = slice(10, 21)
    replacement = np.arange(100, 111, dtype=np.int32)
    _region_write(store_path, _dataset(replacement, times=times[region]), region)

    actual = np.asarray(_open_group(store_path)["value"][:])  # type: ignore[index]  # zarr dynamic typing
    expected = initial_values.copy()
    expected[region] = replacement
    np.testing.assert_array_equal(actual[:10], expected[:10])
    np.testing.assert_array_equal(actual[10:21], replacement)
    np.testing.assert_array_equal(actual[21:], expected[21:])


def test_region_overwrite_state_set_to_1(tmp_path: Path) -> None:
    store_path = tmp_path / "region-state.zarr"
    times = _times()
    _write_initial(store_path, _dataset(np.arange(30, dtype=np.int32), times=times, state_value=2))

    region = slice(10, 20)
    _region_write(store_path, _dataset(np.arange(10, dtype=np.int32), times=times[region]), region)

    state = np.asarray(_open_group(store_path)["firecube_timestamp_state"][:])  # type: ignore[index]  # zarr dynamic typing
    np.testing.assert_array_equal(state[:10], _state(10, 2))
    np.testing.assert_array_equal(state[10:20], _state(10, 1))
    np.testing.assert_array_equal(state[20:], _state(10, 2))


def test_time_coord_mismatch_raises(tmp_path: Path) -> None:
    store_path = tmp_path / "region-time-mismatch.zarr"
    times = _times()
    _write_initial(store_path, _dataset(np.arange(30, dtype=np.int32), times=times))

    region = slice(14, 25)
    wrong_times = times[1:12]

    with pytest.raises(AppendOverwriteRefused) as exc_info:
        _region_write(
            store_path,
            _dataset(np.arange(11, dtype=np.int32), times=wrong_times),
            region,
        )

    assert exc_info.value.reason == "time_coord_mismatch"


def test_static_arrays_excluded_from_region_write(tmp_path: Path) -> None:
    store_path = tmp_path / "region-static.zarr"
    times = _times()
    initial_values = np.arange(60, dtype=np.int32).reshape(30, 2)
    _write_initial(
        store_path,
        _grid_dataset(initial_values, times=times),
        chunk_shape={"timestamp": 15, "y": 2},
    )
    group = _open_group(store_path)
    lat_before = np.asarray(group["lat"][:]).tobytes()  # type: ignore[index]  # zarr dynamic typing
    lon_before = np.asarray(group["lon"][:]).tobytes()  # type: ignore[index]  # zarr dynamic typing

    region = slice(10, 21)
    replacement = np.full((11, 2), 500, dtype=np.int32)
    _region_write(
        store_path,
        _grid_dataset(
            replacement,
            times=times[region],
            lat=np.asarray([99.0, 100.0], dtype=np.float32),
            lon=np.asarray([101.0, 102.0], dtype=np.float32),
        ),
        region,
    )

    group = _open_group(store_path)
    assert np.asarray(group["lat"][:]).tobytes() == lat_before  # type: ignore[index]  # zarr dynamic typing
    assert np.asarray(group["lon"][:]).tobytes() == lon_before  # type: ignore[index]  # zarr dynamic typing
    np.testing.assert_array_equal(np.asarray(group["value"][:])[region], replacement)  # type: ignore[index]  # zarr dynamic typing


def test_state_array_excluded_from_xarray_write(tmp_path: Path) -> None:
    store_path = tmp_path / "region-state-xarray-excluded.zarr"
    times = _times()
    _write_initial(store_path, _dataset(np.arange(30, dtype=np.int32), times=times, state_value=2))

    region = slice(5, 10)
    ds = xr.Dataset(
        {
            "value": (("timestamp",), np.arange(5, dtype=np.int32) + 200),
            "firecube_timestamp_state": (
                ("timestamp", "bogus"),
                np.full((5, 2), 7, dtype=np.uint8),
            ),
        },
        coords={"timestamp": times[region]},
    )

    _region_write(store_path, ds, region)

    state = np.asarray(_open_group(store_path)["firecube_timestamp_state"][:])  # type: ignore[index]  # zarr dynamic typing
    np.testing.assert_array_equal(state[:5], _state(5, 2))
    np.testing.assert_array_equal(state[5:10], _state(5, 1))
    np.testing.assert_array_equal(state[10:], _state(20, 2))
