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

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import numpy as np
import pytest
import xarray as xr
import zarr

from firecube.core.config import StorageConfig
from firecube.ingestor.errors import AppendOverwriteRefused
from firecube.ingestor.runtime.zarr.append_services import AppendResumeService

pytestmark = pytest.mark.unit


def _service_kwargs(store_path: Path) -> dict[str, Any]:
    return {
        "read_source_uri": store_path.as_uri(),
        "read_storage_options": None,
        "resume_existing": True,
        "append_dim": "timestamp",
        "chunk_shape": None,
        "shard_shape": None,
        "sharding": False,
        "logger": logging.getLogger("test-v2-f1-overlap-keys-unit-safe"),
        "state_var_name": "firecube_timestamp_state",
        "storage_config": StorageConfig(storage_type="local", storage_driver="fsspec"),
    }


def _write_store(
    store_path: Path,
    stored_times: np.ndarray,
    *,
    attrs: dict[str, Any] | None = None,
) -> None:
    root = zarr.open_group(str(store_path), mode="w", zarr_format=3)
    group = root.require_group("default")
    timestamp = group.create_array(
        "timestamp",
        shape=stored_times.shape,
        chunks=(max(int(stored_times.size), 1),),
        dtype=stored_times.dtype,
    )
    timestamp[:] = stored_times
    if attrs:
        timestamp.attrs.update(attrs)

    state = group.create_array(
        "firecube_timestamp_state",
        shape=stored_times.shape,
        chunks=(max(int(stored_times.size), 1),),
        dtype=np.uint8,
    )
    state[:] = np.ones(stored_times.shape, dtype=np.uint8)


def _dataset(times: np.ndarray, *, attrs: dict[str, Any] | None = None) -> xr.Dataset:
    ds = xr.Dataset(
        {"value": (("timestamp",), np.arange(times.size, dtype=np.float32))},
        coords={"timestamp": times},
    )
    if attrs:
        ds["timestamp"].attrs.update(attrs)
    return ds


def _assert_slice(actual: slice | None, start: int, stop: int) -> None:
    assert actual is not None
    assert actual.start == start
    assert actual.stop == stop
    assert actual.step is None


def test_stored_ns_incoming_s_tail_overlap_classifies_split(tmp_path: Path) -> None:
    store_path = tmp_path / "stored-ns-incoming-s.zarr"
    stored = np.arange(
        np.datetime64("2024-01-01", "ns"),
        np.datetime64("2024-01-06", "ns"),
        np.timedelta64(1, "D"),
    )
    incoming = np.array(
        ["2024-01-04T00:00:00", "2024-01-05T00:00:00", "2024-01-06T00:00:00"],
        dtype="datetime64[s]",
    )
    _write_store(store_path, stored)

    result = AppendResumeService(**_service_kwargs(store_path)).classify_dataset(
        ds=_dataset(incoming),
        group="default",
    )

    assert result.mode == "split_region_plus_append"
    _assert_slice(result.overwrite_slice, 3, 5)
    assert len(result.new_values) == 1


def test_stored_ns_incoming_d_middle_overlap_classifies_region(tmp_path: Path) -> None:
    store_path = tmp_path / "stored-ns-incoming-d.zarr"
    stored = np.arange(
        np.datetime64("2024-01-01", "ns"),
        np.datetime64("2024-01-06", "ns"),
        np.timedelta64(1, "D"),
    )
    incoming = np.array(["2024-01-02", "2024-01-03", "2024-01-04"], dtype="datetime64[D]")
    _write_store(store_path, stored)

    result = AppendResumeService(**_service_kwargs(store_path)).classify_dataset(
        ds=_dataset(incoming),
        group="default",
    )

    assert result.mode == "region_overwrite"
    _assert_slice(result.overwrite_slice, 1, 4)
    assert result.new_values == []


def test_stored_ns_incoming_ms_full_overlap_classifies_region(tmp_path: Path) -> None:
    store_path = tmp_path / "stored-ns-incoming-ms.zarr"
    stored = np.arange(
        np.datetime64("2024-01-01", "ns"),
        np.datetime64("2024-01-04", "ns"),
        np.timedelta64(1, "D"),
    )
    incoming = np.array(
        ["2024-01-01T00:00:00.000", "2024-01-02T00:00:00.000", "2024-01-03T00:00:00.000"],
        dtype="datetime64[ms]",
    )
    _write_store(store_path, stored)

    result = AppendResumeService(**_service_kwargs(store_path)).classify_dataset(
        ds=_dataset(incoming),
        group="default",
    )

    assert result.mode == "region_overwrite"
    _assert_slice(result.overwrite_slice, 0, 3)
    assert result.new_values == []


def test_stored_cf_numeric_seconds_incoming_ns_finds_overlap(tmp_path: Path) -> None:
    store_path = tmp_path / "stored-cf-seconds-incoming-ns.zarr"
    attrs = {"units": "seconds since 1970-01-01", "calendar": "standard"}
    one_day_s = 24 * 60 * 60
    stored = np.array(
        [1704067200, 1704067200 + one_day_s, 1704067200 + 2 * one_day_s], dtype=np.int32
    )
    incoming = np.array(
        ["2024-01-02T00:00:00", "2024-01-03T00:00:00", "2024-01-04T00:00:00"],
        dtype="datetime64[ns]",
    )
    _write_store(store_path, stored, attrs=attrs)

    result = AppendResumeService(**_service_kwargs(store_path)).classify_dataset(
        ds=_dataset(incoming),
        group="default",
    )

    assert result.mode == "split_region_plus_append"
    _assert_slice(result.overwrite_slice, 1, 3)
    assert len(result.new_values) == 1


def test_stored_ns_incoming_cf_numeric_days_finds_overlap(tmp_path: Path) -> None:
    store_path = tmp_path / "stored-ns-incoming-cf-days.zarr"
    stored = np.arange(
        np.datetime64("2024-01-01", "ns"),
        np.datetime64("2024-01-06", "ns"),
        np.timedelta64(1, "D"),
    )
    incoming = np.array([2, 3, 4, 5, 6], dtype=np.int32)
    _write_store(store_path, stored)

    result = AppendResumeService(**_service_kwargs(store_path)).classify_dataset(
        ds=_dataset(incoming, attrs={"units": "days since 2024-01-01", "calendar": "standard"}),
        group="default",
    )

    assert result.mode == "split_region_plus_append"
    _assert_slice(result.overwrite_slice, 2, 5)
    assert len(result.new_values) == 2


def test_stored_ns_incoming_numeric_without_attrs_refuses_typed(tmp_path: Path) -> None:
    store_path = tmp_path / "stored-ns-incoming-numeric-no-attrs.zarr"
    stored = np.arange(
        np.datetime64("2024-01-01", "ns"),
        np.datetime64("2024-01-06", "ns"),
        np.timedelta64(1, "D"),
    )
    incoming = np.array([2, 3, 4], dtype=np.int32)
    _write_store(store_path, stored)

    with pytest.raises(AppendOverwriteRefused) as exc_info:
        AppendResumeService(**_service_kwargs(store_path)).classify_dataset(
            ds=_dataset(incoming),
            group="default",
        )

    assert exc_info.value.reason == "time_coord_mismatch"
