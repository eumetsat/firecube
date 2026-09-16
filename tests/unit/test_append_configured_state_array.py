#!/usr/bin/env python
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
from typing import Any, cast

import numpy as np
import pandas as pd
import pytest
import xarray as xr
import zarr

from firecube.core.config import StorageConfig
from firecube.core.filesystem.store_factory import create_zarr_store
from firecube.ingestor.errors import AppendOverwriteRefused
from firecube.ingestor.runtime.zarr.alignment import AlignmentMonitor
from firecube.ingestor.runtime.zarr.append_services import (
    AppendResumeService,
    AppendWriteExecutor,
)
from firecube.ingestor.runtime.zarr.write import write_dataset_to_zarr

pytestmark = pytest.mark.unit


def _local_handle(store_path: Path, mode: str):
    return create_zarr_store(
        uri=str(store_path),
        storage_config=StorageConfig(storage_type="local", storage_driver="fsspec"),
        mode=mode,
    )


def _dataset(values: np.ndarray, *, state_value: int = 1) -> xr.Dataset:
    times = np.datetime64("2024-01-01", "ns") + np.arange(len(values)).astype("timedelta64[h]")
    return xr.Dataset(
        {
            "value": (("time",), values),
            "my_state": (("time",), np.full(len(values), state_value, dtype=np.uint8)),
        },
        coords={"time": times},
    )


def test_region_write_uses_configured_state_var_name(tmp_path: Path) -> None:
    """region write uses self.state_var_name, not hardcoded 'firecube_timestamp_state'."""
    store_path = tmp_path / "custom-state.zarr"
    initial = _dataset(np.arange(6, dtype=np.int32), state_value=2)
    write_dataset_to_zarr(
        initial,
        zarr_store=_local_handle(store_path, mode="w"),
        group="G",
        mode="w",
        time_dim="time",
        state_var_name="my_state",
        chunk_shape={"time": 6},
    )

    writer = AppendWriteExecutor(
        zarr_store=_local_handle(store_path, mode="a"),
        chunk_shape=None,
        shard_shape=None,
        sharding=False,
        compression=False,
        append_dim="timestamp",
        time_dim_name="time",
        state_var_name="my_state",
        logger=logging.getLogger(__name__),
        write_fn=write_dataset_to_zarr,
        alignment=AlignmentMonitor(),
    )

    region = slice(2, 5)
    replacement = _dataset(np.asarray([20, 21, 22], dtype=np.int32), state_value=7)
    replacement = replacement.assign_coords(time=initial["time"].values[region])
    writer.execute(ds=replacement, group="G", mode="a", region=region)

    group = cast(Any, zarr.open_group(store=str(store_path), mode="r", zarr_format=3)["G"])
    assert "firecube_timestamp_state" not in group
    np.testing.assert_array_equal(
        np.asarray(group["value"][:]),
        [0, 1, 20, 21, 22, 5],
    )
    np.testing.assert_array_equal(
        np.asarray(group["my_state"][:]),
        [2, 2, 1, 1, 1, 2],
    )


def _resume_service(store_path: Path, *, state_var_name: str) -> AppendResumeService:
    return AppendResumeService(
        read_source_uri=store_path.as_uri(),
        read_storage_options=None,
        resume_existing=True,
        append_dim="time",
        chunk_shape=None,
        shard_shape=None,
        sharding=False,
        logger=logging.getLogger(__name__),
        state_var_name=state_var_name,
        storage_config=StorageConfig(storage_type="local", storage_driver="fsspec"),
    )


def _write_custom_state_store(store_path: Path) -> xr.Dataset:
    initial = _dataset(np.arange(6, dtype=np.int32)).assign(
        my_state=(("time",), np.array([1, 1, 1, 2, 2, 2], dtype=np.uint8))
    )
    write_dataset_to_zarr(
        initial,
        zarr_store=_local_handle(store_path, mode="w"),
        group="G",
        mode="w",
        time_dim="time",
        state_var_name="my_state",
        chunk_shape={"time": 6},
    )
    return initial


def test_resume_service_skips_and_refills_by_configured_state_var_name(tmp_path: Path) -> None:
    """the service reads the configured state array, so state=1 slots are
    skipped and state=2 slots stay refillable under a custom name."""
    store_path = tmp_path / "custom-state-resume.zarr"
    initial = _write_custom_state_store(store_path)
    overlap = initial["time"].values[2:5]  # slot 2 is present, slots 3-4 are deleted
    incoming = _dataset(np.asarray([20, 21, 22], dtype=np.int32)).assign_coords(time=overlap)

    svc = _resume_service(store_path, state_var_name="my_state")

    skip_set = svc.compute_state_aware_skip_set(ds=incoming, group="G")
    assert skip_set == {pd.Timestamp(overlap[0])}

    refill = svc.classify_dataset(
        ds=incoming.isel(time=slice(1, None)),
        group="G",
        allow_refill_plus_append=True,
    )
    assert refill.mode == "region_overwrite"
    assert refill.overwrite_slice == slice(3, 5)


def test_missing_state_array_is_typed_error_naming_the_path(tmp_path: Path) -> None:
    """no silent all-present fallback; a missing state array names its path
    and the resume_existing remedy."""
    store_path = tmp_path / "custom-state-missing.zarr"
    initial = _write_custom_state_store(store_path)
    incoming = _dataset(np.asarray([7], dtype=np.int32)).assign_coords(
        time=initial["time"].values[5:6]
    )

    svc = _resume_service(store_path, state_var_name="firecube_timestamp_state")

    with pytest.raises(AppendOverwriteRefused) as exc_info:
        svc.classify_dataset(ds=incoming, group="G")

    assert exc_info.value.reason == "state_array_missing"
    assert exc_info.value.array_path == "G/firecube_timestamp_state"
    message = str(exc_info.value)
    assert "G/firecube_timestamp_state" in message
    assert "resume_existing=true" in message
