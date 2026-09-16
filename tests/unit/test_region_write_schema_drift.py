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

from pathlib import Path

import numpy as np
import pytest
import xarray as xr

from firecube.core.config import StorageConfig
from firecube.core.filesystem.store_factory import create_zarr_store
from firecube.ingestor.errors import SchemaDriftReingestError
from firecube.ingestor.runtime.zarr.schema import validate_existing_time_array_schema
from firecube.ingestor.runtime.zarr.write import write_dataset_to_zarr

pytestmark = pytest.mark.unit


def _local_handle(store_path: Path, mode: str):
    return create_zarr_store(
        uri=str(store_path),
        storage_config=StorageConfig(storage_type="local", storage_driver="fsspec"),
        mode=mode,
    )


def _times(periods: int = 4) -> np.ndarray:
    offsets = np.arange(periods).astype("timedelta64[h]")
    return np.datetime64("2024-01-01T00:00:00", "ns") + offsets


def _dataset(
    times: np.ndarray,
    *,
    value_offset: int = 0,
    include_store_only: bool = False,
    include_extra: bool = False,
) -> xr.Dataset:
    values = value_offset + np.arange(times.size, dtype=np.int32)
    data_vars: dict[str, tuple[tuple[str, ...], np.ndarray]] = {
        "value": (("timestamp",), values),
        "firecube_timestamp_state": (("timestamp",), np.ones(times.size, dtype=np.uint8)),
    }
    if include_store_only:
        data_vars["store_only"] = (("timestamp",), 100 + values)
    if include_extra:
        data_vars["extra"] = (("timestamp",), 200 + values)
    return xr.Dataset(data_vars, coords={"timestamp": times})


def _seed_store(store_path: Path, *, include_store_only: bool = False) -> np.ndarray:
    times = _times()
    write_dataset_to_zarr(
        _dataset(times, include_store_only=include_store_only),
        zarr_store=_local_handle(store_path, mode="w"),
        group="default",
        mode="w",
        time_dim="timestamp",
        chunk_shape={"timestamp": times.size},
    )
    return times


def test_force_reingest_with_extra_var_raises_schema_drift(tmp_path: Path) -> None:
    """region write with extra incoming var → SchemaDriftReingestError."""
    store_path = tmp_path / "extra-var.zarr"
    times = _seed_store(store_path)

    with pytest.raises(SchemaDriftReingestError, match="extra_incoming_variable") as exc_info:
        write_dataset_to_zarr(
            _dataset(times[1:3], value_offset=10, include_extra=True),
            zarr_store=_local_handle(store_path, mode="a"),
            group="default",
            region=slice(1, 3),
            time_dim="timestamp",
        )

    assert exc_info.value.dataset_variable == "extra"
    assert exc_info.value.reason == "extra_incoming_variable"


def test_force_reingest_missing_store_var_raises_schema_drift(tmp_path: Path) -> None:
    """region write missing store var in batch → SchemaDriftReingestError."""
    store_path = tmp_path / "missing-var.zarr"
    times = _seed_store(store_path, include_store_only=True)

    with pytest.raises(SchemaDriftReingestError, match="batch_missing_store_variable") as exc_info:
        write_dataset_to_zarr(
            _dataset(times[1:3], value_offset=10),
            zarr_store=_local_handle(store_path, mode="a"),
            group="default",
            region=slice(1, 3),
            time_dim="timestamp",
        )

    assert exc_info.value.dataset_variable == "store_only"
    assert exc_info.value.reason == "batch_missing_store_variable"


def test_plain_append_with_missing_var_unchanged(tmp_path: Path) -> None:
    """A missing time-aligned variable cannot leave arrays at different lengths."""
    store_path = tmp_path / "plain-append-missing-var.zarr"
    times = _seed_store(store_path, include_store_only=True)
    appended_times = _times(periods=2) + np.timedelta64(times.size, "h")

    handle = _local_handle(store_path, mode="a")
    ds = _dataset(appended_times, value_offset=10)
    with pytest.raises(SchemaDriftReingestError, match="batch_missing_store_variable"):
        validate_existing_time_array_schema(
            ds,
            handle,
            "default",
            time_dim="timestamp",
            state_var_name="firecube_timestamp_state",
        )
    with xr.open_zarr(str(store_path), group="default", consolidated=False) as actual:
        assert actual.sizes["timestamp"] == len(times)
        np.testing.assert_array_equal(actual.timestamp.values, times)
