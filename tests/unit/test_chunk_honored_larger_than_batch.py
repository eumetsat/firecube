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

"""Configured chunks can be larger than the first batch."""

from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path
from typing import Any, cast

import numpy as np
import pandas as pd
import pytest
import xarray as xr
import zarr

from firecube.core.config import StorageConfig
from firecube.core.filesystem.store_factory import ZarrStoreHandle, create_zarr_store
from firecube.ingestor.runtime.zarr.append import append_time_groups
from firecube.ingestor.runtime.zarr.resume_cache import clear_resume_cache

pytestmark = pytest.mark.unit


@pytest.fixture(autouse=True)
def _clear_resume_cache() -> None:
    clear_resume_cache()


def _local_handle(store_path: Path, mode: str = "a") -> ZarrStoreHandle:
    return create_zarr_store(
        uri=str(store_path),
        storage_config=StorageConfig(storage_type="local", storage_driver="fsspec"),
        mode=mode,
    )


def _timestamps(count: int, start: str = "2024-01-01") -> pd.DatetimeIndex:
    return pd.date_range(start, periods=count, freq="D")


def _dataset_for_batch(_group: str, batch_ts: Sequence[Any]) -> xr.Dataset:
    times = pd.to_datetime(list(batch_ts))
    return xr.Dataset(
        {
            "FWI": (
                ("timestamp", "lat", "lon"),
                np.ones((len(times), 2, 3), dtype=np.float32),
            )
        },
        coords={"timestamp": times, "lat": np.arange(2), "lon": np.arange(3)},
    )


def _open_group(store_path: Path) -> zarr.Group:
    return cast(zarr.Group, zarr.open_group(str(store_path), mode="r")["default"])  # type: ignore[index]


def test_chunk_365_batch_10_writes_chunks_365(tmp_path: Path) -> None:
    """chunk=365, batch=10 persists chunk 365 for data, time, and state."""
    store_path = tmp_path / "chunk365.zarr"

    append_time_groups(
        store=str(store_path),
        zarr_store=_local_handle(store_path),
        group_to_timestamps={"default": list(_timestamps(10))},
        dataset_for_batch=_dataset_for_batch,
        chunk_shape={"timestamp": 365},
        batch_size=10,
    )

    group = _open_group(store_path)
    assert cast(zarr.Array, group["timestamp"]).chunks == (365,)  # type: ignore[index]
    assert cast(zarr.Array, group["firecube_timestamp_state"]).chunks == (365,)  # type: ignore[index]
    assert cast(zarr.Array, group["FWI"]).chunks == (365, 2, 3)  # type: ignore[index]


def test_resume_reads_stored_chunk_not_configured(tmp_path: Path) -> None:
    """chunk_len_used is read from stored chunks, not the current config."""
    store_path = tmp_path / "legacy-clamped.zarr"

    append_time_groups(
        store=str(store_path),
        zarr_store=_local_handle(store_path),
        group_to_timestamps={"default": list(_timestamps(10))},
        dataset_for_batch=_dataset_for_batch,
        chunk_shape={"timestamp": 10},
        batch_size=10,
    )

    clear_resume_cache()
    metrics = append_time_groups(
        store=str(store_path),
        zarr_store=_local_handle(store_path),
        group_to_timestamps={"default": list(_timestamps(1, start="2024-01-11"))},
        dataset_for_batch=_dataset_for_batch,
        chunk_shape={"timestamp": 365},
        resume_existing=True,
        batch_size=1,
    )

    coverage = cast(list[dict[str, Any]], metrics["coverage"])
    assert coverage[0]["chunk_len_used"] == 10
