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
        "logger": logging.getLogger("test-v2-f8-start-overlap-plus-new-tail"),
        "state_var_name": "firecube_timestamp_state",
        "storage_config": StorageConfig(storage_type="local", storage_driver="fsspec"),
    }


def _write_store(
    store_path: Path,
    stored_times: np.ndarray,
    *,
    state: np.ndarray | None = None,
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
    state_vals = state if state is not None else np.ones(stored_times.shape, dtype=np.uint8)
    state_arr = group.create_array(
        "firecube_timestamp_state",
        shape=stored_times.shape,
        chunks=(max(int(stored_times.size), 1),),
        dtype=np.uint8,
    )
    state_arr[:] = state_vals


def _dataset(times: np.ndarray) -> xr.Dataset:
    return xr.Dataset(
        {"value": (("timestamp",), np.arange(times.size, dtype=np.float32))},
        coords={"timestamp": times},
    )


def _assert_slice(actual: slice | None, start: int, stop: int) -> None:
    assert actual is not None
    assert actual.start == start
    assert actual.stop == stop
    assert actual.step is None


def test_overlap_at_start_plus_new_tail_classifies_as_split_region_plus_append(
    tmp_path: Path,
) -> None:
    """batch [d0, d1, d10] against store d0..d9 -> split_region_plus_append."""
    store_path = tmp_path / "f8-start-overlap.zarr"
    stored = np.array(
        [f"2024-01-{d + 1:02d}" for d in range(10)],
        dtype="datetime64[s]",
    )
    _write_store(store_path, stored)
    incoming = np.array(
        ["2024-01-01", "2024-01-02", "2024-01-11"],
        dtype="datetime64[s]",
    )

    result = AppendResumeService(**_service_kwargs(store_path)).classify_dataset(
        ds=_dataset(incoming),
        group="default",
    )

    assert result.mode == "split_region_plus_append"
    _assert_slice(result.overwrite_slice, 0, 2)
    assert len(result.new_values) == 1


def test_overlap_at_start_plus_new_tail_with_gap_still_appends(tmp_path: Path) -> None:
    """overlap at prefix + several later new positions with a gap after existing max."""
    store_path = tmp_path / "f8-start-overlap-with-gap.zarr"
    stored = np.array(
        [f"2024-01-{d + 1:02d}" for d in range(5)],
        dtype="datetime64[s]",
    )
    _write_store(store_path, stored)
    incoming = np.array(
        ["2024-01-01", "2024-01-02", "2024-01-10", "2024-01-20"],
        dtype="datetime64[s]",
    )

    result = AppendResumeService(**_service_kwargs(store_path)).classify_dataset(
        ds=_dataset(incoming),
        group="default",
    )

    assert result.mode == "split_region_plus_append"
    _assert_slice(result.overwrite_slice, 0, 2)
    assert len(result.new_values) == 2


def test_non_contiguous_overlap_still_refused(tmp_path: Path) -> None:
    """guard: gaps between overlap indices still trigger non_contiguous refusal."""
    store_path = tmp_path / "f8-non-contiguous.zarr"
    stored = np.array(
        [f"2024-01-{d + 1:02d}" for d in range(6)],
        dtype="datetime64[s]",
    )
    _write_store(store_path, stored)
    incoming = np.array(["2024-01-01", "2024-01-06"], dtype="datetime64[s]")

    with pytest.raises(AppendOverwriteRefused) as exc_info:
        AppendResumeService(**_service_kwargs(store_path)).classify_dataset(
            ds=_dataset(incoming),
            group="default",
        )

    assert exc_info.value.reason == "non_contiguous"


def test_overlap_not_at_batch_prefix_still_refused(tmp_path: Path) -> None:
    """guard: overlap that is not at the batch prefix stays refused.

    The batch is sorted so the ``unsorted_incoming`` gate does not fire
    first; a sorted batch whose overlap is not its prefix carries a new value
    below the existing tail, which is an insert.
    """
    store_path = tmp_path / "f8-suffix-overlap.zarr"
    stored = np.array(
        [f"2024-01-{d + 1:02d}" for d in range(5)],
        dtype="datetime64[s]",
    )
    _write_store(store_path, stored)
    incoming = np.array(
        ["2023-12-31", "2024-01-04", "2024-01-05"],
        dtype="datetime64[s]",
    )

    with pytest.raises(AppendOverwriteRefused) as exc_info:
        AppendResumeService(**_service_kwargs(store_path)).classify_dataset(
            ds=_dataset(incoming),
            group="default",
        )

    assert exc_info.value.reason == "insert"
