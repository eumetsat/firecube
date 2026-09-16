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

"""Indexed append-coordinate reads preserve timestamp order and precision."""

from __future__ import annotations

import logging
from pathlib import Path

import numpy as np
import pytest
import zarr
from zarr.storage import LocalStore

from firecube.ingestor.runtime.zarr.append_services import (
    AppendResumeService,
    IndexedAppendCoordinate,
)

pytestmark = pytest.mark.unit


def _make_service() -> AppendResumeService:
    return AppendResumeService(
        read_source_uri=None,
        read_storage_options=None,
        resume_existing=False,
        append_dim="timestamp",
        chunk_shape=None,
        shard_shape=None,
        sharding=False,
        logger=logging.getLogger("test-indexed-coord"),
        state_var_name="firecube_timestamp_state",
    )


def _open_group_with_coord(
    tmp_path: Path,
    *,
    times: np.ndarray,
    state: np.ndarray | None = None,
    append_dim: str = "timestamp",
    state_array_name: str = "firecube_timestamp_state",
) -> zarr.Group:
    store_path = tmp_path / "store.zarr"
    root = zarr.open_group(store=LocalStore(store_path), mode="w", zarr_format=3)
    n = int(times.shape[0])
    time_arr = root.create_array(
        append_dim,
        shape=(n,),
        dtype=times.dtype,
        chunks=(max(n, 1),),
    )
    time_arr[:] = times
    state_vals = state if state is not None else np.ones((n,), dtype=np.uint8)
    state_arr = root.create_array(
        state_array_name,
        shape=(n,),
        dtype=np.uint8,
        chunks=(max(n, 1),),
    )
    state_arr[:] = state_vals
    return zarr.open_group(store=LocalStore(store_path), mode="r", zarr_format=3)


def test_unique_sorted_populates_index_map(tmp_path: Path) -> None:
    times = np.array(
        ["2024-01-01T00:00:00", "2024-01-01T01:00:00", "2024-01-01T02:00:00"],
        dtype="datetime64[s]",
    )
    group = _open_group_with_coord(tmp_path, times=times)
    svc = _make_service()

    result = svc.read_indexed_append_coordinate(group, "timestamp")

    assert isinstance(result, IndexedAppendCoordinate)
    assert result.is_sorted is True
    assert result.duplicate_diagnostics == []
    assert len(result.value_to_index) == 3
    for i, v in enumerate(times.tolist()):
        assert result.value_to_index[v] == i


def test_duplicates_empty_map_with_diagnostics(tmp_path: Path) -> None:
    times = np.array(
        [
            "2024-01-01T00:00:00",
            "2024-01-01T00:00:00",
            "2024-01-01T02:00:00",
        ],
        dtype="datetime64[s]",
    )
    group = _open_group_with_coord(tmp_path, times=times)
    svc = _make_service()

    result = svc.read_indexed_append_coordinate(group, "timestamp")

    assert result.value_to_index == {}
    assert result.duplicate_diagnostics, "expected diagnostics for duplicate slot"
    joined = " | ".join(result.duplicate_diagnostics)
    assert "2024-01-01" in joined
    assert "appears 2x" in joined
    assert "index 0" in joined


def test_nat_flagged(tmp_path: Path) -> None:
    times = np.array(
        ["2024-01-01T00:00:00", "NaT", "2024-01-01T02:00:00"],
        dtype="datetime64[s]",
    )
    group = _open_group_with_coord(tmp_path, times=times)
    svc = _make_service()

    result = svc.read_indexed_append_coordinate(group, "timestamp")

    assert result.value_to_index == {}
    joined = " | ".join(result.duplicate_diagnostics)
    assert "NaT" in joined
    assert "index 1" in joined


def test_unsorted_flagged(tmp_path: Path) -> None:
    times = np.array(
        ["2024-01-01T02:00:00", "2024-01-01T01:00:00", "2024-01-01T00:00:00"],
        dtype="datetime64[s]",
    )
    group = _open_group_with_coord(tmp_path, times=times)
    svc = _make_service()

    result = svc.read_indexed_append_coordinate(group, "timestamp")

    assert result.is_sorted is False
    assert result.duplicate_diagnostics == []
    assert len(result.value_to_index) == 3


def test_state_array_returned(tmp_path: Path) -> None:
    times = np.array(
        ["2024-01-01T00:00:00", "2024-01-01T01:00:00", "2024-01-01T02:00:00"],
        dtype="datetime64[s]",
    )
    state = np.array([1, 1, 1], dtype=np.uint8)
    group = _open_group_with_coord(tmp_path, times=times, state=state)
    svc = _make_service()

    result = svc.read_indexed_append_coordinate(group, "timestamp")

    assert result.state.shape == (3,)
    assert result.state.dtype == np.uint8
    assert np.array_equal(result.state, state)


def test_mixed_state_values_preserved(tmp_path: Path) -> None:
    times = np.array(
        [
            "2024-01-01T00:00:00",
            "2024-01-01T01:00:00",
            "2024-01-01T02:00:00",
            "2024-01-01T03:00:00",
        ],
        dtype="datetime64[s]",
    )
    state = np.array([0, 1, 2, 3], dtype=np.uint8)
    group = _open_group_with_coord(tmp_path, times=times, state=state)
    svc = _make_service()

    result = svc.read_indexed_append_coordinate(group, "timestamp")

    assert result.state.dtype == np.uint8
    assert list(result.state.tolist()) == [0, 1, 2, 3]
