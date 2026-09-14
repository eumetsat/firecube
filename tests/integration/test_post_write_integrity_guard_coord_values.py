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

"""Coordinate-value checks in the staged post-write integrity guard."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Literal, cast

import numpy as np
import pytest
import zarr

from firecube.ingestor.errors import IntegrityGuardError
from firecube.ingestor.runtime.zarr.append_services import verify_post_write_integrity
from tests.helpers.storage import make_local_session

pytestmark = pytest.mark.integration

_GROUP = "grp"
_APPEND_DIM = "timestamp"
_STATE_ARRAY = "firecube_timestamp_state"


def _open_array(
    store_path: Path,
    name: str,
    mode: Literal["r", "r+", "a", "w", "w-"],
) -> zarr.Array:
    root = zarr.open_group(str(store_path), mode=mode, zarr_format=3)
    group = cast(Any, root[_GROUP])
    return cast(zarr.Array, group[name])


def _build_target_store(
    store_path: Path,
    *,
    state_values: np.ndarray,
    coord_values: np.ndarray,
    chunk_len: int,
) -> None:
    store_path.mkdir(parents=True, exist_ok=True)
    root = zarr.open_group(store=str(store_path), mode="a", zarr_format=3)
    group = root.require_group(_GROUP)
    state_arr = group.create_array(
        _STATE_ARRAY,
        shape=state_values.shape,
        dtype="uint8",
        chunks=(chunk_len,),
        fill_value=0,
        dimension_names=(_APPEND_DIM,),
    )
    coord_arr = group.create_array(
        _APPEND_DIM,
        shape=coord_values.shape,
        dtype=coord_values.dtype,
        chunks=(chunk_len,),
        fill_value=0,
        dimension_names=(_APPEND_DIM,),
    )
    state_arr[...] = state_values
    coord_arr[...] = coord_values


def _seed_chunk_to_workspace(
    *,
    temp_store_path: Path,
    final_store_path: Path,
    chunk_idx: tuple[int, ...],
) -> None:
    final_state = _open_array(final_store_path, _STATE_ARRAY, mode="r")
    final_coord = _open_array(final_store_path, _APPEND_DIM, mode="r")

    temp_store_path.parent.mkdir(parents=True, exist_ok=True)
    ws_root = zarr.open_group(str(temp_store_path), mode="a", zarr_format=3)
    ws_group = ws_root.require_group(_GROUP)
    ws_state = ws_group.create_array(
        _STATE_ARRAY,
        shape=final_state.shape,
        dtype=final_state.dtype,
        chunks=final_state.chunks,
        fill_value=0,
        dimension_names=(_APPEND_DIM,),
    )
    ws_coord = ws_group.create_array(
        _APPEND_DIM,
        shape=final_coord.shape,
        dtype=final_coord.dtype,
        chunks=final_coord.chunks,
        fill_value=0,
        dimension_names=(_APPEND_DIM,),
    )

    chunk_len = int(final_state.chunks[0])
    array_len = int(final_state.shape[0])
    region = slice(
        chunk_idx[0] * chunk_len,
        min((chunk_idx[0] + 1) * chunk_len, array_len),
    )
    ws_state[region] = np.asarray(final_state[region])
    ws_coord[region] = np.asarray(final_coord[region])


def _touched_chunks() -> dict[str, dict[str, list[tuple[int, ...]]]]:
    return {_GROUP: {_STATE_ARRAY: [(0,)], _APPEND_DIM: [(0,)]}}


def test_drifted_coord_on_touched_slot_raises(tmp_path: Path) -> None:
    final_store = tmp_path / "final.zarr"
    temp_store = tmp_path / "temp" / "final.zarr"

    _build_target_store(
        final_store,
        state_values=np.array([1, 1, 0, 0], dtype=np.uint8),
        coord_values=np.array([10, 20, 30, 40], dtype=np.int64),
        chunk_len=4,
    )
    _seed_chunk_to_workspace(
        temp_store_path=temp_store, final_store_path=final_store, chunk_idx=(0,)
    )
    ws_coord = _open_array(temp_store, _APPEND_DIM, mode="a")
    ws_coord[0] = 999

    with pytest.raises(IntegrityGuardError) as excinfo:
        verify_post_write_integrity(
            temp_store_uri=str(temp_store),
            final_target_uri=str(final_store),
            touched_chunks=_touched_chunks(),
            session=make_local_session(str(temp_store)),
            append_dim=_APPEND_DIM,
        )

    message = str(excinfo.value)
    assert _APPEND_DIM in message
    assert "0" in message
    assert not temp_store.exists(), "Workspace must be deleted on coordinate drift"


def test_matching_coord_values_on_touched_slots_pass(tmp_path: Path) -> None:
    final_store = tmp_path / "final.zarr"
    temp_store = tmp_path / "temp" / "final.zarr"

    _build_target_store(
        final_store,
        state_values=np.array([1, 1, 0, 0], dtype=np.uint8),
        coord_values=np.array([10, 20, 30, 40], dtype=np.int64),
        chunk_len=4,
    )
    _seed_chunk_to_workspace(
        temp_store_path=temp_store, final_store_path=final_store, chunk_idx=(0,)
    )

    verify_post_write_integrity(
        temp_store_uri=str(temp_store),
        final_target_uri=str(final_store),
        touched_chunks=_touched_chunks(),
        session=make_local_session(str(temp_store)),
        append_dim=_APPEND_DIM,
    )

    assert temp_store.exists()


def test_state_zero_slots_with_drifted_coord_do_not_raise(tmp_path: Path) -> None:
    final_store = tmp_path / "final.zarr"
    temp_store = tmp_path / "temp" / "final.zarr"

    _build_target_store(
        final_store,
        state_values=np.array([0, 0, 0, 0], dtype=np.uint8),
        coord_values=np.array([10, 20, 30, 40], dtype=np.int64),
        chunk_len=4,
    )
    _seed_chunk_to_workspace(
        temp_store_path=temp_store, final_store_path=final_store, chunk_idx=(0,)
    )
    ws_coord = _open_array(temp_store, _APPEND_DIM, mode="a")
    ws_coord[0] = 999
    ws_coord[1] = 888

    verify_post_write_integrity(
        temp_store_uri=str(temp_store),
        final_target_uri=str(final_store),
        touched_chunks=_touched_chunks(),
        session=make_local_session(str(temp_store)),
        append_dim=_APPEND_DIM,
    )

    assert temp_store.exists()
