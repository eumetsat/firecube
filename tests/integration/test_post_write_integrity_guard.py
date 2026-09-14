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

"""Post-write integrity guard for staged append writes.

Verifies that ``firecube_timestamp_state == 1`` slots inside the batch's
touched chunks are preserved between seed and promotion. Tests use direct
workspace mutation (no engine hooks) so the guard function is exercised in
isolation.
"""

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
_STATE_ARRAY = "firecube_timestamp_state"


def _open_state_array(store_path: Path, mode: Literal["r", "r+", "a", "w", "w-"]) -> zarr.Array:
    root = zarr.open_group(str(store_path), mode=mode, zarr_format=3)
    group = cast(Any, root[_GROUP])
    return cast(zarr.Array, group[_STATE_ARRAY])


def _build_state_array(store_path: Path, *, state_values: np.ndarray, chunk_len: int) -> None:
    store_path.mkdir(parents=True, exist_ok=True)
    root = zarr.open_group(store=str(store_path), mode="a", zarr_format=3)
    group = root.require_group(_GROUP)
    arr = group.create_array(
        _STATE_ARRAY,
        shape=state_values.shape,
        dtype="uint8",
        chunks=(chunk_len,),
        fill_value=0,
        dimension_names=("timestamp",),
    )
    arr[...] = state_values


def _seed_chunk_to_workspace(
    *,
    temp_store_path: Path,
    final_store_path: Path,
    chunk_idx: tuple[int, ...],
) -> None:
    final_arr = _open_state_array(final_store_path, mode="r")

    temp_store_path.parent.mkdir(parents=True, exist_ok=True)
    ws_root = zarr.open_group(str(temp_store_path), mode="a", zarr_format=3)
    ws_group = ws_root.require_group(_GROUP)
    if _STATE_ARRAY in ws_group:
        ws_arr = cast(zarr.Array, ws_group[_STATE_ARRAY])
    else:
        ws_arr = ws_group.create_array(
            _STATE_ARRAY,
            shape=final_arr.shape,
            dtype=final_arr.dtype,
            chunks=final_arr.chunks,
            fill_value=0,
            dimension_names=("timestamp",),
        )

    chunk_len = int(final_arr.chunks[0])
    array_len = int(final_arr.shape[0])
    region = slice(
        chunk_idx[0] * chunk_len,
        min((chunk_idx[0] + 1) * chunk_len, array_len),
    )
    ws_arr[region] = np.asarray(final_arr[region])


def test_workspace_corruption_inside_seeded_chunk_blocks_promotion(
    tmp_path: Path,
) -> None:
    final_store = tmp_path / "final.zarr"
    temp_store = tmp_path / "temp" / "final.zarr"

    target_state = np.array([1, 1, 1, 1, 0, 0, 0, 0], dtype=np.uint8)
    _build_state_array(final_store, state_values=target_state, chunk_len=4)

    _seed_chunk_to_workspace(
        temp_store_path=temp_store, final_store_path=final_store, chunk_idx=(0,)
    )

    ws_state = _open_state_array(temp_store, mode="a")
    ws_state[2] = 3

    touched_chunks = {_GROUP: {_STATE_ARRAY: [(0,)]}}
    session = make_local_session(str(temp_store))

    with pytest.raises(IntegrityGuardError) as excinfo:
        verify_post_write_integrity(
            temp_store_uri=str(temp_store),
            final_target_uri=str(final_store),
            touched_chunks=touched_chunks,
            session=session,
            append_dim="timestamp",
        )

    message = str(excinfo.value)
    assert _GROUP in message
    assert _STATE_ARRAY in message
    assert not temp_store.exists(), "Workspace must be deleted on integrity failure"

    target_final_state = np.asarray(_open_state_array(final_store, mode="r")[:])
    assert list(target_final_state) == [1, 1, 1, 1, 0, 0, 0, 0], "Target must be untouched"


def test_untouched_slots_do_not_trigger_guard(tmp_path: Path) -> None:
    final_store = tmp_path / "final.zarr"
    temp_store = tmp_path / "temp" / "final.zarr"

    target_state = np.array([1, 1, 1, 1, 1, 1, 1, 1], dtype=np.uint8)
    _build_state_array(final_store, state_values=target_state, chunk_len=4)

    _seed_chunk_to_workspace(
        temp_store_path=temp_store, final_store_path=final_store, chunk_idx=(0,)
    )

    ws_state = _open_state_array(temp_store, mode="a")
    ws_state[5] = 3

    touched_chunks = {_GROUP: {_STATE_ARRAY: [(0,)]}}
    session = make_local_session(str(temp_store))

    verify_post_write_integrity(
        temp_store_uri=str(temp_store),
        final_target_uri=str(final_store),
        touched_chunks=touched_chunks,
        session=session,
        append_dim="timestamp",
    )

    assert temp_store.exists(), "Workspace must not be deleted when only untouched slots differ"
    ws_state_check = np.asarray(_open_state_array(temp_store, mode="r")[:])
    assert int(ws_state_check[5]) == 3


def test_fresh_write_guard_noop(tmp_path: Path) -> None:
    final_store = tmp_path / "final.zarr"
    temp_store = tmp_path / "temp" / "final.zarr"

    target_state = np.zeros(8, dtype=np.uint8)
    _build_state_array(final_store, state_values=target_state, chunk_len=4)

    _seed_chunk_to_workspace(
        temp_store_path=temp_store, final_store_path=final_store, chunk_idx=(0,)
    )

    session = make_local_session(str(temp_store))
    touched_chunks = {_GROUP: {_STATE_ARRAY: [(0,)]}}

    verify_post_write_integrity(
        temp_store_uri=str(temp_store),
        final_target_uri=str(final_store),
        touched_chunks=touched_chunks,
        session=session,
        append_dim="timestamp",
    )
    assert temp_store.exists()

    missing_target = tmp_path / "missing.zarr"
    missing_temp = tmp_path / "missing_temp" / "final.zarr"
    verify_post_write_integrity(
        temp_store_uri=str(missing_temp),
        final_target_uri=str(missing_target),
        touched_chunks={_GROUP: {_STATE_ARRAY: [(0,)]}},
        session=make_local_session(str(missing_temp)),
        append_dim="timestamp",
    )
