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

"""Re-offered-and-skipped items must not corrupt partial-chunk seeding.

Dedup (`_apply_state_aware_skip`) runs at the dataset level BEFORE any
workspace write. The partial-chunk seeding path must not depend on the
accident that an all-skip batch happens to be a no-op: this test locks
in that a fully re-offered batch is silently dropped without touching
the workspace, and that a subsequent new-timestamp batch appends cleanly
while the previously seeded chunks remain intact.
"""

from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
import xarray as xr

from firecube.ingestor.runtime.zarr.append import append_time_groups
from firecube.ingestor.runtime.zarr.staged_metadata import seed_staged_store_metadata
from tests.helpers.storage import local_zarr_handle, make_local_session

pytestmark = pytest.mark.integration

_GROUP = "G"
_CHUNK_LEN = 5
_INITIAL_TS_COUNT = 10
_NEW_TS_COUNT = 5


def _make_dataset(batch_timestamps: Sequence[pd.Timestamp]) -> xr.Dataset:
    ts = pd.to_datetime(list(batch_timestamps))
    data = np.arange(len(ts) * 2, dtype=np.float32).reshape(len(ts), 2)
    return xr.Dataset(
        {"val": (("timestamp", "x"), data)},
        coords={"timestamp": ts, "x": np.arange(2)},
    )


def _dataset_for_batch(group: str, batch: Sequence[pd.Timestamp]) -> xr.Dataset:
    return _make_dataset(batch)


def _read_workspace(temp_store: Path) -> xr.Dataset:
    return xr.open_zarr(str(temp_store), group=_GROUP, consolidated=False)


def test_reoffered_batch_skip_does_not_corrupt_seeded_chunks(tmp_path: Path) -> None:
    """A fully re-offered staged batch is skipped without touching the workspace.

    Setup:
      - final target: 10 timestamps, state=1 for all, chunk_len=5
        (two chunks: [0..4] and [5..9])
      - workspace: seeded metadata + timestamp + state coord arrays from final

    Step 1 (all re-offered): batch of the same 10 timestamps.
      - `timestamps_skipped == 10`, `timestamps_written == 0`
      - workspace shape stays 10, no coverage entry, state chunks still =1.

    Step 2 (new batch): timestamps 10..14 (no overlap).
      - `timestamps_skipped == 0`, `timestamps_written == 5`
      - workspace shape becomes 15, state[0:15] all == 1 (seeded chunks
        [0..4] and [5..9] preserved; new chunk [10..14] appended).
    """
    final_store = tmp_path / "final.zarr"
    temp_store = tmp_path / "temp" / "final.zarr"
    temp_store.parent.mkdir(parents=True, exist_ok=True)

    initial_ts = pd.date_range("2024-01-01", periods=_INITIAL_TS_COUNT, freq="h")

    # Step 0: Build the final target with 10 timestamps at state=1, chunk_len=5.
    append_time_groups(
        store=str(final_store),
        zarr_store=local_zarr_handle(final_store),
        session=make_local_session(str(final_store)),
        group_to_timestamps={_GROUP: list(initial_ts)},
        dataset_for_batch=_dataset_for_batch,
        arrays_for_group=lambda g: [f"{g}/val"],
        chunk_shape={"timestamp": _CHUNK_LEN},
        batch_size=_INITIAL_TS_COUNT,
    )

    ds_final = xr.open_zarr(str(final_store), group=_GROUP, consolidated=False)
    assert ds_final.sizes["timestamp"] == _INITIAL_TS_COUNT
    assert np.all(ds_final["firecube_timestamp_state"].values == 1)

    # Seed workspace: zarr.json for every array + coord chunks for timestamp and
    # state. Data-array (`val`) chunks are deliberately NOT copied — data-chunk
    # seeding is the job of `seed_touched_data_chunks`, invoked from
    # `_write_batch` when a partial chunk is touched.
    seed_result = seed_staged_store_metadata(
        temp_store_uri=str(temp_store),
        final_target_uri=str(final_store),
        groups=[_GROUP],
        session=make_local_session(str(temp_store)),
        coordinate_arrays=["timestamp", "firecube_timestamp_state"],
    )
    assert seed_result[_GROUP]["seeded"] is True, f"seeding skipped: {seed_result}"

    # Sanity: workspace has the seeded state chunks (all 1) before any batch runs.
    ws_seeded = _read_workspace(temp_store)
    assert ws_seeded.sizes["timestamp"] == _INITIAL_TS_COUNT
    assert np.all(ws_seeded["firecube_timestamp_state"].values == 1)

    # Step 1: Offer a batch that fully overlaps the final target. All 10 must
    # be silently skipped; the workspace must be untouched (no shape change,
    # no coverage entry, seeded state chunks preserved).
    metrics_skip = append_time_groups(
        store=str(temp_store),
        zarr_store=local_zarr_handle(temp_store),
        session=make_local_session(str(temp_store)),
        resume_zarr_store=local_zarr_handle(final_store, mode="r"),
        group_to_timestamps={_GROUP: list(initial_ts)},
        dataset_for_batch=_dataset_for_batch,
        arrays_for_group=lambda g: [f"{g}/val"],
        chunk_shape={"timestamp": _CHUNK_LEN},
        resume_existing=True,
        batch_size=_INITIAL_TS_COUNT,
        pipeline_write_mode="staged",
        final_target_uri=str(final_store),
    )

    assert metrics_skip["timestamps_skipped"] == _INITIAL_TS_COUNT
    assert metrics_skip["batch_processing"]["timestamps_skipped"] == _INITIAL_TS_COUNT
    assert metrics_skip["batch_processing"]["timestamps_written"] == 0
    assert metrics_skip["batch_processing"]["batches_written"] == 0
    assert "coverage" not in metrics_skip, (
        "A fully-skipped batch must not produce a coverage entry; "
        f"got: {metrics_skip.get('coverage')}"
    )

    ws_after_skip = _read_workspace(temp_store)
    assert ws_after_skip.sizes["timestamp"] == _INITIAL_TS_COUNT, (
        "Workspace shape must remain unchanged when every timestamp is skipped"
    )
    assert np.all(ws_after_skip["firecube_timestamp_state"].values == 1), (
        "Seeded state chunks must survive an all-skip batch"
    )

    # Step 2: Offer a batch of 5 brand-new timestamps. They must all append;
    # the seeded state chunks [0..4] and [5..9] must remain intact and the
    # new chunk [10..14] must also be state=1.
    new_ts = pd.date_range(
        initial_ts[-1] + pd.Timedelta(hours=1),
        periods=_NEW_TS_COUNT,
        freq="h",
    )
    metrics_append = append_time_groups(
        store=str(temp_store),
        zarr_store=local_zarr_handle(temp_store),
        session=make_local_session(str(temp_store)),
        resume_zarr_store=local_zarr_handle(final_store, mode="r"),
        group_to_timestamps={_GROUP: list(new_ts)},
        dataset_for_batch=_dataset_for_batch,
        arrays_for_group=lambda g: [f"{g}/val"],
        chunk_shape={"timestamp": _CHUNK_LEN},
        resume_existing=True,
        batch_size=_NEW_TS_COUNT,
        pipeline_write_mode="staged",
        final_target_uri=str(final_store),
    )

    assert metrics_append["timestamps_skipped"] == 0
    assert metrics_append["batch_processing"]["timestamps_written"] == _NEW_TS_COUNT

    ws_after_append = _read_workspace(temp_store)
    expected_shape = _INITIAL_TS_COUNT + _NEW_TS_COUNT
    assert ws_after_append.sizes["timestamp"] == expected_shape
    state_after = ws_after_append["firecube_timestamp_state"].values
    assert state_after.shape == (expected_shape,)
    assert np.all(state_after == 1), (
        "Seeded state chunks [0..4] and [5..9] must remain state=1 after a new-chunk "
        f"append; got: {state_after.tolist()}"
    )
