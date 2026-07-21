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

"""Real-store behavior tests for AppendStrategy.

These tests drive the strategy against a real local Zarr store under
``tmp_path``.  Unlike the unit suite, they assert on-disk array shape,
written values, and resume-cursor positioning — never on mock call counts.
The mutation check: commenting out the second ``write_groups`` call MUST
break ``test_append_strategy_two_batches_grow_real_store``.
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pytest
import xarray as xr
import zarr

from firecube.core.config import StorageConfig
from firecube.ingestor.runtime.zarr.strategies.append import AppendStrategy
from tests.helpers.storage import make_local_session

pytestmark = pytest.mark.integration


_GROUP = "G"
_X_COUNT = 4


def _build_dataset(timestamps: list[int]) -> xr.Dataset:
    ts_arr = np.array(timestamps, dtype=np.int64)
    values = np.outer(ts_arr, np.arange(1, _X_COUNT + 1, dtype=np.float32)).astype(np.float32)
    return xr.Dataset(
        {"val": (["timestamp", "x"], values)},
        coords={"timestamp": ts_arr, "x": np.arange(_X_COUNT)},
    )


def _make_strategy(target_uri: str) -> AppendStrategy:
    return AppendStrategy(
        store=object(),
        store_uri=target_uri,
        storage_config=StorageConfig(storage_type="local", storage_driver="fsspec"),
        session=make_local_session(target_uri),
        append_dim="timestamp",
        chunk_shape={"timestamp": 1, "x": _X_COUNT},
    )


def _open_val_array(target: Path) -> zarr.Array:
    return zarr.open_array(str(target / _GROUP / "val"), mode="r")


def test_append_strategy_two_batches_grow_real_store(tmp_path: Path) -> None:
    """Two sequential write_groups calls extend the time dim without overwrite.

    Asserts four invariants on a real store:
      1. Batch 1 creates the array with the right shape and values.
      2. Batch 2 (via a fresh strategy) appends without overwriting batch 1.
      3. Final shape == sum of batch sizes along ``timestamp``.
      4. Resume cursor saw the appended length: otherwise batch 2 would have
         landed at index 0 and overwritten batch 1.
    """
    target = tmp_path / "product.zarr"
    target_uri = str(target)

    batch1_ts = [10, 11, 12]
    ds_b1 = _build_dataset(batch1_ts)

    result_b1 = _make_strategy(target_uri).write_groups(
        group_to_timestamps={_GROUP: batch1_ts},
        dataset_for_batch=lambda _group, _ts: ds_b1,
        batch_size=len(batch1_ts),
    )

    assert result_b1["batch_processing"]["timestamps_written"] == len(batch1_ts)

    arr_after_b1 = _open_val_array(target)
    assert arr_after_b1.shape == (len(batch1_ts), _X_COUNT)
    np.testing.assert_array_equal(arr_after_b1[:], ds_b1["val"].values)

    batch2_ts = [20, 21, 22]
    ds_b2 = _build_dataset(batch2_ts)

    result_b2 = _make_strategy(target_uri).write_groups(
        group_to_timestamps={_GROUP: batch2_ts},
        dataset_for_batch=lambda _group, _ts: ds_b2,
        batch_size=len(batch2_ts),
    )

    assert result_b2["batch_processing"]["timestamps_written"] == len(batch2_ts)

    arr_after_b2 = _open_val_array(target)
    expected_rows = len(batch1_ts) + len(batch2_ts)
    assert arr_after_b2.shape == (expected_rows, _X_COUNT), (
        f"resume cursor must have positioned at {len(batch1_ts)}; "
        f"final shape {arr_after_b2.shape} indicates an overwrite"
    )

    full_values = np.concatenate([ds_b1["val"].values, ds_b2["val"].values], axis=0)
    np.testing.assert_array_equal(arr_after_b2[:], full_values)

    ds_final = xr.open_zarr(str(target), group=_GROUP, consolidated=False)
    try:
        np.testing.assert_array_equal(
            ds_final["timestamp"].values,
            np.array(batch1_ts + batch2_ts, dtype=np.int64),
        )
    finally:
        ds_final.close()


def test_append_strategy_resume_existing_skips_duplicate_timestamps(tmp_path: Path) -> None:
    """With ``resume_existing=True``, the cursor must advance from the existing tail.

    Writes batch 1, then a smaller batch 2 supplying only the genuinely new
    timestamp.  The on-disk length must equal len(batch1) + len(new) — proving
    the cursor advanced from the existing tail rather than from zero.
    """
    target = tmp_path / "product.zarr"
    target_uri = str(target)

    batch1_ts = [0, 1, 2]
    ds_b1 = _build_dataset(batch1_ts)
    _make_strategy(target_uri).write_groups(
        group_to_timestamps={_GROUP: batch1_ts},
        dataset_for_batch=lambda _group, _ts: ds_b1,
        batch_size=len(batch1_ts),
    )

    assert _open_val_array(target).shape[0] == len(batch1_ts)

    new_only_ts = [3]
    ds_new_only = _build_dataset(new_only_ts)

    strategy_b = AppendStrategy(
        store=object(),
        store_uri=target_uri,
        storage_config=StorageConfig(storage_type="local", storage_driver="fsspec"),
        session=make_local_session(target_uri),
        append_dim="timestamp",
        chunk_shape={"timestamp": 1, "x": _X_COUNT},
        resume_existing=True,
    )
    strategy_b.write_groups(
        group_to_timestamps={_GROUP: new_only_ts},
        dataset_for_batch=lambda _group, _ts: ds_new_only,
        batch_size=len(new_only_ts),
    )

    arr = _open_val_array(target)
    assert arr.shape == (len(batch1_ts) + len(new_only_ts), _X_COUNT)
    np.testing.assert_array_equal(arr[: len(batch1_ts)], ds_b1["val"].values)
    np.testing.assert_array_equal(arr[len(batch1_ts) :], ds_new_only["val"].values)
