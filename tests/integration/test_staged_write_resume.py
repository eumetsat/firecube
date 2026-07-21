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

from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import xarray as xr

from firecube.core.storage.uri import StorageUri  # pyright: ignore[reportMissingImports]
from tests.helpers.storage import (
    local_zarr_handle,
    make_local_session,
    make_test_session,
)


def test_staged_write_resume_preserves_cumulative_shape(tmp_path: Path) -> None:
    final_store = tmp_path / "product.zarr"
    temp_store = tmp_path / "temp" / "product.zarr"
    temp_store.parent.mkdir(parents=True, exist_ok=True)

    # Step 1: Create final target with 10 existing timestamps
    ds_old = xr.Dataset(
        {"val": (["timestamp", "x"], np.random.rand(10, 3))},
        coords={"timestamp": np.arange(10), "x": np.arange(3)},
    )
    ds_old.to_zarr(str(final_store), group="G", mode="w")

    # Step 2: Seed temp store from final target
    from firecube.ingestor.runtime.zarr.staged_metadata import seed_staged_store_metadata

    result = seed_staged_store_metadata(
        temp_store_uri=str(temp_store),
        final_target_uri=str(final_store),
        groups=["G"],
        session=make_local_session(str(temp_store)),
    )

    assert result["G"]["seeded"] is True, f"seeding skipped: {result}"
    assert result["G"]["files"] > 0, f"no files seeded: {result}"

    # Step 3: Append 5 new timestamps to temp store using append_time_groups
    from firecube.ingestor.runtime.zarr.append import append_time_groups

    new_ds = xr.Dataset(
        {"val": (["timestamp", "x"], np.random.rand(5, 3))},
        coords={"timestamp": np.arange(10, 15), "x": np.arange(3)},
    )

    append_time_groups(
        store=str(temp_store),
        zarr_store=local_zarr_handle(temp_store),
        session=make_local_session(str(temp_store)),
        resume_zarr_store=local_zarr_handle(final_store, mode="r"),
        group_to_timestamps={"G": list(range(10, 15))},
        dataset_for_batch=lambda g, ts: new_ds,
        resume_existing=True,
    )

    # Step 4: Copy temp -> final via StorageSession.upload_tree()
    make_test_session(tmp_path).upload_tree(
        StorageUri.from_local_path(temp_store),
        StorageUri.from_local_path(final_store),
    )

    # Step 5: Verify final zarr.json has cumulative shape >= 15
    zarr_json_path = final_store / "G" / "val" / "zarr.json"
    assert zarr_json_path.exists(), f"zarr.json missing at {zarr_json_path}"
    meta = json.loads(zarr_json_path.read_text())
    shape0 = meta["shape"][0]
    assert shape0 >= 15, (
        f"Expected cumulative shape[0] >= 15, got {shape0}. "
        "Metadata was overwritten by temp store's smaller shape."
    )
