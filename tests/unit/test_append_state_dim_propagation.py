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

from collections.abc import Sequence
from typing import Any, cast

import numpy as np
import pytest
import xarray as xr
import zarr

from firecube.ingestor.runtime.zarr.append import append_time_groups
from tests.helpers.storage import local_zarr_handle


@pytest.mark.unit
def test_state_array_uses_declared_time_dim(tmp_path):
    target = tmp_path / "out.zarr"

    ds = xr.Dataset(
        {"pixel": (("time", "y", "x"), np.zeros((3, 4, 5), dtype=np.float32))},
        coords={"time": np.arange(3, dtype=np.float64)},
    )
    ds.to_zarr(str(target), group="default", mode="w", zarr_format=3, consolidated=False)

    def dataset_for_batch(_group: str, _batch: Sequence[Any]) -> xr.Dataset:
        return xr.Dataset(
            {"pixel": (("time", "y", "x"), np.zeros((1, 4, 5), dtype=np.float32))},
            coords={"time": np.array([99.0], dtype=np.float64)},
        )

    metrics = append_time_groups(
        store=str(target),
        zarr_store=local_zarr_handle(target),
        group_to_timestamps={"default": [99.0]},
        dataset_for_batch=dataset_for_batch,
        append_dim="time",
    )

    arr = zarr.open_array(str(target), path="default/firecube_timestamp_state", mode="r")
    ds = xr.open_zarr(str(target), group="default", consolidated=False)
    assert metrics["coverage"][0]["time_index_ranges"] == [[3, 3]]
    assert metrics["coverage"][0]["time_dim_name"] == "time"
    assert ds.sizes["time"] == 4
    assert cast(Any, arr.metadata).dimension_names == ("time",)
