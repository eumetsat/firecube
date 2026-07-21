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

from typing import Any
from unittest.mock import MagicMock

import numpy as np
import pandas as pd
import pytest
import xarray as xr

from firecube.ingestor.templates.generic import GenericZarrIngestor
from tests.helpers.storage import local_zarr_handle, make_local_session


class _NewStyleIngestor(GenericZarrIngestor):
    PRODUCT_NAME = "new_style"
    name = "new_style"

    def __init__(self):
        super().__init__(name="new_style")
        self.received_items: list[list[Any]] = []

    def build_dataset(self, group: str, items: list[Any], ctx) -> xr.Dataset | None:
        self.received_items.append(list(items))
        return None

    def ingest(self, ctx: Any) -> Any:
        raise NotImplementedError


@pytest.mark.unit
def test_items_subset_forwarded_to_build_dataset(tmp_path):
    ingestor = _NewStyleIngestor()
    ctx = MagicMock()

    all_timestamps = list(pd.date_range("2024-01-01", periods=9, freq="h"))

    from firecube.ingestor.runtime.zarr.append import append_time_groups

    received: list[list[Any]] = []

    def capturing_build_dataset(group: str, items: list[Any], ctx_) -> xr.Dataset | None:
        received.append(list(items))
        ts = pd.to_datetime(list(items))
        data = np.zeros((len(ts), 2, 3), dtype=np.float32)
        return xr.Dataset(
            {"FWI": (("timestamp", "lat", "lon"), data)},
            coords={"timestamp": ts, "lat": np.arange(2), "lon": np.arange(3)},
        )

    ingestor.build_dataset = lambda group, items, ctx: capturing_build_dataset(
        group,
        items,
        ctx,
    )

    store = str(tmp_path / "out.zarr")
    append_time_groups(
        store=store,
        zarr_store=local_zarr_handle(store),
        session=make_local_session(store),
        group_to_timestamps={"default": all_timestamps},
        dataset_for_batch=lambda g, items: ingestor.build_dataset(g, list(items), ctx),
        chunk_shape={"timestamp": 3, "lat": 2, "lon": 3},
        compression=False,
        consolidate=False,
        resume_existing=False,
        batch_size=3,
    )

    assert len(received) == 3, f"Expected 3 sub-batch calls, got {len(received)}"
    for sub_batch in received:
        assert len(sub_batch) == 3, f"Expected 3 items per sub-batch, got {len(sub_batch)}"

    all_received = [item for sub in received for item in sub]
    assert len(all_received) == 9
