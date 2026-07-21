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

from contextlib import nullcontext
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import patch

import numpy as np
import pandas as pd
import pytest
import xarray as xr

from firecube.ingestor.runtime.zarr.append import append_time_groups
from firecube.ingestor.templates.generic import _build_zarr_batch_runtime
from tests.helpers.storage import local_zarr_handle, make_local_session


def _dataset(batch_ts, *, append_dim: str) -> xr.Dataset:
    batch_ts = pd.to_datetime(list(batch_ts))
    data = np.arange(len(batch_ts) * 2 * 3, dtype=np.float32).reshape((len(batch_ts), 2, 3))
    return xr.Dataset(
        {"FWI": ((append_dim, "lat", "lon"), data)},
        coords={append_dim: batch_ts, "lat": np.arange(2), "lon": np.arange(3)},
    )


@pytest.mark.unit
def test_append_time_groups_accepts_custom_time_dim(tmp_path):
    store = tmp_path / "custom-time.zarr"
    timestamps = pd.date_range("2024-01-01", periods=3, freq="h")

    metrics = append_time_groups(
        store=str(store),
        zarr_store=local_zarr_handle(store),
        session=make_local_session(str(store)),
        group_to_timestamps={"F024": list(timestamps)},
        dataset_for_batch=lambda _group, batch: _dataset(batch, append_dim="time"),
        arrays_for_group=lambda group: [f"{group}/FWI"],
        chunk_shape={"time": 2, "lat": 2, "lon": 3},
        append_dim="time",
        batch_size=2,
    )

    assert metrics["coverage"][0]["time_index_ranges"] == [[0, 1], [2, 2]]
    ds = xr.open_zarr(str(store), group="F024", consolidated=False)
    assert ds.sizes["time"] == 3
    assert "firecube_timestamp_state" in ds
    assert "timestamp" not in ds.dims


@pytest.mark.unit
def test_append_time_groups_keeps_timestamp_default_back_compat(tmp_path):
    store = tmp_path / "timestamp-default.zarr"
    timestamps = pd.date_range("2024-01-01", periods=2, freq="h")

    append_time_groups(
        store=str(store),
        zarr_store=local_zarr_handle(store),
        session=make_local_session(str(store)),
        group_to_timestamps={"F024": list(timestamps)},
        dataset_for_batch=lambda _group, batch: _dataset(batch, append_dim="timestamp"),
        arrays_for_group=lambda group: [f"{group}/FWI"],
        chunk_shape={"timestamp": 2, "lat": 2, "lon": 3},
        batch_size=2,
    )

    ds = xr.open_zarr(str(store), group="F024", consolidated=False)
    assert ds.sizes["timestamp"] == 2
    assert "firecube_timestamp_state" in ds


@pytest.mark.unit
def test_generic_zarr_runtime_passes_host_time_dim_to_append_strategy():
    ingestor = SimpleNamespace(
        _log=SimpleNamespace(debug=lambda *args, **kwargs: None),
        _chunk_manager=SimpleNamespace(storage_config=object()),
        _write_lock=object(),
        name="test-product",
        _resolve_time_dim_name=lambda: "time",
    )
    ctx = SimpleNamespace(
        storage=None,
        run_id="run-1",
        option=lambda key, default=None: default,
    )

    with (
        patch(
            "firecube.ingestor.templates.generic.batch_runner.build_zarr_write_context",
            return_value=nullcontext(),
        ),
        patch(
            "firecube.ingestor.templates.generic.batch_runner.build_claim_closure_for_append",
            return_value=None,
        ),
        patch(
            "firecube.ingestor.templates.generic.batch_runner.build_append_strategy",
            return_value=object(),
        ) as build_append_strategy,
    ):
        _build_zarr_batch_runtime(
            ingestor,
            cast(Any, ctx),
            store_uri="memory://out.zarr",
            final_target_uri=None,
            groups=["F024"],
            zarr_config={},
            resume_existing=False,
            force_reingest=False,
            write_mode="direct",
        )

    assert build_append_strategy.call_args.kwargs["append_dim"] == "time"
