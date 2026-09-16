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

"""Refuse — never silently sort — an unsorted existing coord.

`AppendResumeService.classify_incoming` uses `coord.values[-1]` as the append
tail marker; that is only correct on a monotonically non-decreasing axis.
When the axis is unsorted we must raise
``AppendOverwriteRefused(reason="unsorted_existing_coord")`` BEFORE any
`existing_max`/`existing_last` computation, and the error message must direct
the user to `firecube zarr validate` for diagnosis.
"""

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
        "logger": logging.getLogger("test-v2-f9-unsorted-existing-refused"),
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


def test_error_message_points_at_zarr_validate(tmp_path: Path) -> None:
    """Error message must direct users at `firecube zarr validate` for diagnosis."""
    store_path = tmp_path / "f9-unsorted-message.zarr"
    stored = np.array(
        ["2024-06-15", "2024-01-01", "2024-03-10"],
        dtype="datetime64[s]",
    )
    _write_store(store_path, stored)
    incoming = np.array(["2024-07-01"], dtype="datetime64[s]")

    with pytest.raises(AppendOverwriteRefused) as exc_info:
        AppendResumeService(**_service_kwargs(store_path)).classify_dataset(
            ds=_dataset(incoming),
            group="default",
        )

    message = str(exc_info.value)
    assert "unsorted_existing_coord" in message
    assert "firecube zarr validate" in message
    assert "validate" in message


def test_unsorted_incoming_refused_via_classify_dataset(tmp_path: Path) -> None:
    """an out-of-order incoming batch is refused before any tail comparison."""
    store_path = tmp_path / "f9-unsorted-incoming.zarr"
    stored = np.array(["2024-01-01", "2024-01-02"], dtype="datetime64[s]")
    _write_store(store_path, stored)
    incoming = np.array(["2024-01-04", "2024-01-03", "2024-01-05"], dtype="datetime64[s]")

    with pytest.raises(AppendOverwriteRefused) as exc_info:
        AppendResumeService(**_service_kwargs(store_path)).classify_dataset(
            ds=_dataset(incoming),
            group="default",
        )

    assert exc_info.value.reason == "unsorted_incoming"
    assert exc_info.value.refused_timestamps == ["2024-01-04 00:00:00", "2024-01-03 00:00:00"]
    message = str(exc_info.value)
    assert "build_dataset" in message
    assert "pipeline_batch_size" in message
