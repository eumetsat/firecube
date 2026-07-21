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

"""Regression guard for the staged-metadata pollution bug."""

from __future__ import annotations

import logging
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
import xarray as xr

from firecube.core.config import StorageConfig
from firecube.core.filesystem.store_factory import create_zarr_store
from firecube.core.storage.uri import StorageUri
from firecube.ingestor.runtime.zarr.append import append_time_groups
from firecube.ingestor.runtime.zarr.staged_metadata import seed_staged_store_metadata
from tests.helpers.storage import (
    assert_no_fsspec_bypass,
    local_zarr_handle,
    make_local_session,
    make_test_session,
)

pytestmark = pytest.mark.integration


def _write_group(path: Path, *, timestamps: np.ndarray, value: float) -> None:
    ds = xr.Dataset(
        {"val": (["timestamp", "x"], np.full((len(timestamps), 3), value, dtype=np.float32))},
        coords={"timestamp": timestamps, "x": np.arange(3)},
    )
    ds.to_zarr(str(path), group="G", mode="w", zarr_format=3)


def _append_dataset(_group: str, batch_ts) -> xr.Dataset:
    timestamps = np.asarray(list(batch_ts))
    return xr.Dataset(
        {
            "val": (
                ["timestamp", "x"],
                np.full((len(timestamps), 3), 7.0, dtype=np.float32),
            )
        },
        coords={"timestamp": timestamps, "x": np.arange(3)},
    )


def test_obstore_no_bypass_guard_does_not_pollute_staged_resume(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.DEBUG, logger="firecube.ingestor.runtime.zarr")

    obstore_path = tmp_path / "obstore_resume.zarr"
    _write_group(
        obstore_path,
        timestamps=pd.date_range("2024-01-01", periods=2, freq="h"),  # type: ignore[arg-type]
        value=1.0,
    )
    obstore_session = make_test_session(tmp_path, product="obstore_resume.zarr", driver="obstore")
    obstore_handle = create_zarr_store(
        uri=str(obstore_path),
        storage_config=StorageConfig(storage_type="local", storage_driver="obstore"),
        mode="a",
    )

    with assert_no_fsspec_bypass():
        append_time_groups(
            store=str(obstore_path),
            zarr_store=obstore_handle,
            group_to_timestamps={"G": list(pd.date_range("2024-01-01T02:00", periods=1, freq="h"))},
            dataset_for_batch=_append_dataset,
            resume_existing=True,
            batch_size=1,
            session=obstore_session,
        )

    obstore_readback = xr.open_zarr(str(obstore_path), group="G", zarr_format=3)
    try:
        assert obstore_readback.sizes["timestamp"] == 2
        np.testing.assert_array_equal(
            obstore_readback["timestamp"].values,
            pd.date_range("2024-01-01", periods=2, freq="h").values,
        )
    finally:
        obstore_readback.close()

    final_store = tmp_path / "product.zarr"
    temp_store = tmp_path / "temp" / "product.zarr"
    temp_store.parent.mkdir(parents=True, exist_ok=True)
    _write_group(final_store, timestamps=np.arange(10), value=3.0)

    seed_result = seed_staged_store_metadata(
        temp_store_uri=str(temp_store),
        final_target_uri=str(final_store),
        groups=["G"],
        session=make_local_session(str(temp_store)),
    )
    assert seed_result["G"]["seeded"] is True
    assert seed_result["G"]["files"] > 0

    append_time_groups(
        store=str(temp_store),
        zarr_store=local_zarr_handle(temp_store),
        session=make_local_session(str(temp_store)),
        resume_zarr_store=local_zarr_handle(final_store, mode="r"),
        group_to_timestamps={"G": list(range(10, 15))},
        dataset_for_batch=_append_dataset,
        resume_existing=True,
    )

    make_test_session(tmp_path).upload_tree(
        StorageUri.from_local_path(temp_store),
        StorageUri.from_local_path(final_store),
    )

    final = xr.open_zarr(str(final_store), group="G", zarr_format=3)
    try:
        assert final.sizes["timestamp"] == 15
        np.testing.assert_array_equal(final["timestamp"].values, np.arange(15))
        np.testing.assert_array_equal(
            final["val"].isel(timestamp=slice(10, 15)).values,
            np.full((5, 3), 7.0, dtype=np.float32),
        )
    finally:
        final.close()

    assert not any(
        "Staged metadata seeding skipped" in record.getMessage() for record in caplog.records
    )
