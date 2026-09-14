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

import logging
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
import xarray as xr
import zarr

from firecube.ingestor.runtime.zarr.write import write_dataset_to_zarr
from tests.helpers.storage import local_zarr_handle

pytestmark = pytest.mark.integration

_GROUP = "G"


def _dataset(start: str, *, title: str) -> xr.Dataset:
    timestamps = pd.date_range(start, periods=2, freq="h")
    return xr.Dataset(
        {
            "dynamic": (("timestamp", "x"), np.ones((2, 3), dtype=np.float32)),
            "static": (("x",), np.array([1, 2, 3], dtype=np.float32)),
        },
        coords={"timestamp": timestamps, "x": np.arange(3)},
        attrs={"title": title},
    )


def _write(store: Path, ds: xr.Dataset, *, mode: str, logger: logging.Logger) -> None:
    write_dataset_to_zarr(
        ds,
        zarr_store=local_zarr_handle(store, mode=mode),
        preflight_compare_zarr_store=local_zarr_handle(store, mode="r") if mode == "a" else None,
        group=_GROUP,
        mode=mode,  # type: ignore[arg-type]
        time_dim="timestamp",
        compression=False,
        logger=logger,
    )


def _stored_attrs(store: Path) -> dict[str, object]:
    root = zarr.open_group(str(store), mode="r", zarr_format=3)
    return dict(root[_GROUP].attrs)


def test_first_write_emits_no_attr_warning(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    logger = logging.getLogger("test.attrs.first")
    store = tmp_path / "attrs-first.zarr"

    with caplog.at_level(logging.WARNING, logger=logger.name):
        _write(store, _dataset("2024-01-01", title="first"), mode="w", logger=logger)

    assert "Group attributes differ" not in caplog.text
    assert _stored_attrs(store)["title"] == "first"


def test_second_write_warns_once_and_preserves_first_attrs(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    logger = logging.getLogger("test.attrs.second")
    store = tmp_path / "attrs-second.zarr"
    _write(store, _dataset("2024-01-01", title="first"), mode="w", logger=logger)

    with caplog.at_level(logging.WARNING, logger=logger.name):
        _write(store, _dataset("2024-01-01T02:00", title="second"), mode="a", logger=logger)

    records = [
        record
        for record in caplog.records
        if record.message == "Group attributes differ from stored; keeping first-write values"
    ]
    assert len(records) == 1
    assert "title" in getattr(records[0], "changed", ())
    assert _stored_attrs(store)["title"] == "first"


def test_fresh_staged_two_batches_preserves_first_write_attrs(
    tmp_path: Path,
) -> None:
    logger = logging.getLogger("test.attrs.fresh_staged")
    workspace_store = tmp_path / "workspace.zarr"
    final_store = tmp_path / "final-target.zarr"

    write_dataset_to_zarr(
        _dataset("2024-01-01", title="first"),
        zarr_store=local_zarr_handle(workspace_store, mode="w"),
        group=_GROUP,
        mode="w",
        time_dim="timestamp",
        compression=False,
        logger=logger,
    )
    write_dataset_to_zarr(
        _dataset("2024-01-01T02:00", title="second"),
        zarr_store=local_zarr_handle(workspace_store, mode="a"),
        preflight_compare_zarr_store=local_zarr_handle(final_store, mode="r"),
        group=_GROUP,
        mode="a",
        time_dim="timestamp",
        compression=False,
        logger=logger,
    )

    assert _stored_attrs(workspace_store)["title"] == "first"
