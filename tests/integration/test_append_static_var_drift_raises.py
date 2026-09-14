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
from typing import Any

import numpy as np
import pandas as pd
import pytest
import xarray as xr

from firecube.core.storage.session import storage_config_from_binding
from firecube.ingestor.errors import SchemaDriftError
from firecube.ingestor.runtime.zarr.strategies.append import AppendStrategy
from firecube.ingestor.runtime.zarr.write import write_dataset_to_zarr
from tests.helpers.storage import local_zarr_handle, make_local_session

pytestmark = pytest.mark.integration

_GROUP = "G"


def _dataset(
    start: str,
    *,
    static: np.ndarray | None = None,
    extra_static: bool = False,
) -> xr.Dataset:
    timestamps = pd.date_range(start, periods=2, freq="h")
    data_vars: dict[str, Any] = {
        "dynamic": (("timestamp", "x"), np.arange(6, dtype=np.float32).reshape(2, 3)),
    }
    if static is not None:
        data_vars["static"] = (("x",), static)
    if extra_static:
        data_vars["new_static"] = (("x",), np.array([5, 6, 7], dtype=np.int16))
    return xr.Dataset(
        data_vars,
        coords={"timestamp": timestamps, "x": np.arange(3)},
    )


def _write_first(store: Path, ds: xr.Dataset) -> None:
    write_dataset_to_zarr(
        ds,
        zarr_store=local_zarr_handle(store, mode="w"),
        group=_GROUP,
        mode="w",
        time_dim="timestamp",
        compression=False,
    )


def _append(store: Path, ds: xr.Dataset, *, force_reingest: bool = False) -> None:
    write_dataset_to_zarr(
        ds,
        zarr_store=local_zarr_handle(store, mode="a"),
        preflight_compare_zarr_store=local_zarr_handle(store, mode="r"),
        group=_GROUP,
        mode="a",
        time_dim="timestamp",
        compression=False,
        force_reingest=force_reingest,
    )


def test_fresh_first_write_stores_static_var(tmp_path: Path) -> None:
    store = tmp_path / "fresh.zarr"

    _write_first(store, _dataset("2024-01-01", static=np.array([1, 2, 3], dtype=np.float32)))

    opened = xr.open_zarr(store, group=_GROUP, consolidated=False)
    assert "static" in opened.data_vars
    np.testing.assert_array_equal(opened["static"].values, np.array([1, 2, 3], dtype=np.float32))


def test_same_static_value_across_runs_does_not_raise(tmp_path: Path) -> None:
    store = tmp_path / "same.zarr"
    _write_first(store, _dataset("2024-01-01", static=np.array([1, 2, 3], dtype=np.float32)))

    _append(store, _dataset("2024-01-01T02:00", static=np.array([1, 2, 3], dtype=np.float32)))

    opened = xr.open_zarr(store, group=_GROUP, consolidated=False)
    assert opened.sizes["timestamp"] == 4


def test_different_static_value_raises_schema_drift(tmp_path: Path) -> None:
    store = tmp_path / "drift.zarr"
    _write_first(store, _dataset("2024-01-01", static=np.array([1, 2, 3], dtype=np.float32)))

    with pytest.raises(SchemaDriftError, match="static"):
        _append(store, _dataset("2024-01-01T02:00", static=np.array([1, 9, 3], dtype=np.float32)))


def test_new_static_var_on_append_raises_schema_drift(tmp_path: Path) -> None:
    store = tmp_path / "new-static.zarr"
    _write_first(store, _dataset("2024-01-01", static=np.array([1, 2, 3], dtype=np.float32)))

    with pytest.raises(SchemaDriftError, match="new_static"):
        _append(
            store,
            _dataset(
                "2024-01-01T02:00",
                static=np.array([1, 2, 3], dtype=np.float32),
                extra_static=True,
            ),
        )


def test_same_static_value_float32_float64_does_not_raise(tmp_path: Path) -> None:
    store = tmp_path / "dtype-tolerant.zarr"
    _write_first(store, _dataset("2024-01-01", static=np.array([1, 2, 3], dtype=np.float32)))

    _append(store, _dataset("2024-01-01T02:00", static=np.array([1, 2, 3], dtype=np.float64)))

    opened = xr.open_zarr(store, group=_GROUP, consolidated=False)
    assert opened.sizes["timestamp"] == 4


def test_force_reingest_static_drift_raises_new_store_guidance(tmp_path: Path) -> None:
    store = tmp_path / "force.zarr"
    _write_first(store, _dataset("2024-01-01", static=np.array([1, 2, 3], dtype=np.float32)))

    with pytest.raises(SchemaDriftError, match="Create a new store"):
        _append(
            store,
            _dataset("2024-01-01T02:00", static=np.array([1, 9, 3], dtype=np.float32)),
            force_reingest=True,
        )


def test_staged_preflight_compare_opens_final_target_not_workspace(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    workspace = tmp_path / "workspace" / "product.zarr"
    final = tmp_path / "final" / "product.zarr"
    final.parent.mkdir(parents=True)
    workspace.parent.mkdir(parents=True)
    captured: list[str] = []

    from firecube.ingestor.runtime.zarr import append as append_module

    original_write = append_module.write_dataset_to_zarr

    def capturing_write(*args: Any, **kwargs: Any) -> None:
        handle = kwargs.get("preflight_compare_zarr_store")
        if handle is not None:
            captured.append(str(handle.target_uri))
        original_write(*args, **kwargs)

    monkeypatch.setattr(append_module, "write_dataset_to_zarr", capturing_write)
    session = make_local_session(str(workspace))
    storage_config = storage_config_from_binding(session._binding)
    strategy = AppendStrategy(
        store=str(workspace),
        store_uri=str(workspace),
        resume_target_uri=None,
        preflight_compare_target_uri=str(final),
        chunk_shape={"timestamp": 2},
        compression=False,
        append_dim="timestamp",
        logger=logging.getLogger("test.staged-preflight"),
        storage_config=storage_config,
        session=session,
        pipeline_write_mode="staged",
        final_target_uri=str(final),
    )

    strategy.write_groups(
        group_to_timestamps={_GROUP: list(pd.date_range("2024-01-01", periods=2, freq="h"))},
        dataset_for_batch=lambda _group, _ts: _dataset(
            "2024-01-01", static=np.array([1, 2, 3], dtype=np.float32)
        ),
        batch_size=2,
    )

    assert captured == [f"file://{final}"]
    assert captured[0] != f"file://{workspace}"
