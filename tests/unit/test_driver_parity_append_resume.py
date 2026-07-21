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

"""Driver-parity tests for AppendResumeService timestamp-state initialization (T4.3).

The append-resume path used to call ``ensure_timestamp_state_array`` with only
``storage_options=...``, which falls into the legacy raw-fsspec branch in
``firecube.core.zarr.state``. That bypassed ``StorageConfig.storage_driver``
and silently downgraded obstore deployments to fsspec — violating the
"one driver everywhere" invariant in AGENTS.md.

After T4.3 the engine derives ``storage_config`` from the active
``StorageSession`` and forwards it through ``AppendTimestampState.ensure_existing``
so ``ensure_timestamp_state_array`` routes through the driver-aware
``_session_zarr_store`` branch. These tests assert that contract.
"""

from __future__ import annotations

import builtins
import logging
import warnings
from pathlib import Path
from typing import Any, cast
from unittest.mock import patch

import numpy as np
import pandas as pd
import pytest
import xarray as xr

from firecube.core.config import StorageConfig
from firecube.core.filesystem.store_factory import create_zarr_store
from firecube.ingestor.runtime.zarr.append import append_time_groups
from firecube.ingestor.runtime.zarr.append_services import (
    AppendResumeService,
    AppendTimestampState,
)
from firecube.ingestor.runtime.zarr.resume_cache import clear_resume_cache
from tests.helpers.storage import assert_no_fsspec_bypass, make_test_session

pytestmark = pytest.mark.unit


@pytest.fixture(autouse=True)
def _clear_cache():
    clear_resume_cache()
    yield
    clear_resume_cache()


def _write_initial_store(store_path: Path, group: str, n_timestamps: int) -> None:
    ts = pd.date_range("2024-01-01", periods=n_timestamps, freq="h")
    ds = xr.Dataset(
        {
            "FWI": (
                ("timestamp", "lat", "lon"),
                np.zeros((n_timestamps, 2, 3), dtype=np.float32),
            )
        },
        coords={"timestamp": ts, "lat": np.arange(2), "lon": np.arange(3)},
    )
    with warnings.catch_warnings():
        warnings.filterwarnings(
            "ignore",
            message="Consolidated metadata is currently not part in the Zarr format 3 specification",
        )
        ds.to_zarr(str(store_path), group=group, mode="w", zarr_format=3, safe_chunks=False)


def _make_resume_ds(n: int = 2, start: str = "2024-01-01T05:00") -> xr.Dataset:
    ts = pd.date_range(start, periods=n, freq="h")
    return xr.Dataset(
        {"FWI": (("timestamp", "lat", "lon"), np.zeros((n, 2, 3), dtype=np.float32))},
        coords={"timestamp": ts, "lat": np.arange(2), "lon": np.arange(3)},
    )


def _append_dataset_for_batch(_group: str, batch_ts) -> xr.Dataset:
    batch_ts = pd.to_datetime(list(batch_ts))
    return xr.Dataset(
        {
            "FWI": (
                ("timestamp", "lat", "lon"),
                np.ones((len(batch_ts), 2, 3), dtype=np.float32),
            )
        },
        coords={"timestamp": batch_ts, "lat": np.arange(2), "lon": np.arange(3)},
    )


def test_append_time_groups_rejects_storage_options() -> None:
    with pytest.raises(TypeError, match="storage_options"):
        cast(Any, append_time_groups)(
            store=object(),
            group_to_timestamps={},
            dataset_for_batch=_append_dataset_for_batch,
            storage_options={"key": "value"},
        )


def test_append_time_groups_rejects_store_uri() -> None:
    with pytest.raises(TypeError, match="store_uri"):
        cast(Any, append_time_groups)(
            store=object(),
            group_to_timestamps={},
            dataset_for_batch=_append_dataset_for_batch,
            store_uri="s3://bucket/path",
        )


def test_append_time_groups_rejects_resume_storage_options() -> None:
    with pytest.raises(TypeError, match="resume_storage_options"):
        cast(Any, append_time_groups)(
            store=object(),
            group_to_timestamps={},
            dataset_for_batch=_append_dataset_for_batch,
            resume_storage_options={"key": "value"},
        )


def test_append_resume_distinct_uris_uses_resume_session(tmp_path: Path) -> None:
    write_target = tmp_path / "write_target.zarr"
    resume_target = tmp_path / "resume_target.zarr"
    _write_initial_store(resume_target, "G1", 2)

    write_session = make_test_session(tmp_path, product="write_target.zarr")
    resume_session = make_test_session(tmp_path, product="resume_target.zarr")
    write_handle = create_zarr_store(
        uri=str(write_target),
        storage_config=StorageConfig(storage_type="local", storage_driver="fsspec"),
        mode="a",
    )
    resume_handle = create_zarr_store(
        uri=str(resume_target),
        storage_config=StorageConfig(storage_type="local", storage_driver="fsspec"),
        mode="r",
    )

    with patch(
        "firecube.ingestor.runtime.zarr.append._read_existing_append_values",
        wraps=__import__(
            "firecube.ingestor.runtime.zarr.append", fromlist=["_read_existing_append_values"]
        )._read_existing_append_values,
    ) as read_values_spy:
        append_time_groups(
            store=str(write_target),
            zarr_store=write_handle,
            resume_zarr_store=resume_handle,
            group_to_timestamps={
                "G1": list(pd.date_range("2024-01-01T02:00", periods=1, freq="h"))
            },
            dataset_for_batch=_append_dataset_for_batch,
            resume_existing=True,
            batch_size=1,
            session=write_session,
            resume_session=resume_session,
        )

    assert (write_target / "G1" / "FWI" / "zarr.json").exists()
    assert read_values_spy.call_args.kwargs["store_uri"] == str(resume_target)


def test_append_resume_obstore_no_bypass(tmp_path: Path) -> None:
    store_path = tmp_path / "obstore_resume.zarr"
    _write_initial_store(store_path, "G1", 2)
    session = make_test_session(tmp_path, product="obstore_resume.zarr", driver="obstore")
    handle = create_zarr_store(
        uri=str(store_path),
        storage_config=StorageConfig(storage_type="local", storage_driver="obstore"),
        mode="a",
    )

    with assert_no_fsspec_bypass():
        append_time_groups(
            store=str(store_path),
            zarr_store=handle,
            group_to_timestamps={
                "G1": list(pd.date_range("2024-01-01T02:00", periods=1, freq="h"))
            },
            dataset_for_batch=_append_dataset_for_batch,
            resume_existing=True,
            batch_size=1,
            session=session,
        )


def test_append_resume_fsspec_no_obstore_imports(tmp_path: Path) -> None:
    store_path = tmp_path / "fsspec_resume.zarr"
    _write_initial_store(store_path, "G1", 2)
    session = make_test_session(tmp_path, product="fsspec_resume.zarr", driver="fsspec")
    handle = create_zarr_store(
        uri=str(store_path),
        storage_config=StorageConfig(storage_type="local", storage_driver="fsspec"),
        mode="a",
    )
    original_import = builtins.__import__

    def guard_obstore_import(name, globals=None, locals=None, fromlist=(), level=0):
        if name == "obstore" or name.startswith("obstore."):
            raise AssertionError(f"fsspec append-resume path imported {name!r}")
        return original_import(name, globals, locals, fromlist, level)

    with patch("builtins.__import__", side_effect=guard_obstore_import):
        append_time_groups(
            store=str(store_path),
            zarr_store=handle,
            group_to_timestamps={
                "G1": list(pd.date_range("2024-01-01T02:00", periods=1, freq="h"))
            },
            dataset_for_batch=_append_dataset_for_batch,
            resume_existing=True,
            batch_size=1,
            session=session,
        )


def test_ensure_timestamp_state_no_bypass(tmp_path: Path) -> None:
    """``AppendResumeService.prepare_write`` must hit ``_session_zarr_store``.

    With a session bound to the engine, the resume path must:
      1. NOT call the legacy fsspec opener (``_open_fsspec_url``).
      2. Route the state-array creation through ``_session_zarr_store`` with
         a non-None ``storage_config`` derived from the session.
    """
    store_path = tmp_path / "product.zarr"
    _write_initial_store(store_path, "G1", 4)
    store = str(store_path)

    session = make_test_session(tmp_path, product="product.zarr")

    svc = AppendResumeService(
        read_source_uri=store,
        read_storage_options=None,
        resume_existing=False,
        append_dim="timestamp",
        chunk_shape=None,
        shard_shape=None,
        sharding=False,
        logger=logging.getLogger("test-driver-parity"),
        session=session,
    )

    ds = _make_resume_ds(2)
    ts_state = AppendTimestampState("firecube_timestamp_state", time_dim_name="timestamp")
    ds = ts_state.attach(ds, append_dim="timestamp")

    with (
        assert_no_fsspec_bypass(),
        patch(
            "firecube.core.zarr.state._session_zarr_store",
            wraps=__import__(
                "firecube.core.zarr.state", fromlist=["_session_zarr_store"]
            )._session_zarr_store,
        ) as session_store_spy,
    ):
        svc.prepare_write(
            ds=ds,
            group="G1",
            store=store,
            write_target_uri=store,
            arrays_for_group=None,
            ts_state=ts_state,
        )

    assert session_store_spy.call_count == 1, (
        "AppendResumeService.prepare_write must route ensure_timestamp_state_array "
        f"through _session_zarr_store; got {session_store_spy.call_count} calls."
    )
    forwarded_config = session_store_spy.call_args.kwargs.get("storage_config")
    assert forwarded_config is not None, (
        "ensure_timestamp_state_array received storage_config=None — caller fell "
        "into the legacy fsspec branch."
    )


def test_ensure_existing_routes_through_session_branch(tmp_path: Path) -> None:
    """Direct ``ensure_existing(storage_config=...)`` call hits the session branch.

    Guards the contract for unit-level callers that bypass ``AppendResumeService``.
    """
    from firecube.core.storage.session import storage_config_from_binding

    store_path = tmp_path / "direct.zarr"
    _write_initial_store(store_path, "G1", 3)
    session = make_test_session(tmp_path, product="direct.zarr")
    storage_config = storage_config_from_binding(session._binding)

    svc = AppendTimestampState("firecube_timestamp_state", time_dim_name="timestamp")
    with (
        assert_no_fsspec_bypass(),
        patch(
            "firecube.core.zarr.state._session_zarr_store",
            wraps=__import__(
                "firecube.core.zarr.state", fromlist=["_session_zarr_store"]
            )._session_zarr_store,
        ) as session_store_spy,
    ):
        svc.ensure_existing(
            store_uri=str(store_path),
            group="G1",
            existing_time=3,
            chunk_len=2,
            cached=None,
            resume_cache_key=None,
            preexisting_values=None,
            storage_config=storage_config,
        )

    assert session_store_spy.call_count == 1
