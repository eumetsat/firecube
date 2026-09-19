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

import pytest
import xarray as xr
import zarr

from firecube.core.config import StorageConfig
from firecube.core.intake import _contains_zarr_arrays, _is_readable_dataset_group, _read_node_type

pytestmark = pytest.mark.unit


def _corrupt_store(tmp_path: Path) -> str:
    store = tmp_path / "corrupt.zarr"
    store.mkdir()
    (store / "zarr.json").write_text("not json", encoding="utf-8")
    return str(store)


def _storage_config() -> StorageConfig:
    return StorageConfig(storage_type="local")


def test_read_node_type_warns_on_corrupt_store(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    store_uri = _corrupt_store(tmp_path)
    caplog.set_level(logging.DEBUG, logger="firecube.core.intake")

    result = _read_node_type(store_uri, None, _storage_config())

    assert result is None
    warnings = [record for record in caplog.records if record.levelno == logging.WARNING]
    assert len(warnings) == 1
    assert store_uri in warnings[0].getMessage()
    assert "JSONDecodeError" in warnings[0].getMessage()


def test_contains_zarr_arrays_warns_on_corrupt_store(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    store_uri = _corrupt_store(tmp_path)
    caplog.set_level(logging.DEBUG, logger="firecube.core.intake")

    result = _contains_zarr_arrays(store_uri, _storage_config())

    assert result is False
    warnings = [record for record in caplog.records if record.levelno == logging.WARNING]
    assert len(warnings) == 1
    assert store_uri in warnings[0].getMessage()
    assert "JSONDecodeError" in warnings[0].getMessage()


def test_is_readable_dataset_group_on_corrupt_store_logs_debug_and_warning(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    store_uri = _corrupt_store(tmp_path)
    caplog.set_level(logging.DEBUG, logger="firecube.core.intake")

    result = _is_readable_dataset_group(store_uri, None, storage_config=_storage_config())

    assert result is False
    assert any(record.levelno == logging.DEBUG for record in caplog.records)
    assert any(record.levelno == logging.WARNING for record in caplog.records)


def test_is_readable_dataset_group_fallback_for_healthy_zarr_array_is_not_warning(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The zarr-array fallback returns True silently for a healthy store.

    When ``xr.open_zarr`` rejects a bare-array store (no CF dimension
    metadata), ``_is_readable_dataset_group`` must fall back to the
    zarr-array probe and return True for a real Zarr array WITHOUT logging
    a WARNING; a WARNING at that level would surface as user-visible noise
    for perfectly-healthy stores that simply lack dimension metadata.
    """
    from firecube.core import intake as _intake

    store = tmp_path / "healthy.zarr"
    group = zarr.open_group(store, mode="w")
    group.create_array("data", shape=(3,), dtype="f4")

    def _reject(*_args: object, **_kwargs: object) -> xr.Dataset:
        raise ValueError("no dimension metadata")

    monkeypatch.setattr(_intake.xr, "open_zarr", _reject)

    caplog.set_level(logging.DEBUG, logger="firecube.core.intake")

    result = _is_readable_dataset_group(str(store), None, storage_config=_storage_config())

    assert result is True
    assert not any(record.levelno >= logging.WARNING for record in caplog.records)
