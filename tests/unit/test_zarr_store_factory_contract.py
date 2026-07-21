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

"""Boundary contract tests for create_zarr_store."""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pytest
import xarray as xr
from zarr.storage import LocalStore

from firecube.core.config import StorageConfig
from firecube.core.filesystem.store_factory import create_zarr_store


@pytest.mark.unit
@pytest.mark.parametrize("variant", ["file-uri", "bare-path"])
def test_create_zarr_store_returns_localstore_for_local_targets(
    tmp_path: Path,
    variant: str,
) -> None:
    target = tmp_path / f"T4.4-{variant}.zarr"
    uri = target.as_uri() if variant == "file-uri" else str(target)
    fsspec_local_config = StorageConfig(storage_type="local", storage_driver="fsspec")

    result = create_zarr_store(uri=uri, storage_config=fsspec_local_config, mode="w")

    assert isinstance(result.store, LocalStore)
    assert result.storage_options is None

    xr.Dataset({"x": (["t"], np.arange(3))}).to_zarr(
        store=result.store,
        group="G",
        mode="w",
        zarr_format=3,
    )

    assert (target / "G" / "zarr.json").exists()


@pytest.mark.unit
def test_create_zarr_store_returns_string_store_and_storage_options_for_s3() -> None:
    sc = StorageConfig(
        storage_type="s3",
        storage_driver="fsspec",
        endpoint_url="http://localhost:9000",
        access_key="key",
        secret_key="secret",
        region="us-east-1",
    )

    result = create_zarr_store(
        uri="s3://test-bucket/T4.4-s3.zarr",
        storage_config=sc,
        mode="w",
    )

    assert isinstance(result.store, str)
    assert isinstance(result.storage_options, dict)
