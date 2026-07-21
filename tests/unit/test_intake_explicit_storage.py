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

from pathlib import Path

import pytest
import xarray as xr

from firecube.core.config import StorageConfig
from firecube.core.intake import discover_catalog_groups


def _write_group(store: Path, group: str) -> None:
    ds = xr.Dataset({"value": (("time",), [1, 2])}, coords={"time": [0, 1]})
    ds.to_zarr(str(store), group=group, mode="a", consolidated=False, zarr_format=3)


def test_discover_catalog_groups_requires_storage_config(tmp_path: Path) -> None:
    store = tmp_path / "product.zarr"
    _write_group(store, "group_a")

    with pytest.raises(ValueError, match="storage_config is required"):
        discover_catalog_groups(str(store), storage_config=None)


def test_discover_catalog_groups_succeeds_with_explicit_storage_config(tmp_path: Path) -> None:
    store = tmp_path / "product.zarr"
    _write_group(store, "group_a")

    config = StorageConfig(storage_type="local")
    config.target_path = str(tmp_path)  # type: ignore[attr-defined]

    groups = discover_catalog_groups(str(store), storage_config=config)

    assert groups == ["group_a"]
