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

import numpy as np
import pytest
import xarray as xr

from firecube.core.intake import CatalogSourceSpec, _catalog_entry_args

pytestmark = pytest.mark.unit


def test_zarr_entry_args_chunks_default_is_on_disk_chunking() -> None:
    spec = CatalogSourceSpec(name="test", description="test", group="", data_format="zarr")

    args = _catalog_entry_args(store_uri="file:///tmp/some.zarr", spec=spec, storage_opts={})

    assert args["chunks"] == {}
    assert "urlpath" in args
    assert "consolidated" in args


def test_zarr_entry_args_survives_object_dtype_variable(tmp_path: Path) -> None:
    store = tmp_path / "test.zarr"
    ds = xr.Dataset({"str_var": (["x"], np.array(["a", "b"], dtype=object))})
    ds.to_zarr(store, mode="w")
    spec = CatalogSourceSpec(name="test", description="test", group="", data_format="zarr")

    args = _catalog_entry_args(store_uri=str(store), spec=spec, storage_opts={})

    opened = xr.open_zarr(
        args["urlpath"],
        group=args.get("group") or None,
        consolidated=args.get("consolidated", False),
        storage_options=args.get("storage_options"),
        chunks=args["chunks"],
    )
    try:
        assert args["chunks"] == {}
        assert "str_var" in opened.data_vars
    finally:
        opened.close()


def test_parquet_entry_args_unchanged() -> None:
    spec = CatalogSourceSpec(name="test", description="test", group="", data_format="parquet")

    args = _catalog_entry_args(store_uri="file:///tmp/some.parquet", spec=spec, storage_opts={})

    assert "chunks" not in args
    assert args["urlpath"] == "file:///tmp/some.parquet"
    assert args["engine"] == "pyarrow"
