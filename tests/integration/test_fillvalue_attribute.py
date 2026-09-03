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

import json
from pathlib import Path
from typing import Any, Literal, cast

import numpy as np
import pytest
import xarray as xr
import zarr
from zarr.storage import LocalStore

from firecube.core.zarr.region_writer import RegionZarrWriter

pytestmark = pytest.mark.integration

_GROUP = "grid"
_ARRAY = "counts"
_DIMS = ("y", "x")
_SHAPE = (2, 2)
_DTYPE = np.uint16
_FILL_VALUE = np.uint16(65535)


def _open_counts(store_path: Path, *, mode: Literal["r", "a"] = "r") -> Any:
    root = zarr.open_group(store=LocalStore(str(store_path)), mode=mode, zarr_format=3)
    return cast(Any, cast(Any, root[_GROUP])[_ARRAY])


def test_fill_value_round_trip_masks_unwritten_cells(tmp_path: Path) -> None:
    store_path = tmp_path / "fillvalue.zarr"
    writer = RegionZarrWriter(f"file://{store_path}")
    writer.ensure_group(
        f"{_GROUP}/{_ARRAY}",
        shape=_SHAPE,
        dtype=_DTYPE,
        fill_value=_FILL_VALUE,
        dimension_names=_DIMS,
    )

    counts = _open_counts(store_path, mode="a")
    counts[0, :] = np.array([7, 11], dtype=_DTYPE)

    ds = xr.open_dataset(
        str(store_path),
        engine="zarr",
        group=_GROUP,
        mask_and_scale=True,
        consolidated=False,
    )
    try:
        data = ds[_ARRAY]
        assert data.encoding["_FillValue"] == int(_FILL_VALUE)
        assert np.issubdtype(data.dtype, np.floating)
        assert np.isnan(data.values[1, 0])
        assert np.isnan(data.values[1, 1])
        assert dict(_open_counts(store_path).attrs)["_FillValue"] == int(_FILL_VALUE)
    finally:
        ds.close()


def test_resume_backfills_missing_fill_value_attr(tmp_path: Path) -> None:
    store_path = tmp_path / "resume.zarr"
    root = zarr.open_group(store=LocalStore(str(store_path)), mode="a", zarr_format=3)
    root.require_group(_GROUP).create_array(
        name=_ARRAY,
        shape=_SHAPE,
        dtype=_DTYPE,
        fill_value=_FILL_VALUE,
        dimension_names=_DIMS,
    )

    existing = _open_counts(store_path)
    assert dict(existing.attrs).get("_FillValue") is None

    writer = RegionZarrWriter(f"file://{store_path}")
    created = writer.ensure_group(
        f"{_GROUP}/{_ARRAY}",
        shape=_SHAPE,
        dtype=_DTYPE,
        fill_value=_FILL_VALUE,
        dimension_names=_DIMS,
    )
    assert dict(created.attrs)["_FillValue"] == int(_FILL_VALUE)

    resumed = writer.ensure_group(
        f"{_GROUP}/{_ARRAY}",
        shape=_SHAPE,
        dtype=_DTYPE,
        fill_value=_FILL_VALUE,
        dimension_names=_DIMS,
    )
    assert dict(resumed.attrs)["_FillValue"] == int(_FILL_VALUE)


def test_datetime64_nat_fill_array_is_xarray_openable(tmp_path: Path) -> None:
    store_path = tmp_path / "nat-fill.zarr"
    writer = RegionZarrWriter(f"file://{store_path}")
    arr = writer.ensure_group(
        f"{_GROUP}/{_ARRAY}",
        shape=_SHAPE,
        dtype=np.dtype("datetime64[s]"),
        fill_value=np.datetime64("NaT", "s"),
        dimension_names=_DIMS,
    )

    assert "_FillValue" not in dict(arr.attrs)

    ds = xr.open_dataset(
        str(store_path),
        engine="zarr",
        group=_GROUP,
        mask_and_scale=True,
        consolidated=False,
    )
    try:
        assert "_FillValue" not in dict(_open_counts(store_path).attrs)
    finally:
        ds.close()


def test_nan_fill_array_gets_no_fill_value_attr(tmp_path: Path) -> None:
    store_path = tmp_path / "nan-fill.zarr"
    writer = RegionZarrWriter(f"file://{store_path}")
    arr = writer.ensure_group(
        f"{_GROUP}/{_ARRAY}",
        shape=_SHAPE,
        dtype=np.float32,
        fill_value=np.nan,
        dimension_names=_DIMS,
    )

    assert "_FillValue" not in dict(arr.attrs)
    json.loads((store_path / _GROUP / _ARRAY / "zarr.json").read_text())


def test_finite_float_fill_array_is_xarray_openable(tmp_path: Path) -> None:
    # A bare float _FillValue attr makes xr.open_zarr raise TypeError for
    # float dtypes; the attr must carry the base64-packed IEEE-754 form.
    store_path = tmp_path / "float-fill.zarr"
    writer = RegionZarrWriter(f"file://{store_path}")
    arr = writer.ensure_group(
        f"{_GROUP}/{_ARRAY}",
        shape=_SHAPE,
        dtype=np.float32,
        fill_value=np.float32(-999.0),
        dimension_names=_DIMS,
    )

    stamped = dict(arr.attrs)["_FillValue"]
    assert isinstance(stamped, str)

    counts = _open_counts(store_path, mode="a")
    counts[0, :] = np.array([7.0, 11.0], dtype=np.float32)

    ds = xr.open_dataset(
        str(store_path),
        engine="zarr",
        group=_GROUP,
        mask_and_scale=True,
        consolidated=False,
    )
    try:
        data = ds[_ARRAY]
        assert data.encoding["_FillValue"] == -999.0
        assert data.values[0, 0] == 7.0
        assert np.isnan(data.values[1, 0])
        assert np.isnan(data.values[1, 1])
    finally:
        ds.close()


def test_integer_fill_value_attr_still_stamped(tmp_path: Path) -> None:
    store_path = tmp_path / "int-fill.zarr"
    writer = RegionZarrWriter(f"file://{store_path}")
    arr = writer.ensure_group(
        f"{_GROUP}/{_ARRAY}",
        shape=_SHAPE,
        dtype=np.uint16,
        fill_value=np.uint16(65535),
        dimension_names=_DIMS,
    )

    assert dict(arr.attrs)["_FillValue"] == 65535
