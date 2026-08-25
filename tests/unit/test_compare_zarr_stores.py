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
from typing import Any

import numpy as np
import pytest
import zarr

from firecube.core.api import FIRECUBE_STATIC_WRITTEN_ATTR, compare_zarr_stores

pytestmark = pytest.mark.unit


def _make_store(
    path: Path,
    *,
    shape: tuple[int, ...] = (2, 2),
    dtype: str = "float32",
    chunks: tuple[int, ...] = (1, 2),
    dimension_names: tuple[str, ...] = ("y", "x"),
    attrs: dict[str, Any] | None = None,
    values: Any | None = None,
    static_written: bool | None = None,
) -> Path:
    root = zarr.open_group(store=str(path), mode="w", zarr_format=3)
    group = root.require_group("data")
    arr = group.create_array(
        "values",
        shape=shape,
        dtype=dtype,
        chunks=chunks,
        dimension_names=dimension_names,
    )
    if attrs:
        arr.attrs.update(attrs)
    data = (
        np.arange(np.prod(shape), dtype=np.dtype(dtype)).reshape(shape)
        if values is None
        else values
    )
    arr[...] = data
    if static_written is not None:
        arr.attrs[FIRECUBE_STATIC_WRITTEN_ATTR] = static_written
    return path


def _compare(a: Path, b: Path):
    return compare_zarr_stores(
        a.as_uri(),
        b.as_uri(),
        storage_type="local",
        storage_driver="fsspec",
    )


def test_identical_stores_are_equivalent(tmp_path: Path) -> None:
    a = _make_store(tmp_path / "a.zarr", attrs={"units": "K"})
    b = _make_store(tmp_path / "b.zarr", attrs={"units": "K"})

    report = _compare(a, b)

    assert report.equivalent is True
    assert report.mismatches == []


def test_shape_mismatch_detected(tmp_path: Path) -> None:
    a = _make_store(tmp_path / "a.zarr", shape=(2, 2), chunks=(1, 2))
    b = _make_store(tmp_path / "b.zarr", shape=(3, 2), chunks=(1, 2))

    report = _compare(a, b)

    assert report.equivalent is False
    assert any("shape" in mismatch for mismatch in report.mismatches)


def test_dtype_mismatch_detected(tmp_path: Path) -> None:
    a = _make_store(tmp_path / "a.zarr", dtype="float32")
    b = _make_store(tmp_path / "b.zarr", dtype="int16")

    report = _compare(a, b)

    assert report.equivalent is False
    assert any("dtype" in mismatch for mismatch in report.mismatches)


def test_chunks_mismatch_detected(tmp_path: Path) -> None:
    a = _make_store(tmp_path / "a.zarr", chunks=(1, 2))
    b = _make_store(tmp_path / "b.zarr", chunks=(2, 1))

    report = _compare(a, b)
    assert report.equivalent is False
    assert any("chunks" in mismatch for mismatch in report.mismatches)


def test_attrs_mismatch_detected(tmp_path: Path) -> None:
    a = _make_store(tmp_path / "a.zarr", attrs={"units": "K"})
    b = _make_store(tmp_path / "b.zarr", attrs={"units": "C"})

    report = _compare(a, b)
    assert report.equivalent is False
    assert any("attrs" in mismatch for mismatch in report.mismatches)


def test_nan_values_at_same_positions_equal(tmp_path: Path) -> None:
    values = np.array([[1.0, np.nan], [3.0, 4.0]], dtype=np.float32)
    a = _make_store(tmp_path / "a.zarr", values=values)
    b = _make_store(tmp_path / "b.zarr", values=values.copy())

    report = _compare(a, b)
    assert report.equivalent is True
    assert report.mismatches == []


def test_static_marker_mismatch_detected(tmp_path: Path) -> None:
    a = _make_store(tmp_path / "a.zarr", static_written=True)
    b = _make_store(tmp_path / "b.zarr")

    report = _compare(a, b)
    assert report.equivalent is False
    assert any("firecube_static_written" in mismatch for mismatch in report.mismatches)


def test_ignores_runtime_managed_attrs(tmp_path: Path) -> None:
    a = _make_store(
        tmp_path / "a.zarr", attrs={"firecube_run_id": "run-a", "firecube_span_id": "span-a"}
    )
    b = _make_store(
        tmp_path / "b.zarr", attrs={"firecube_run_id": "run-b", "firecube_span_id": "span-b"}
    )

    report = _compare(a, b)
    assert report.equivalent is True
    assert report.mismatches == []
