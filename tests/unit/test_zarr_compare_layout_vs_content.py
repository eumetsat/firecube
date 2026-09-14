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
import zarr

from firecube.core.api import compare_zarr_stores

pytestmark = pytest.mark.unit


def _make_store(
    path: Path,
    *,
    shape: tuple[int, ...] = (4, 4),
    dtype: str = "float32",
    chunks: tuple[int, ...] = (2, 2),
    values: np.ndarray | None = None,
) -> Path:
    root = zarr.open_group(store=str(path), mode="w", zarr_format=3)
    arr = root.create_array(
        "data",
        shape=shape,
        dtype=dtype,
        chunks=chunks,
    )
    arr[...] = (
        np.arange(np.prod(shape), dtype=np.dtype(dtype)).reshape(shape)
        if values is None
        else values
    )
    return path


def _compare(a: Path, b: Path):
    return compare_zarr_stores(
        a.as_uri(),
        b.as_uri(),
        storage_type="local",
        storage_driver="fsspec",
    )


def test_content_difference_classified_correctly(tmp_path: Path) -> None:
    a = _make_store(tmp_path / "a.zarr")
    b = _make_store(
        tmp_path / "b.zarr",
        values=np.zeros((4, 4), dtype="float32"),
    )

    report = _compare(a, b)

    assert report.equivalent is False
    assert report.content_mismatches != []
    assert report.layout_mismatches == []
    assert any("values differ" in m for m in report.content_mismatches)


def test_layout_difference_classified_correctly(tmp_path: Path) -> None:
    a = _make_store(tmp_path / "a.zarr", chunks=(2, 2))
    b = _make_store(tmp_path / "b.zarr", chunks=(4, 4))

    report = _compare(a, b)

    assert report.equivalent is False
    assert report.content_mismatches == []
    assert report.layout_mismatches != []
    assert any("chunks" in m for m in report.layout_mismatches)


def test_mismatches_property_is_union(tmp_path: Path) -> None:
    a = _make_store(tmp_path / "a.zarr", chunks=(2, 2))
    b = _make_store(
        tmp_path / "b.zarr",
        chunks=(4, 4),
        values=np.zeros((4, 4), dtype="float32"),
    )

    report = _compare(a, b)

    assert report.mismatches == report.content_mismatches + report.layout_mismatches
