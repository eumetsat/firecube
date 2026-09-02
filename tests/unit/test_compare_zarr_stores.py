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
from firecube.core.zarr import validation

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


def _add_scalar_array(path: Path, value: int) -> None:
    root = zarr.open_group(store=str(path), mode="r+")
    scalar = root.require_group("data").create_array(
        "spatial_ref", shape=(), dtype="int32", chunks=()
    )
    scalar[...] = value


def test_zero_dimensional_arrays_compare(tmp_path: Path) -> None:
    # A CF grid-mapping scalar (shape ()) must compare, not crash on indexing.
    a = _make_store(tmp_path / "a.zarr")
    b = _make_store(tmp_path / "b.zarr")
    _add_scalar_array(a, 0)
    _add_scalar_array(b, 0)

    report = _compare(a, b)

    assert report.equivalent is True

    c = _make_store(tmp_path / "c.zarr")
    _add_scalar_array(c, 7)
    report = _compare(a, c)

    assert report.equivalent is False
    assert any("values differ" in m for m in report.mismatches)


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


def test_multi_slab_streamed_comparison_equal(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Budget forced below one chunk-row pair so the slab loop genuinely
    # iterates at fixture scale.
    monkeypatch.setattr(validation, "_COMPARE_SLAB_BYTES", 16)
    values = np.arange(16, dtype=np.float32).reshape(8, 2)
    a = _make_store(tmp_path / "a.zarr", shape=(8, 2), chunks=(2, 2), values=values)
    b = _make_store(tmp_path / "b.zarr", shape=(8, 2), chunks=(2, 2), values=values.copy())

    report = _compare(a, b)

    assert report.equivalent is True
    assert report.mismatches == []


def test_mismatch_in_final_slab_detected(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    # The regression a compare-first-slab-and-return implementation would pass.
    monkeypatch.setattr(validation, "_COMPARE_SLAB_BYTES", 16)
    values = np.arange(16, dtype=np.float32).reshape(8, 2)
    changed = values.copy()
    changed[7, 1] = -1.0
    a = _make_store(tmp_path / "a.zarr", shape=(8, 2), chunks=(2, 2), values=values)
    b = _make_store(tmp_path / "b.zarr", shape=(8, 2), chunks=(2, 2), values=changed)

    report = _compare(a, b)

    assert report.equivalent is False
    assert any("values differ" in mismatch for mismatch in report.mismatches)


def test_nat_values_at_same_positions_equal(tmp_path: Path) -> None:
    # A dense time coordinate stores explicit NaT for unfilled slots; two
    # identical partially-filled cubes must compare equal.
    values = np.array(
        ["2024-01-01T00:00", "2024-01-01T00:10", "NaT", "NaT"],
        dtype="datetime64[ns]",
    ).reshape(2, 2)
    a = _make_store(tmp_path / "a.zarr", dtype="datetime64[ns]", values=values)
    b = _make_store(tmp_path / "b.zarr", dtype="datetime64[ns]", values=values.copy())

    report = _compare(a, b)

    assert report.equivalent is True
    assert report.mismatches == []


def test_sub_row_slabs_when_one_chunk_row_overruns_budget(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # One chunk along axis 0 (16 bytes) exceeds the budget (8 bytes), so the
    # comparison must split along axis 1 as well — and still detect a
    # difference confined to the very last sub-row block.
    monkeypatch.setattr(validation, "_COMPARE_SLAB_BYTES", 8)
    values = np.arange(8, dtype=np.float32).reshape(2, 4)
    a = _make_store(tmp_path / "a.zarr", shape=(2, 4), chunks=(1, 2), values=values)
    b = _make_store(tmp_path / "b.zarr", shape=(2, 4), chunks=(1, 2), values=values.copy())

    assert _compare(a, b).equivalent is True

    changed = values.copy()
    changed[1, 3] = -1.0
    c = _make_store(tmp_path / "c.zarr", shape=(2, 4), chunks=(1, 2), values=changed)

    report = _compare(a, c)
    assert report.equivalent is False
    assert any("values differ" in mismatch for mismatch in report.mismatches)


def test_slab_starts_align_with_chunks(monkeypatch: pytest.MonkeyPatch) -> None:
    # Sub-chunk stepping decompresses each chunk once per row; every slab
    # slice must start on a chunk boundary on its axis, and the slabs must
    # tile the array exactly once. The last two geometries force the
    # single-chunk-overruns-budget recursion into the trailing axes.
    monkeypatch.setattr(validation, "_COMPARE_SLAB_BYTES", 1)
    for shape, chunks, itemsize in [
        ((100, 7), (9, 7), 8),
        ((5,), (2,), 4),
        ((64, 64, 3), (16, 32, 3), 2),
        ((4, 10, 6), (1, 3, 6), 8),
        ((3, 5, 4, 2), (1, 2, 3, 2), 8),
    ]:
        slabs = list(validation._chunk_aligned_slabs(shape, chunks, itemsize))
        assert len(slabs) > 1
        covered = np.zeros(shape, dtype=bool)
        for slab in slabs:
            for axis, index in enumerate(slab):
                assert index.start % chunks[axis] == 0
            assert not covered[slab].any()
            covered[slab] = True
        assert covered.all()


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
