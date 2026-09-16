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

"""Tests for :mod:`firecube.core.zarr._drift` helpers."""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pytest
import zarr

from firecube.core.zarr._drift import (
    GroupAttrsDiff,
    arrays_equal_missing_aware,
    chunk_by_chunk_equal,
    group_attrs_diff,
    touched_chunks_for_slice,
)

# ── arrays_equal_missing_aware ────────────────────────────────────────────────


@pytest.mark.unit
def test_identical_float64_equal() -> None:
    a = np.array([1.0, 2.0, 3.0])
    assert arrays_equal_missing_aware(a, a.copy())


@pytest.mark.unit
def test_nan_same_positions_equal() -> None:
    a = np.array([1.0, float("nan"), 3.0])
    b = np.array([1.0, float("nan"), 3.0])
    assert arrays_equal_missing_aware(a, b)


@pytest.mark.unit
def test_nan_different_positions_not_equal() -> None:
    a = np.array([1.0, float("nan"), 3.0])
    b = np.array([1.0, 2.0, float("nan")])
    assert not arrays_equal_missing_aware(a, b)


@pytest.mark.unit
def test_float32_vs_float64_same_values_equal() -> None:
    """Mirrors xarray compat='equals' dtype-tolerance."""
    a = np.array([1.0, 2.0], dtype=np.float32)
    b = np.array([1.0, 2.0], dtype=np.float64)
    assert arrays_equal_missing_aware(a, b)


@pytest.mark.unit
def test_float32_vs_bytes_not_equal() -> None:
    a = np.array([1.0], dtype=np.float32)
    b = np.array([b"\x00"], dtype="S1")
    assert not arrays_equal_missing_aware(a, b)


@pytest.mark.unit
def test_shape_mismatch_not_equal() -> None:
    a = np.array([1.0, 2.0])
    b = np.array([1.0])
    assert not arrays_equal_missing_aware(a, b)


@pytest.mark.unit
def test_shape_mismatch_multidim_not_equal() -> None:
    a = np.zeros((3, 4))
    b = np.zeros((3, 5))
    assert not arrays_equal_missing_aware(a, b)


@pytest.mark.unit
def test_nat_same_positions_equal() -> None:
    a = np.array(["NaT", "2020-01-01"], dtype="datetime64[ns]")
    b = np.array(["NaT", "2020-01-01"], dtype="datetime64[ns]")
    assert arrays_equal_missing_aware(a, b)


@pytest.mark.unit
def test_nat_different_positions_not_equal() -> None:
    a = np.array(["NaT", "2020-01-01"], dtype="datetime64[ns]")
    b = np.array(["2020-01-01", "NaT"], dtype="datetime64[ns]")
    assert not arrays_equal_missing_aware(a, b)


@pytest.mark.unit
def test_string_arrays_equal() -> None:
    a = np.array(["foo", "bar"])
    b = np.array(["foo", "bar"])
    assert arrays_equal_missing_aware(a, b)


@pytest.mark.unit
def test_string_arrays_different_not_equal() -> None:
    a = np.array(["foo", "bar"])
    b = np.array(["foo", "baz"])
    assert not arrays_equal_missing_aware(a, b)


@pytest.mark.unit
def test_int_vs_float_same_values_equal() -> None:
    """Int and float promote to a common float dtype; values compare equal."""
    a = np.array([1, 2, 3], dtype=np.int32)
    b = np.array([1.0, 2.0, 3.0], dtype=np.float64)
    assert arrays_equal_missing_aware(a, b)


@pytest.mark.unit
def test_int_arrays_equal() -> None:
    a = np.array([1, 2, 3], dtype=np.int64)
    b = np.array([1, 2, 3], dtype=np.int64)
    assert arrays_equal_missing_aware(a, b)


@pytest.mark.unit
def test_int_arrays_different_not_equal() -> None:
    a = np.array([1, 2, 3], dtype=np.int64)
    b = np.array([1, 2, 4], dtype=np.int64)
    assert not arrays_equal_missing_aware(a, b)


@pytest.mark.unit
def test_multidim_float_equal() -> None:
    a = np.arange(12.0).reshape(3, 4)
    b = np.arange(12.0).reshape(3, 4)
    assert arrays_equal_missing_aware(a, b)


@pytest.mark.unit
def test_empty_arrays_equal() -> None:
    a = np.array([], dtype=np.float64)
    b = np.array([], dtype=np.float32)
    assert arrays_equal_missing_aware(a, b)


# ── group_attrs_diff ──────────────────────────────────────────────────────────


@pytest.mark.unit
def test_identical_attrs_is_empty() -> None:
    d = group_attrs_diff({"title": "A", "x": 1}, {"title": "A", "x": 1})
    assert d.is_empty
    assert isinstance(d, GroupAttrsDiff)


@pytest.mark.unit
def test_empty_dicts_is_empty() -> None:
    d = group_attrs_diff({}, {})
    assert d.is_empty
    assert d.added == ()
    assert d.removed == ()
    assert d.changed == ()


@pytest.mark.unit
def test_added_key_detected() -> None:
    d = group_attrs_diff({"title": "A"}, {"title": "A", "history": "new"})
    assert d.added == ("history",)
    assert d.removed == ()
    assert d.changed == ()


@pytest.mark.unit
def test_removed_key_detected() -> None:
    d = group_attrs_diff({"title": "A", "old": 1}, {"title": "A"})
    assert d.removed == ("old",)
    assert d.added == ()
    assert d.changed == ()


@pytest.mark.unit
def test_changed_value_detected() -> None:
    d = group_attrs_diff({"title": "Run 1"}, {"title": "Run 2"})
    assert d.changed == ("title",)


@pytest.mark.unit
def test_tuple_vs_list_same_values_is_not_a_diff() -> None:
    d = group_attrs_diff({"k": [1, 2, 3]}, {"k": (1, 2, 3)})
    assert d.is_empty


@pytest.mark.unit
def test_nested_tuple_of_lists_is_not_a_diff() -> None:
    stored = {"k": {"outer": [[1, 2], [3, 4]], "flag": True}}
    incoming = {"k": {"outer": ([1, 2], (3, 4)), "flag": True}}
    d = group_attrs_diff(stored, incoming)
    assert d.is_empty


@pytest.mark.unit
def test_actual_value_change_is_still_flagged() -> None:
    d = group_attrs_diff({"k": [1, 2]}, {"k": [1, 3]})
    assert d.changed == ("k",)


@pytest.mark.unit
def test_mixed_diff() -> None:
    stored = {"title": "Run 1", "Conventions": "CF-1.8"}
    incoming = {"title": "Run 2", "Conventions": "CF-1.8", "history": "appended"}
    d = group_attrs_diff(stored, incoming)
    assert "title" in d.changed
    assert "history" in d.added
    assert d.removed == ()


@pytest.mark.unit
def test_ndarray_attr_equal_not_changed() -> None:
    stored = {"bounds": np.array([1.0, 2.0])}
    incoming = {"bounds": np.array([1.0, 2.0])}
    d = group_attrs_diff(stored, incoming)
    assert d.is_empty


@pytest.mark.unit
def test_ndarray_attr_dtype_promoted_equal() -> None:
    stored = {"bounds": np.array([1.0, 2.0], dtype=np.float32)}
    incoming = {"bounds": np.array([1.0, 2.0], dtype=np.float64)}
    d = group_attrs_diff(stored, incoming)
    assert d.is_empty


@pytest.mark.unit
def test_ndarray_attr_diff_reported_changed() -> None:
    stored = {"bounds": np.array([1.0, 2.0])}
    incoming = {"bounds": np.array([1.0, 3.0])}
    d = group_attrs_diff(stored, incoming)
    assert d.changed == ("bounds",)


@pytest.mark.unit
def test_no_volatile_attr_filter_baked_in() -> None:
    """Engine is generic first-write-wins; no key is silently ignored."""
    stored = {"history": "run-1", "firecube_run_id": "aaa"}
    incoming = {"history": "run-2", "firecube_run_id": "bbb"}
    d = group_attrs_diff(stored, incoming)
    assert set(d.changed) == {"history", "firecube_run_id"}


@pytest.mark.unit
def test_added_keys_sorted() -> None:
    d = group_attrs_diff({}, {"z": 1, "a": 2, "m": 3})
    assert d.added == ("a", "m", "z")


# ── touched_chunks_for_slice ──────────────────────────────────────────────────


@pytest.mark.unit
def test_touched_chunks_single_chunk() -> None:
    chunks = touched_chunks_for_slice(array_shape=(10,), chunk_shape=(5,), region={0: slice(0, 3)})
    assert chunks == [(0,)]


@pytest.mark.unit
def test_touched_chunks_spanning_two() -> None:
    chunks = touched_chunks_for_slice(array_shape=(10,), chunk_shape=(2,), region={0: slice(1, 4)})
    assert set(chunks) == {(0,), (1,)}


@pytest.mark.unit
def test_touched_chunks_all_dims_when_not_in_region() -> None:
    chunks = touched_chunks_for_slice(
        array_shape=(4, 6), chunk_shape=(2, 3), region={0: slice(0, 2)}
    )
    assert set(chunks) == {(0, 0), (0, 1)}


@pytest.mark.unit
def test_touched_chunks_unaligned_boundary() -> None:
    chunks = touched_chunks_for_slice(array_shape=(7,), chunk_shape=(3,), region={0: slice(4, 7)})
    assert set(chunks) == {(1,), (2,)}


@pytest.mark.unit
def test_touched_chunks_no_region_returns_full_grid() -> None:
    chunks = touched_chunks_for_slice(array_shape=(4, 6), chunk_shape=(2, 3), region={})
    assert set(chunks) == {(0, 0), (0, 1), (1, 0), (1, 1)}


@pytest.mark.unit
def test_touched_chunks_slice_none_start_treated_as_zero() -> None:
    chunks = touched_chunks_for_slice(
        array_shape=(10,), chunk_shape=(5,), region={0: slice(None, 3)}
    )
    assert chunks == [(0,)]


@pytest.mark.unit
def test_touched_chunks_slice_none_stop_treated_as_shape() -> None:
    chunks = touched_chunks_for_slice(
        array_shape=(10,), chunk_shape=(5,), region={0: slice(5, None)}
    )
    assert chunks == [(1,)]


@pytest.mark.unit
def test_touched_chunks_full_region_covers_all() -> None:
    chunks = touched_chunks_for_slice(array_shape=(10,), chunk_shape=(2,), region={0: slice(0, 10)})
    assert set(chunks) == {(0,), (1,), (2,), (3,), (4,)}


@pytest.mark.unit
def test_touched_chunks_zero_chunk_size_raises() -> None:
    with pytest.raises(ValueError, match="must be positive"):
        touched_chunks_for_slice(array_shape=(10,), chunk_shape=(0,), region={0: slice(0, 3)})


# ── chunk_by_chunk_equal ──────────────────────────────────────────────────────


@pytest.mark.unit
def test_chunk_by_chunk_identical_equal(tmp_path: Path) -> None:
    a = zarr.create_array(f"{tmp_path}/a.zarr", shape=(20,), dtype="f8", chunks=(5,))
    b = zarr.create_array(f"{tmp_path}/b.zarr", shape=(20,), dtype="f8", chunks=(5,))
    vals = np.arange(20.0)
    a[:] = vals
    b[:] = vals
    assert chunk_by_chunk_equal(a, b)


@pytest.mark.unit
def test_chunk_by_chunk_mismatch_detected(tmp_path: Path) -> None:
    a = zarr.create_array(f"{tmp_path}/a.zarr", shape=(20,), dtype="f8", chunks=(5,))
    b = zarr.create_array(f"{tmp_path}/b.zarr", shape=(20,), dtype="f8", chunks=(5,))
    a[:] = np.arange(20.0)
    b[:] = np.arange(20.0)
    b[15:] = 999.0
    assert not chunk_by_chunk_equal(a, b)


@pytest.mark.unit
def test_chunk_by_chunk_ndarray_incoming_equal(tmp_path: Path) -> None:
    a = zarr.create_array(f"{tmp_path}/a.zarr", shape=(20,), dtype="f8", chunks=(5,))
    vals = np.arange(20.0)
    a[:] = vals
    assert chunk_by_chunk_equal(a, vals)


@pytest.mark.unit
def test_chunk_by_chunk_ndarray_incoming_mismatch(tmp_path: Path) -> None:
    a = zarr.create_array(f"{tmp_path}/a.zarr", shape=(20,), dtype="f8", chunks=(5,))
    a[:] = np.arange(20.0)
    diverged = np.arange(20.0)
    diverged[7] = -1.0
    assert not chunk_by_chunk_equal(a, diverged)


@pytest.mark.unit
def test_chunk_by_chunk_shape_mismatch_not_equal(tmp_path: Path) -> None:
    a = zarr.create_array(f"{tmp_path}/a.zarr", shape=(10,), dtype="f8", chunks=(5,))
    a[:] = np.arange(10.0)
    assert not chunk_by_chunk_equal(a, np.arange(8.0))


@pytest.mark.unit
def test_chunk_by_chunk_multidim_equal(tmp_path: Path) -> None:
    a = zarr.create_array(f"{tmp_path}/a.zarr", shape=(6, 8), dtype="f8", chunks=(3, 4))
    b = zarr.create_array(f"{tmp_path}/b.zarr", shape=(6, 8), dtype="f8", chunks=(3, 4))
    vals = np.arange(48.0).reshape(6, 8)
    a[:] = vals
    b[:] = vals
    assert chunk_by_chunk_equal(a, b)


@pytest.mark.unit
def test_chunk_by_chunk_multidim_last_chunk_mismatch(tmp_path: Path) -> None:
    a = zarr.create_array(f"{tmp_path}/a.zarr", shape=(6, 8), dtype="f8", chunks=(3, 4))
    b = zarr.create_array(f"{tmp_path}/b.zarr", shape=(6, 8), dtype="f8", chunks=(3, 4))
    vals = np.arange(48.0).reshape(6, 8)
    a[:] = vals
    b[:] = vals
    b[5, 7] = -99.0
    assert not chunk_by_chunk_equal(a, b)


@pytest.mark.unit
def test_chunk_by_chunk_sharded_equal(tmp_path: Path) -> None:
    a = zarr.create_array(
        f"{tmp_path}/a.zarr",
        shape=(100,),
        dtype="f8",
        chunks=(10,),
        shards=(50,),
    )
    b = zarr.create_array(
        f"{tmp_path}/b.zarr",
        shape=(100,),
        dtype="f8",
        chunks=(10,),
        shards=(50,),
    )
    vals = np.arange(100.0)
    a[:] = vals
    b[:] = vals
    assert chunk_by_chunk_equal(a, b)


@pytest.mark.unit
def test_chunk_by_chunk_sharded_mismatch_detected(tmp_path: Path) -> None:
    a = zarr.create_array(
        f"{tmp_path}/a.zarr",
        shape=(100,),
        dtype="f8",
        chunks=(10,),
        shards=(50,),
    )
    b = zarr.create_array(
        f"{tmp_path}/b.zarr",
        shape=(100,),
        dtype="f8",
        chunks=(10,),
        shards=(50,),
    )
    a[:] = np.arange(100.0)
    b[:] = np.arange(100.0)
    b[75] = -1.0
    assert not chunk_by_chunk_equal(a, b)


@pytest.mark.unit
def test_chunk_by_chunk_nan_positions_equal(tmp_path: Path) -> None:
    a = zarr.create_array(f"{tmp_path}/a.zarr", shape=(10,), dtype="f8", chunks=(4,))
    b = zarr.create_array(f"{tmp_path}/b.zarr", shape=(10,), dtype="f8", chunks=(4,))
    vals = np.arange(10.0)
    vals[3] = np.nan
    vals[7] = np.nan
    a[:] = vals
    b[:] = vals
    assert chunk_by_chunk_equal(a, b)


@pytest.mark.unit
def test_chunk_by_chunk_unaligned_last_chunk_equal(tmp_path: Path) -> None:
    a = zarr.create_array(f"{tmp_path}/a.zarr", shape=(7,), dtype="f8", chunks=(3,))
    b = zarr.create_array(f"{tmp_path}/b.zarr", shape=(7,), dtype="f8", chunks=(3,))
    vals = np.arange(7.0)
    a[:] = vals
    b[:] = vals
    assert chunk_by_chunk_equal(a, b)


@pytest.mark.unit
def test_chunk_by_chunk_chunk_shape_override_used(tmp_path: Path) -> None:
    a = zarr.create_array(f"{tmp_path}/a.zarr", shape=(20,), dtype="f8", chunks=(5,))
    b = zarr.create_array(f"{tmp_path}/b.zarr", shape=(20,), dtype="f8", chunks=(5,))
    vals = np.arange(20.0)
    a[:] = vals
    b[:] = vals
    assert chunk_by_chunk_equal(a, b, chunk_shape=(4,))
