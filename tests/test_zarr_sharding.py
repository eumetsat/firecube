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

"""Tests for zarr v3 sharding support in firecube write/read pipeline."""

import numpy as np
import pandas as pd
import pytest
import xarray as xr
import zarr

from firecube.core.config import StorageConfig
from firecube.core.filesystem.store_factory import create_zarr_store
from firecube.core.zarr.validation import read_chunk_grid, read_chunk_grid_with_shards
from firecube.ingestor.runtime.zarr.write import _build_zarr_encoding, write_dataset_to_zarr
from firecube.ingestor.templates.config import ZarrTemplateConfig
from tests.helpers.storage import make_local_session


def _local_handle(path: str, mode: str = "w"):
    return create_zarr_store(
        uri=path,
        storage_config=StorageConfig(storage_type="local"),
        mode=mode,
    )


def make_dataset(nt=2, ny=20, nx=20, dtype="float32"):
    return xr.Dataset(
        {"var1": (["timestamp", "ny", "nx"], np.random.rand(nt, ny, nx).astype(dtype))},
        coords={"timestamp": pd.date_range("2023-12-01", periods=nt, freq="5min")},
    )


def test_encoding_without_sharding_unchanged():
    ds = make_dataset()
    enc = _build_zarr_encoding(ds, compression=True)
    assert "var1" in enc
    assert "compressors" in enc["var1"]
    assert "shards" not in enc["var1"]
    assert "chunks" not in enc["var1"]


def test_encoding_with_sharding_includes_shards_and_chunks():
    ds = make_dataset(nt=2, ny=20, nx=20)
    shard_shape = {"timestamp": 2, "ny": 20, "nx": 20}
    chunk_shape = {"timestamp": 1, "ny": 10, "nx": 10}
    enc = _build_zarr_encoding(
        ds,
        compression=True,
        shard_shape=shard_shape,
        chunk_shape=chunk_shape,
    )
    assert "var1" in enc
    assert "compressors" in enc["var1"]
    assert "shards" in enc["var1"]
    assert "chunks" in enc["var1"]
    assert enc["var1"]["shards"] == (2, 20, 20)
    assert enc["var1"]["chunks"] == (1, 10, 10)


def test_write_sharded_zarr_creates_correct_store(tmp_path):
    ds = make_dataset(nt=2, ny=20, nx=20)
    store = str(tmp_path / "test.zarr")
    write_dataset_to_zarr(
        ds,
        zarr_store=_local_handle(store),
        group="TEST",
        shard_shape={"timestamp": 2, "ny": 20, "nx": 20},
        chunk_shape={"timestamp": 1, "ny": 10, "nx": 10},
        compression=True,
    )
    _dim_names, shape, outer_chunks, inner_chunks = read_chunk_grid_with_shards(
        store,
        "TEST/var1",
    )
    assert shape == [2, 20, 20]
    assert outer_chunks == [2, 20, 20]
    assert inner_chunks is not None
    assert inner_chunks == [1, 10, 10]


def test_write_without_sharding_backward_compat(tmp_path):
    ds = make_dataset(nt=2, ny=20, nx=20)
    store = str(tmp_path / "test.zarr")
    write_dataset_to_zarr(
        ds,
        zarr_store=_local_handle(store),
        group="TEST",
        chunk_shape={"timestamp": 1, "ny": 10, "nx": 10},
        compression=True,
    )
    _dim_names, shape, outer_chunks, inner_chunks = read_chunk_grid_with_shards(
        store,
        "TEST/var1",
    )
    assert shape == [2, 20, 20]
    assert outer_chunks == [1, 10, 10]
    assert inner_chunks is None


def test_append_to_sharded_store_preserves_shards(tmp_path):
    ds1 = make_dataset(nt=1, ny=20, nx=20)
    ds2 = make_dataset(nt=1, ny=20, nx=20)
    ds2.coords["timestamp"] = pd.date_range("2023-12-01 00:05", periods=1, freq="5min")
    store = str(tmp_path / "test.zarr")
    shard_shape = {"timestamp": 2, "ny": 20, "nx": 20}
    chunk_shape = {"timestamp": 1, "ny": 10, "nx": 10}

    write_dataset_to_zarr(
        ds1,
        zarr_store=_local_handle(store),
        group="TEST",
        shard_shape=shard_shape,
        chunk_shape=chunk_shape,
        compression=True,
    )
    write_dataset_to_zarr(
        ds2,
        zarr_store=_local_handle(store, mode="a"),
        group="TEST",
        mode="a",
        append_dim="timestamp",
        compression=True,
    )
    ds_read = xr.open_zarr(store, group="TEST", consolidated=False)
    assert ds_read.sizes["timestamp"] == 2
    arr = zarr.open_array(store, path="TEST/var1", mode="r")
    assert arr.metadata.shards is not None, "Shard metadata lost after append"
    expected_shards = (2, 20, 20)
    assert arr.shards == expected_shards, f"Shard shape changed: {arr.shards} != {expected_shards}"
    expected_chunks = (1, 10, 10)
    assert arr.chunks == expected_chunks, f"Chunk shape changed: {arr.chunks} != {expected_chunks}"


def test_xarray_reads_sharded_store_transparently(tmp_path):
    ds = make_dataset(nt=2, ny=20, nx=20)
    store = str(tmp_path / "test.zarr")
    write_dataset_to_zarr(
        ds,
        zarr_store=_local_handle(store),
        group="TEST",
        shard_shape={"timestamp": 2, "ny": 20, "nx": 20},
        chunk_shape={"timestamp": 1, "ny": 10, "nx": 10},
        compression=True,
    )
    ds_read = xr.open_zarr(store, group="TEST", consolidated=False)
    assert "var1" in ds_read.data_vars
    assert ds_read.sizes["timestamp"] == 2
    val = float(ds_read["var1"].isel(timestamp=0, ny=0, nx=0).values)
    assert not np.isnan(val)


def test_config_default_shard_shape_is_none():
    cfg = ZarrTemplateConfig()
    assert cfg.zarr_sharding is False
    assert cfg.zarr_shard_shape is None


def test_read_chunk_grid_unchanged(tmp_path):
    ds = make_dataset()
    store = str(tmp_path / "test.zarr")
    write_dataset_to_zarr(
        ds,
        zarr_store=_local_handle(store),
        group="GRPX",
        chunk_shape={"timestamp": 1, "ny": 10, "nx": 10},
    )
    result = read_chunk_grid(store, "GRPX/var1")
    assert len(result) == 3, f"Expected 3-tuple, got {len(result)}"
    _dim_names, shape, _chunk_shape_out = result
    assert shape == [2, 20, 20]


def test_read_chunk_grid_with_shards_returns_4_tuple(tmp_path):
    ds = make_dataset(nt=2, ny=20, nx=20)
    store = str(tmp_path / "test.zarr")
    write_dataset_to_zarr(
        ds,
        zarr_store=_local_handle(store),
        group="GRPX",
        shard_shape={"timestamp": 2, "ny": 20, "nx": 20},
        chunk_shape={"timestamp": 1, "ny": 10, "nx": 10},
        compression=True,
    )
    result = read_chunk_grid_with_shards(store, "GRPX/var1")
    assert len(result) == 4, f"Expected 4-tuple, got {len(result)}"
    _dim_names, shape, outer_chunks, inner_chunks = result
    assert shape == [2, 20, 20]
    assert outer_chunks == [2, 20, 20]
    assert inner_chunks is not None
    assert inner_chunks == [1, 10, 10]


# --- Regression: cold-cache resume with sharding=True ---
def test_cold_resume_sharding_true_does_not_reject(tmp_path):
    """Cold-cache resume with sharding=True on sharded store must not falsely reject."""
    from firecube.ingestor.runtime.zarr.append import append_time_groups

    ds1 = make_dataset(nt=1, ny=20, nx=20)
    store = str(tmp_path / "test.zarr")
    write_dataset_to_zarr(
        ds1,
        zarr_store=_local_handle(store),
        group="TEST",
        sharding=True,
        chunk_shape={"timestamp": 1, "ny": 5, "nx": 5},
        compression=True,
    )
    ds2 = make_dataset(nt=1, ny=20, nx=20)
    ds2.coords["timestamp"] = pd.date_range("2023-12-01 00:05", periods=1, freq="5min")
    append_time_groups(
        store=store,
        zarr_store=_local_handle(store, mode="a"),
        session=make_local_session(store),
        group_to_timestamps={"TEST": [pd.Timestamp("2023-12-01 00:05")]},
        dataset_for_batch=lambda g, items: ds2,
        sharding=True,
        chunk_shape={"timestamp": 1, "ny": 5, "nx": 5},
    )
    arr = zarr.open_array(store + "/TEST/var1", mode="r")
    assert arr.shape[0] == 2


# --- Regression: cache-hit mismatched shard raises ---
def test_cache_hit_mismatched_shard_raises(tmp_path):
    """Cache-hit with wrong shard_shape must raise ValueError."""
    from firecube.ingestor.runtime.zarr.append import append_time_groups

    store = str(tmp_path / "test.zarr")
    shard = {"timestamp": 1, "ny": 20, "nx": 20}
    chunk = {"timestamp": 1, "ny": 5, "nx": 5}
    ds = make_dataset(nt=1, ny=20, nx=20)
    append_time_groups(
        store=store,
        zarr_store=_local_handle(store, mode="a"),
        session=make_local_session(store),
        group_to_timestamps={"TEST": [pd.Timestamp("2023-12-01")]},
        dataset_for_batch=lambda g, items: ds,
        shard_shape=shard,
        chunk_shape=chunk,
    )
    ds2 = make_dataset(nt=1, ny=20, nx=20)
    ds2.coords["timestamp"] = pd.date_range("2023-12-01 00:05", periods=1, freq="5min")
    with pytest.raises(ValueError, match="shard_shape"):
        append_time_groups(
            store=store,
            zarr_store=_local_handle(store, mode="a"),
            session=make_local_session(store),
            group_to_timestamps={"TEST": [pd.Timestamp("2023-12-01 00:05")]},
            dataset_for_batch=lambda g, items: ds2,
            shard_shape={"timestamp": 1, "ny": 10, "nx": 10},
            chunk_shape={"timestamp": 1, "ny": 2, "nx": 2},
        )


# --- Regression: from_options rejects invalid bool ---
def test_from_options_rejects_invalid_bool():
    """from_options must reject non-boolean strings like 'maybe'."""
    with pytest.raises(ValueError, match="Invalid boolean"):
        ZarrTemplateConfig.from_options({"zarr_sharding": "maybe"})


# --- Regression: from_options rejects invalid JSON ---
def test_from_options_rejects_invalid_json():
    """from_options must reject non-JSON strings for dict fields."""
    with pytest.raises(ValueError, match="Invalid JSON"):
        ZarrTemplateConfig.from_options({"zarr_shard_shape": "not-json"})


# --- Regression: Flaw 11 — _auto_inner_chunk_size NameError ---
def test_write_dataset_explicit_shard_no_chunk_shape_uses_auto_inner(tmp_path):
    """Explicit shard_shape without chunk_shape derives persisted inner chunks."""
    ds = xr.Dataset(
        {
            "var1": xr.DataArray(
                np.zeros((2, 100, 80), dtype="float32"), dims=["timestamp", "ny", "nx"]
            )
        }
    )
    store = str(tmp_path / "test_auto_inner.zarr")
    write_dataset_to_zarr(
        ds,
        zarr_store=_local_handle(store),
        group="G",
        shard_shape={"timestamp": 1, "ny": 100, "nx": 80},
        chunk_shape=None,
    )

    _dim_names, shape, outer_chunks, inner_chunks = read_chunk_grid_with_shards(store, "G/var1")
    assert shape == [2, 100, 80]
    assert outer_chunks == [1, 100, 80]
    assert inner_chunks == [1, 25, 20]

    arr = zarr.open_array(store, path="G/var1", mode="r")
    assert arr.shards == (1, 100, 80)
    assert arr.chunks == (1, 25, 20)


# --- Regression: Flaw 10 — unconditional rechunk ---
def test_write_dataset_skips_rechunk_when_chunks_already_match(tmp_path):
    """Write uses matching source chunks directly instead of executing rechunk tasks."""
    import dask.array as da
    from dask.callbacks import Callback

    data = da.from_array(np.zeros((4, 40, 40), dtype="float32"), chunks=(1, 40, 40))  # pyright: ignore[reportArgumentType]
    ds = xr.Dataset({"var1": xr.DataArray(data, dims=["timestamp", "ny", "nx"])})
    store = str(tmp_path / "test_skip_rechunk.zarr")

    executed_keys: list[str] = []

    with Callback(pretask=lambda key, _dsk, _state: executed_keys.append(str(key))):
        write_dataset_to_zarr(
            ds,
            zarr_store=_local_handle(store),
            group="G",
            shard_shape={"timestamp": 1, "ny": 40, "nx": 40},
        )

    assert not any("rechunk" in key.lower() for key in executed_keys)
    _dim_names, shape, outer_chunks, inner_chunks = read_chunk_grid_with_shards(store, "G/var1")
    assert shape == [4, 40, 40]
    assert outer_chunks == [1, 40, 40]
    assert inner_chunks == [1, 10, 10]
