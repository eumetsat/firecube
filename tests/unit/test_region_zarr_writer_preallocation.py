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
from typing import Any, cast

import numpy as np
import pytest
import zarr

from firecube.core.zarr.region_writer import RegionZarrWriter
from firecube.ingestor.runtime.zarr.strategies.indexed_region import IndexedRegionStrategy
from firecube.ingestor.templates.direct_zarr import WriteIntent, ZarrArraySpec, ZarrGroupSpec


@pytest.fixture()
def store_path(tmp_path: Path) -> str:
    store_dir = tmp_path / "preallocated.zarr"
    zarr.open_group(store=str(store_dir), mode="w", zarr_format=3)
    return str(store_dir)


def test_preallocate_sets_time_dimension(store_path: str) -> None:
    schema = [
        ZarrGroupSpec(
            group="grp",
            arrays=[
                ZarrArraySpec(
                    name="data",
                    shape=(1, 4, 5),
                    dtype=np.float32,
                    chunks=(1, 4, 5),
                    expected_time_count=3,
                )
            ],
        )
    ]
    IndexedRegionStrategy(store_uri=store_path, schema=schema).write_groups(
        group_to_intents={"grp": []}
    )

    arr = cast(Any, zarr.open_group(store=store_path, mode="r", zarr_format=3)["grp/data"])
    assert arr.shape == (3, 4, 5)


def test_preallocate_skips_resize_on_write(
    store_path: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    schema = [
        ZarrGroupSpec(
            group="grp",
            arrays=[
                ZarrArraySpec(
                    name="data",
                    shape=(1, 4, 5),
                    dtype=np.float32,
                    chunks=(1, 4, 5),
                    expected_time_count=3,
                )
            ],
        )
    ]
    IndexedRegionStrategy(store_uri=store_path, schema=schema).write_groups(
        group_to_intents={"grp": []}
    )

    writer = RegionZarrWriter(store_path)
    arr = writer._open_root()["grp/data"]
    resize_calls: list[tuple[int, ...]] = []
    original_resize = type(arr).resize

    def spy_resize(self: object, shape: tuple[int, ...]) -> object:
        resize_calls.append(shape)
        return original_resize(self, shape)

    monkeypatch.setattr(type(arr), "resize", spy_resize)

    for ts_index in range(3):
        writer.ensure_timestamp_slot("grp", ts_index)

    assert resize_calls == []


def test_manual_array_without_capacity_raises(
    store_path: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    writer = RegionZarrWriter(store_path)
    arr = writer.ensure_group("grp/data", shape=(1, 4, 5), dtype=np.float32, chunks=(1, 4, 5))
    resize_calls: list[tuple[int, ...]] = []
    original_resize = type(arr).resize

    def spy_resize(self: object, shape: tuple[int, ...]) -> object:
        resize_calls.append(shape)
        return original_resize(self, shape)

    monkeypatch.setattr(type(arr), "resize", spy_resize)

    with pytest.raises(ValueError, match="Preallocate timestamped arrays"):
        writer.ensure_timestamp_slot("grp", ts_index=1)

    assert resize_calls == []
    assert arr.shape == (1, 4, 5)


def test_negative_expected_time_count_raises() -> None:
    with pytest.raises(ValueError, match="expected_time_count must be non-negative"):
        ZarrArraySpec(
            name="data",
            shape=(1, 4, 5),
            dtype=np.float32,
            chunks=(1, 4, 5),
            expected_time_count=-1,
        )


def test_zero_expected_time_count_allocates_zero_and_requires_valid_slot(
    store_path: str,
) -> None:
    schema = [
        ZarrGroupSpec(
            group="grp",
            arrays=[
                ZarrArraySpec(
                    name="data",
                    shape=(1, 4, 5),
                    dtype=np.float32,
                    chunks=(1, 4, 5),
                    expected_time_count=0,
                )
            ],
        )
    ]
    IndexedRegionStrategy(store_uri=store_path, schema=schema).write_groups(
        group_to_intents={"grp": []}
    )
    writer = RegionZarrWriter(store_path)

    with pytest.raises(ValueError, match="Preallocate timestamped arrays"):
        writer.ensure_timestamp_slot("grp", ts_index=0)

    assert writer._open_root()["grp/data"].shape == (0, 4, 5)


def test_too_small_array_raises_instead_of_growing(store_path: str) -> None:
    writer = RegionZarrWriter(store_path)
    writer.ensure_group("grp/data", shape=(2, 4, 5), dtype=np.float32, chunks=(1, 4, 5))

    with pytest.raises(ValueError, match="Preallocate timestamped arrays"):
        writer.ensure_timestamp_slot("grp", ts_index=5)

    assert writer._open_root()["grp/data"].shape == (2, 4, 5)


def test_existing_array_unchanged_by_ensure(store_path: str) -> None:
    writer = RegionZarrWriter(store_path)
    writer.ensure_group("grp/data", shape=(2, 4, 5), dtype=np.float32, chunks=(1, 4, 5))
    schema = [
        ZarrGroupSpec(
            group="grp",
            arrays=[
                ZarrArraySpec(
                    name="data",
                    shape=(1, 4, 5),
                    dtype=np.float32,
                    chunks=(1, 4, 5),
                    expected_time_count=5,
                )
            ],
        )
    ]

    IndexedRegionStrategy(store_uri=store_path, schema=schema).write_groups(
        group_to_intents={"grp": []}
    )
    assert writer._open_root()["grp/data"].shape == (2, 4, 5)
    with pytest.raises(ValueError, match="Preallocate timestamped arrays"):
        writer.ensure_timestamp_slot("grp", ts_index=5)
    assert writer._open_root()["grp/data"].shape == (2, 4, 5)


def test_existing_larger_array_unchanged(store_path: str) -> None:
    writer = RegionZarrWriter(store_path)
    arr = writer.ensure_group("grp/data", shape=(10, 4, 5), dtype=np.float32, chunks=(1, 4, 5))
    schema = [
        ZarrGroupSpec(
            group="grp",
            arrays=[
                ZarrArraySpec(
                    name="data",
                    shape=(1, 4, 5),
                    dtype=np.float32,
                    chunks=(1, 4, 5),
                    expected_time_count=5,
                )
            ],
        )
    ]

    IndexedRegionStrategy(store_uri=store_path, schema=schema).write_groups(
        group_to_intents={"grp": []}
    )
    writer.ensure_timestamp_slot("grp", ts_index=5)

    assert arr.shape == (10, 4, 5)


def test_sparse_storage_no_eager_chunk_writes(store_path: str) -> None:
    schema = [
        ZarrGroupSpec(
            group="grp",
            arrays=[
                ZarrArraySpec(
                    name="data",
                    shape=(1, 4, 5),
                    dtype=np.float32,
                    fill_value=0.0,
                    chunks=(1, 4, 5),
                    expected_time_count=1000,
                )
            ],
        )
    ]
    IndexedRegionStrategy(store_uri=store_path, schema=schema).write_groups(
        group_to_intents={"grp": []}
    )

    files = [path.relative_to(store_path).as_posix() for path in Path(store_path).rglob("*")]
    data_chunk_files = [path for path in files if "/c/" in path or path.startswith("c/")]

    assert data_chunk_files == []
    assert all(path.endswith("zarr.json") or ".zarr/" not in path for path in files)


def test_auto_compute_from_write_intents(store_path: str, monkeypatch: pytest.MonkeyPatch) -> None:
    schema = [
        ZarrGroupSpec(
            group="grp",
            arrays=[
                ZarrArraySpec(
                    name="data",
                    shape=(1, 4, 5),
                    dtype=np.float32,
                    chunks=(1, 4, 5),
                    expected_time_count=None,
                )
            ],
        )
    ]
    intents = [
        WriteIntent(
            group="grp",
            array="data",
            ts_index=ts_index,
            data=np.zeros((4, 5), dtype=np.float32),
            y_slice=slice(0, 4),
        )
        for ts_index in range(5)
    ]
    ensure_group_calls: list[tuple[str, tuple[int, ...] | None]] = []
    original_ensure_group = RegionZarrWriter.ensure_group

    def spy_ensure_group(
        self: RegionZarrWriter,
        group: str,
        shape: tuple[int, ...] | None = None,
        dtype: Any | None = None,
        fill_value: Any | None = None,
        chunks: tuple[int, ...] | None = None,
        **kwargs: Any,
    ) -> Any:
        ensure_group_calls.append((group, shape))
        return original_ensure_group(
            self, group, shape=shape, dtype=dtype, fill_value=fill_value, chunks=chunks, **kwargs
        )

    monkeypatch.setattr(RegionZarrWriter, "ensure_group", spy_ensure_group)

    IndexedRegionStrategy(store_uri=store_path, schema=schema).write_groups(
        group_to_intents={"grp": intents}
    )

    assert ("grp/data", (5, 4, 5)) in ensure_group_calls


def test_plugin_override_wins(store_path: str, monkeypatch: pytest.MonkeyPatch) -> None:
    schema = [
        ZarrGroupSpec(
            group="grp",
            arrays=[
                ZarrArraySpec(
                    name="data",
                    shape=(1, 4, 5),
                    dtype=np.float32,
                    chunks=(1, 4, 5),
                    expected_time_count=100,
                )
            ],
        )
    ]
    intents = [
        WriteIntent(
            group="grp",
            array="data",
            ts_index=ts_index,
            data=np.zeros((4, 5), dtype=np.float32),
            y_slice=slice(0, 4),
        )
        for ts_index in range(5)
    ]
    ensure_group_calls: list[tuple[str, tuple[int, ...] | None]] = []
    original_ensure_group = RegionZarrWriter.ensure_group

    def spy_ensure_group(
        self: RegionZarrWriter,
        group: str,
        shape: tuple[int, ...] | None = None,
        dtype: Any | None = None,
        fill_value: Any | None = None,
        chunks: tuple[int, ...] | None = None,
        **kwargs: Any,
    ) -> Any:
        ensure_group_calls.append((group, shape))
        return original_ensure_group(
            self, group, shape=shape, dtype=dtype, fill_value=fill_value, chunks=chunks, **kwargs
        )

    monkeypatch.setattr(RegionZarrWriter, "ensure_group", spy_ensure_group)

    IndexedRegionStrategy(store_uri=store_path, schema=schema).write_groups(
        group_to_intents={"grp": intents}
    )

    assert ("grp/data", (100, 4, 5)) in ensure_group_calls


def test_empty_intents_skips_auto_compute(store_path: str) -> None:
    schema = [
        ZarrGroupSpec(
            group="grp",
            arrays=[
                ZarrArraySpec(
                    name="data",
                    shape=(1, 4, 5),
                    dtype=np.float32,
                    chunks=(1, 4, 5),
                    expected_time_count=None,
                )
            ],
        )
    ]

    IndexedRegionStrategy(store_uri=store_path, schema=schema).write_groups(
        group_to_intents={"grp": []}
    )

    arr = cast(Any, zarr.open_group(store=store_path, mode="r", zarr_format=3)["grp/data"])
    assert arr.shape == (1, 4, 5)
