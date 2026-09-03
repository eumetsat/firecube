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

"""Regression check for xarray open cost on dense time coordinates.

The contract here is simple: a legacy time coordinate chunked at one slot per
file forces xarray to fetch one chunk per time value, while the dense
preallocated layout only needs ceil(T / 256) chunk reads.
"""

from __future__ import annotations

import subprocess
import sys
from collections import Counter
from pathlib import Path

import numpy as np
import pytest
import xarray as xr
import zarr
from zarr.storage import LocalStore

pytestmark = pytest.mark.integration

_GROUP = "data"
_COORD = "time"
_TOTAL_SLOTS = 4320
_DENSE_CHUNK = 256
_EXPECTED_DENSE_READS = 17


class CountingStore(LocalStore):
    """LocalStore proxy that counts reads by key."""

    def __init__(
        self, root: Path | str, *, read_only: bool = False, counts: Counter[str] | None = None
    ) -> None:
        super().__init__(root, read_only=read_only)
        self.counts = counts if counts is not None else Counter()

    async def get(self, key: str, prototype=None, byte_range=None):
        self.counts[key] += 1
        return await super().get(key, prototype=prototype, byte_range=byte_range)

    def with_read_only(self, read_only: bool = False) -> CountingStore:
        return type(self)(root=self.root, read_only=read_only, counts=self.counts)


def _create_dense_cube(path: Path, *, chunk_len: int) -> None:
    root = zarr.open_group(str(path), mode="w", zarr_format=3)
    group = root.require_group(_GROUP)
    epoch = np.datetime64("2024-01-01T00:00:00", "ns")
    values = epoch + np.arange(_TOTAL_SLOTS, dtype=np.int64) * np.timedelta64(600, "s")
    group.create_array(
        _COORD,
        data=values,
        chunks=(chunk_len,),
        overwrite=True,
        dimension_names=(_COORD,),
    )


def _count_time_chunk_reads(cube_path: Path, *, consolidated: bool) -> int:
    store = CountingStore(cube_path)
    ds = xr.open_zarr(store, group=_GROUP, consolidated=consolidated, zarr_format=3)
    try:
        _ = ds[_COORD].values
    finally:
        ds.close()
    return sum(count for key, count in store.counts.items() if f"{_COORD}/c/" in key)


@pytest.fixture()
def legacy_cube(tmp_path: Path) -> Path:
    path = tmp_path / "legacy.zarr"
    subprocess.run(
        [
            sys.executable,
            str(Path(__file__).resolve().parents[1] / "scripts" / "create_legacy_cube.py"),
            str(path),
            str(_TOTAL_SLOTS),
        ],
        check=True,
    )
    return path


@pytest.fixture()
def preallocated_cube(tmp_path: Path) -> Path:
    path = tmp_path / "preallocated.zarr"
    _create_dense_cube(path, chunk_len=_DENSE_CHUNK)
    return path


def test_xarray_open_cost_scales_with_time_coord_chunking(
    legacy_cube: Path,
    preallocated_cube: Path,
) -> None:
    legacy_reads = _count_time_chunk_reads(legacy_cube, consolidated=False)
    preallocated_reads = _count_time_chunk_reads(preallocated_cube, consolidated=False)

    assert legacy_reads == _TOTAL_SLOTS, legacy_reads
    assert preallocated_reads == _EXPECTED_DENSE_READS, preallocated_reads
