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

"""Default-reader open-time gate for coord-managed cubes.

The read-cost win captured by ``chunks=(256,)`` on the time coordinate is
worthless if only clients that hand-tune ``zarr.config.set(...)`` see it. A
naive caller running ``xr.open_zarr(url)`` with default reader settings must
still hit the low-GET, low-latency envelope on a cube that carries the
``firecube_coord_managed`` marker and 4320 slots at ``chunks=(256,)``.

Two tests:

* ``test_local_open_time`` — always runs. Builds a synthetic cube on a local
  Zarr store, opens it with ``xr.open_zarr`` and DEFAULT reader settings,
  measures total GET count on a counting store wrapper. Gate: total GETs
  <= 25 (headroom over target 20).
* ``test_remote_open_time`` — SKIP unless ``FIRECUBE_OBJSTORE_URL`` is set,
  in which case it measures open time against the real bucket URL using the
  same default reader settings. Never FAILs on missing credentials — the
  local test is the mandatory CI gate.
"""

from __future__ import annotations

import os
import time
from collections import Counter
from pathlib import Path
from typing import TYPE_CHECKING

import numpy as np
import pytest
import xarray as xr
import zarr
from zarr.storage import LocalStore

from firecube.core.zarr._sealing_markers import ATTR_COORD_MANAGED

if TYPE_CHECKING:
    from collections.abc import Iterable

    from zarr.abc.store import ByteRequest
    from zarr.core.buffer import Buffer, BufferPrototype

pytestmark = pytest.mark.integration

_GROUP = "data"
_COORD = "timestamp"
_TOTAL_SLOTS = 4320
_CHUNK_LEN = 256
_REMOTE_OPEN_BUDGET_S = 5.0
_GET_BUDGET = 25
_EPOCH_NS = np.datetime64("2024-01-01T00:00:00", "ns")
_CADENCE_S = 600


class GetCountingStore(LocalStore):
    """LocalStore proxy that counts every ``get`` and ``get_partial_values`` call.

    Mirrors the ``CountingStore`` pattern in ``test_xarray_open_cost.py`` but
    also traces ``get_partial_values`` since default-reader open paths in
    zarr>=3.0 sometimes batch chunk fetches through the partial-values API.
    Every key request counts as a single GET — that matches the "network
    round-trips" mental model an operator uses when budgeting object-store
    open cost.
    """

    def __init__(
        self,
        root: Path | str,
        *,
        read_only: bool = False,
        counts: Counter[str] | None = None,
    ) -> None:
        super().__init__(root, read_only=read_only)
        self.counts: Counter[str] = counts if counts is not None else Counter()

    async def get(
        self,
        key: str,
        prototype: BufferPrototype | None = None,
        byte_range: ByteRequest | None = None,
    ) -> Buffer | None:
        self.counts[key] += 1
        return await super().get(key, prototype=prototype, byte_range=byte_range)

    async def get_partial_values(
        self,
        prototype: BufferPrototype,
        key_ranges: Iterable[tuple[str, ByteRequest | None]],
    ) -> list[Buffer | None]:
        materialized = list(key_ranges)
        for key, _ in materialized:
            self.counts[key] += 1
        return await super().get_partial_values(prototype, materialized)

    def with_read_only(self, read_only: bool = False) -> GetCountingStore:
        return type(self)(root=self.root, read_only=read_only, counts=self.counts)


def _build_coord_managed_cube(path: Path) -> None:
    """Materialize a 4320-slot ``chunks=(256,)`` cube with the coord-managed marker.

    Mirrors the on-disk state ``firecube zarr preallocate`` leaves behind for
    an engine-managed coordinate: dense values written into every slot, then
    the ``firecube_coord_managed`` attr stamped on the coord array. Built
    directly against ``zarr.open_group`` — no plugin dependency, no CLI —
    so this test measures the reader behaviour in isolation.
    """
    store = LocalStore(str(path))
    root = zarr.open_group(store=store, mode="w", zarr_format=3)
    group = root.require_group(_GROUP)
    values = _EPOCH_NS + np.arange(_TOTAL_SLOTS, dtype=np.int64) * np.timedelta64(_CADENCE_S, "s")
    arr = group.create_array(
        name=_COORD,
        shape=(_TOTAL_SLOTS,),
        dtype="datetime64[ns]",
        fill_value=np.datetime64("NaT", "ns"),
        chunks=(_CHUNK_LEN,),
        dimension_names=[_COORD],
    )
    arr[:] = values
    arr.attrs[ATTR_COORD_MANAGED] = True


def _warm_up_reader(tmp_path: Path) -> None:
    """Prime xarray + zarr module caches before the timed open.

    First-in-process ``xr.open_zarr`` calls pay a large one-off cost for
    lazy imports (``xarray.backends.zarr``, codec pipelines, numpy dtype
    metadata) that has nothing to do with per-cube read cost. This warm-up
    replicates a hot-process condition so the gate isolates read-cost
    regressions from Python module loading.
    """
    warm_path = tmp_path / "_warmup.zarr"
    store = LocalStore(str(warm_path))
    root = zarr.open_group(store=store, mode="w", zarr_format=3)
    group = root.require_group(_GROUP)
    group.create_array(
        name=_COORD,
        shape=(1,),
        dtype="datetime64[ns]",
        fill_value=np.datetime64("NaT", "ns"),
        chunks=(1,),
        dimension_names=[_COORD],
    )
    ds = xr.open_zarr(store, group=_GROUP, consolidated=False, zarr_format=3)
    _ = ds[_COORD].values
    ds.close()


def _measure_open(store: GetCountingStore) -> tuple[float, int]:
    """Return (elapsed_seconds, total_get_count) for a default-reader open.

    Timed span covers ``xr.open_zarr`` and the immediate materialization of
    the ``timestamp`` coordinate values. That combination is the operator-
    visible cost the read-cost fix targets: a client that just wants to know
    "what time slots does this cube cover" pays this exact bill on every
    open.

    Explicitly does NOT touch ``zarr.config`` — the whole point of this gate
    is that default reader settings hit the low-GET envelope.
    """
    start = time.perf_counter()
    ds = xr.open_zarr(store, group=_GROUP, consolidated=False, zarr_format=3)
    try:
        _ = ds[_COORD].values
    finally:
        ds.close()
    elapsed = time.perf_counter() - start
    return elapsed, sum(store.counts.values())


def test_local_open_time(tmp_path: Path) -> None:
    """Default-reader open+first-touch fits the local budget on coord-managed cubes.

    Builds the synthetic coord-managed cube on a local store, wraps the
    store to count every ``get`` / ``get_partial_values`` call, and asserts
    the GET budget that a naive ``xr.open_zarr(url)`` caller must satisfy.

    Any failure means the operational deliverable — the read-cost win
    reaching a default-configured reader — has silently broken.
    """
    _warm_up_reader(tmp_path)

    cube_path = tmp_path / "coord_managed.zarr"
    _build_coord_managed_cube(cube_path)

    store = GetCountingStore(cube_path)
    _elapsed, gets = _measure_open(store)

    assert gets <= _GET_BUDGET, (
        f"Local default-reader open issued {gets} GETs, "
        f"budget is {_GET_BUDGET} (target 20: ceil(4320/256)=17 "
        f"chunk reads + metadata). Coord-array chunking may have regressed "
        f"toward per-slot chunks; verify chunks=(256,) is preserved."
    )


@pytest.mark.benchmark
@pytest.mark.skipif(
    not os.environ.get("FIRECUBE_OBJSTORE_URL"),
    reason="FIRECUBE_OBJSTORE_URL not set; remote credentials are never required for CI",
)
def test_remote_open_time() -> None:
    """Object-store default-reader open time is within budget if credentials provided.

    Reads the target URL from ``FIRECUBE_OBJSTORE_URL``. When the variable is
    unset the test SKIPs cleanly — remote credentials are never required for
    CI to pass; ``test_local_open_time`` is the mandatory local gate. When
    the variable is set, opens the remote store with default reader settings
    (no ``zarr.config.set({"async.concurrency": 64})`` — the whole point is
    default-reader friendliness) and asserts a wall-clock budget generous
    enough to absorb typical network jitter while still catching a regression
    to per-slot chunk fetches.
    """
    url = os.environ["FIRECUBE_OBJSTORE_URL"]

    start = time.perf_counter()
    ds = xr.open_zarr(url, group=_GROUP, consolidated=False, zarr_format=3)
    try:
        _ = ds[_COORD].values
    finally:
        ds.close()
    elapsed = time.perf_counter() - start

    assert elapsed <= _REMOTE_OPEN_BUDGET_S, (
        f"Remote default-reader open+first-touch took {elapsed:.4f}s against "
        f"{url!r}, budget is {_REMOTE_OPEN_BUDGET_S}s. Either the cube at that "
        "URL is not coord-managed with chunks=(256,) or the reader has "
        "regressed to per-slot fetches."
    )
