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

"""Real-store behavior tests for IndexedRegionStrategy.

These tests drive the strategy against a pre-allocated local Zarr store
under ``tmp_path``.  They assert the exact cells written, fill-value
preservation in untouched slots, and per-slot claim visibility in a real
control plane — never on mock call counts.  The mutation check: removing
the slot-2 intent MUST fail
``test_indexed_region_writes_only_targeted_slots_with_real_store``.
"""

from __future__ import annotations

import functools
from collections import Counter
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import numpy as np
import pytest
import zarr
from zarr.codecs import BloscCodec
from zarr.storage import LocalStore

from firecube.core.controlplane import ChunkManager
from firecube.core.controlplane.types import WriteDomain
from firecube.ingestor.runtime.coverage import CoverageTracker
from firecube.ingestor.runtime.zarr.strategies.indexed_region import IndexedRegionStrategy
from firecube.ingestor.templates.direct_zarr import WriteIntent
from tests.helpers.storage import make_test_binding

pytestmark = pytest.mark.integration


_GROUP = "F024"
_TIME_LEN = 5
_Y = 4
_X = 4
_FILL = -1.0


def _make_region_intent(ts_index: int, data: np.ndarray) -> WriteIntent:
    return WriteIntent(
        group=_GROUP,
        array="data",
        ts_index=ts_index,
        data=data,
        kind="region",
        y_slice=slice(0, data.shape[0]),
    )


def _preallocate_store(target: Path) -> None:
    root = zarr.open_group(str(target), mode="a", zarr_format=3)
    grp = root.require_group(_GROUP)
    grp.create_array(
        name="data",
        shape=(_TIME_LEN, _Y, _X),
        dtype="float32",
        fill_value=_FILL,
        chunks=(1, _Y, _X),
    )


def test_indexed_region_writes_only_targeted_slots_with_real_store(tmp_path: Path) -> None:
    """Region writes to slots 0 and 2 must leave slots 1, 3, 4 at fill value.

    Pre-allocates a (5, 4, 4) float32 array with fill_value=-1.0, then writes
    distinct constant payloads to slots 0 and 2.  Asserts:
      1. Slot 0 and slot 2 contain the written values exactly.
      2. Slots 1, 3, 4 remain at the fill value (no bleed across slots).
      3. The on-disk shape is unchanged (5, 4, 4).
    """
    target = tmp_path / "product.zarr"
    _preallocate_store(target)

    slot0_data = np.full((_Y, _X), 7.0, dtype=np.float32)
    slot2_data = np.full((_Y, _X), 13.0, dtype=np.float32)

    IndexedRegionStrategy(store_uri=str(target)).write_groups(
        group_to_intents={
            _GROUP: [
                _make_region_intent(0, slot0_data),
                _make_region_intent(2, slot2_data),
            ],
        },
    )

    arr = zarr.open_array(str(target / _GROUP / "data"), mode="r")
    assert arr.shape == (_TIME_LEN, _Y, _X)

    np.testing.assert_array_equal(arr[0], slot0_data)
    np.testing.assert_array_equal(arr[2], slot2_data)

    fill_block = np.full((_Y, _X), _FILL, dtype=np.float32)
    for untouched_ts in (1, 3, 4):
        np.testing.assert_array_equal(
            arr[untouched_ts],
            fill_block,
            err_msg=f"slot {untouched_ts} bled from a neighboring write",
        )


def test_indexed_region_per_slot_claims_appear_in_control_plane(tmp_path: Path) -> None:
    """Each per-slot claim must be visible in the on-disk control plane while held.

    Wraps a real ``ChunkManager.acquire_claim(...)`` per slot, sampling
    ``manager.list_claims(product=...)`` inside the slot context to verify the
    expected ``WriteDomain`` identifier is recorded.  Also confirms data
    correctness so the claim wrapping does not interfere with writes.
    """
    product = "region_behavior.zarr"
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    manager = ChunkManager(
        binding=make_test_binding(tmp_path, product=product),
        workspace=workspace,
    )

    target = tmp_path / product
    _preallocate_store(target)

    expected_slots = {0, 2}
    observed: list[tuple[str, int, set[str]]] = []

    def claim_for_slot(group_name: str, ts_index: int):
        domain = WriteDomain(
            product=product,
            category="zarr_region",
            name=f"{group_name}:slot={ts_index}",
        )
        owner_id = f"behavior-run:{group_name}:slot={ts_index}"
        inner = manager.acquire_claim(product=product, domain=domain, owner_id=owner_id)

        @contextmanager
        def _tracking():
            with inner as handle:
                active = manager.list_claims(product=product)
                observed.append(
                    (group_name, ts_index, {claim.domain for claim in active}),
                )
                yield handle

        return _tracking()

    slot0_data = np.full((_Y, _X), 100.0, dtype=np.float32)
    slot2_data = np.full((_Y, _X), 200.0, dtype=np.float32)

    try:
        IndexedRegionStrategy(store_uri=str(target)).write_groups(
            group_to_intents={
                _GROUP: [
                    _make_region_intent(0, slot0_data),
                    _make_region_intent(2, slot2_data),
                ],
            },
            claim_for_slot=claim_for_slot,
        )
    finally:
        manager.close()

    assert {(group, ts) for group, ts, _ in observed} == {(_GROUP, ts) for ts in expected_slots}

    for group_name, ts_index, active_domains in observed:
        expected_id = WriteDomain(
            product=product,
            category="zarr_region",
            name=f"{group_name}:slot={ts_index}",
        ).identifier
        assert expected_id in active_domains, (
            f"slot claim {expected_id!r} was not visible in the live control "
            f"plane while held; observed: {sorted(active_domains)}"
        )

    arr = zarr.open_array(str(target / _GROUP / "data"), mode="r")
    np.testing.assert_array_equal(arr[0], slot0_data)
    np.testing.assert_array_equal(arr[2], slot2_data)
    fill_block = np.full((_Y, _X), _FILL, dtype=np.float32)
    np.testing.assert_array_equal(arr[1], fill_block)


class _CountingStore(LocalStore):
    def __init__(self, root: Path, sets: Counter[str], *, read_only: bool) -> None:
        super().__init__(root, read_only=read_only)
        self._sets = sets

    async def set(self, key: str, value: Any) -> None:
        self._sets[key] += 1
        await super().set(key, value)


# Row: (arrays name to (shape, inner chunk, shard), intents (array, ts, y0, y1[, channel[, how]])
# with how "region", "1d" or "list" (a payload the box does not take), options). Field on: every
# object set once, or with option "same" as often as with the field off.
_S3 = ((2, 4, 6), (1, 2, 6), (1, 4, 6))
_TWO = [("a", 0, 0, 2), ("a", 0, 2, 4)]
_A3 = {"a": _S3}
# fmt: off
_ROWS = {
    "rank3-two-arrays-interleaved": ({"a": _S3, "b": _S3}, [("a", 0, 0, 2), ("b", 0, 0, 2), ("a", 0, 2, 4), ("b", 0, 2, 4)], {}),
    "rank4-channel-clipped-edge-shard": ({"a": ((2, 5, 4, 3), (1, 2, 4, 1), (1, 4, 4, 1))}, [("a", 0, y0, y1, c) for c in range(3) for y0, y1 in ((0, 2), (2, 4), (4, 5))], {}),
    "rank4-no-channel-units-two-channels-wide": ({"a": ((2, 4, 4, 4), (1, 2, 4, 2), (1, 4, 4, 2))}, _TWO, {}),
    "rank5-member-per-piece": ({"a": ((2, 4, 3, 2, 2), (1, 1, 3, 2, 2), (1, 2, 3, 2, 2))}, [("a", 0, m, m + 1) for m in range(4)], {}),
    "root-group": (_A3, _TWO, {"group": "/"}),
    "slot-window": (_A3, [("a", 1, 0, 2), ("a", 1, 2, 4)], {"kwargs": {"slot_range": (0, 2), "slot_group": _GROUP}}),
    "tracked-units": (_A3, [("a", t, y, y + 2) for t in (0, 1) for y in (0, 2)], {"results": True}),
    "codec-outside-lossless-list": ({"a": ((2, 4, 6), (1, 2, 6), None)}, _TWO, {"same": 1, "gzip": 1}),
    "negative-ts-index": (_A3, [("a", -1, 0, 2), ("a", -1, 2, 4)], {"same": 1}),
    "1d-write-between-pieces": (_A3, [("a", 0, 0, 2), ("a", 0, 0, 0, None, "1d"), ("a", 0, 2, 4)], {"same": 1}),
    "payload-only-write-region-takes": (_A3, [("a", 0, 0, 2, None, "list"), ("a", 0, 2, 4)], {"same": 1}),
}
# fmt: on


def _run_row(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, row: tuple, buffer_on: bool) -> dict:
    arrays, specs, opts = row
    group, target = opts.get("group", _GROUP), tmp_path / f"buffer-{buffer_on}.zarr"
    root = zarr.open_group(str(target), mode="a", zarr_format=3)
    holder = root if group == "/" else root.require_group(group)
    codecs: dict[str, Any] = {"compressors": BloscCodec()} if "gzip" in opts else {}
    for name, (shape, chunks, shards) in arrays.items():
        holder.create_array(
            name, shape=shape, dtype="f4", fill_value=_FILL, chunks=chunks, shards=shards, **codecs
        )
    sets, resolved, results = Counter(), Counter(), []
    monkeypatch.setattr(
        IndexedRegionStrategy,
        "_session_store",
        lambda self, *, mode: _CountingStore(target, sets, read_only=mode == "r"),
    )

    def payload(i: int, shape: tuple, how: str) -> Any:
        resolved[i] += 1
        values = 100.0 * (i + 1) + np.arange(np.prod(shape), dtype="f4").reshape(shape)
        return values.tolist() if how == "list" else values

    intents = []
    for i, (name, ts, y0, y1, *rest) in enumerate(specs):
        ch, how = rest + [None, "region"][len(rest) :]
        shape = arrays[name][0]
        dims = shape[1:] if how == "1d" else (y1 - y0, *shape[2 : 3 if ch is not None else None])
        data = functools.partial(payload, i, dims, how)
        kind = "1d" if how == "1d" else "region"
        intents.append(WriteIntent(group, name, ts, data, kind, slice(y0, y1), ch))
    out = IndexedRegionStrategy(store_uri=str(target)).write_groups(
        group_to_intents={group: intents},
        zarr_unit_write_buffer=buffer_on,
        track_units="results" in opts,
        on_write=results.append if "results" in opts else None,
        **opts.get("kwargs", {}),
    )
    assert set(resolved.values()) == {1}, "a payload was resolved more than once"
    files = {
        p.relative_to(target).as_posix(): p.read_bytes()
        for p in sorted(target.rglob("*"))
        if p.is_file() and ".firecube" not in p.parts
    }
    return {"files": files, "sets": sets, "results": results, "coverage": out["coverage"]}


@pytest.mark.parametrize("name", list(_ROWS))
def test_unit_write_buffer_writes_the_same_objects_and_bytes_as_unbuffered(
    name: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    opts = _ROWS[name][2]
    off, on = (_run_row(tmp_path, monkeypatch, _ROWS[name], buffer_on=b) for b in (False, True))
    assert on["files"] == off["files"]
    assert on["coverage"] == off["coverage"]
    if opts.get("same"):
        assert on["sets"] == off["sets"], "an ineligible row changed how often objects are set"
    else:
        keys = {k for k in on["files"] if not k.endswith("zarr.json")}
        assert keys and dict(on["sets"]) == dict.fromkeys(keys, 1)
    if opts.get("results"):
        got = [(r.footprint.array_path, r.units, r.absent, r.tracked) for r in on["results"]]
        assert got == [("F024/a", ((t, 0, 0),), (), True) for t in (0, 1)]


@pytest.mark.parametrize("buffer_on", [False, True], ids=["off", "on"])
def test_payload_raising_in_a_slot_covers_no_piece_that_was_not_written(
    buffer_on: bool, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    target = tmp_path / "product.zarr"
    group = zarr.open_group(str(target), mode="a", zarr_format=3).require_group(_GROUP)
    shape, chunks, shards = _S3
    group.create_array("a", shape=shape, dtype="f4", fill_value=_FILL, chunks=chunks, shards=shards)

    covered: list[Any] = []
    record = CoverageTracker.record_write
    monkeypatch.setattr(
        CoverageTracker,
        "record_write",
        lambda self, **kw: [covered.append((kw["arrays"], kw["ts_index"])), record(self, **kw)][1],
    )
    ok = WriteIntent(_GROUP, "a", 0, np.full((2, 6), 5, "f4"), y_slice=slice(0, 2))
    bad = WriteIntent(_GROUP, "a", 0, functools.partial(int, "payload failed"), y_slice=slice(2, 4))

    with pytest.raises(ValueError, match="payload failed"):
        IndexedRegionStrategy(store_uri=str(target)).write_groups(
            group_to_intents={_GROUP: [ok, bad]}, zarr_unit_write_buffer=buffer_on
        )

    stored = np.asarray(zarr.open_array(str(target / _GROUP / "a"), mode="r")[0, :2])
    assert not covered or (stored == 5).all(), f"covered {covered} but its cells were not written"
