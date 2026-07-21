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

"""Slot-range parity regression with the new ZarrArraySpec fields and static intents.

Wave-3 lock-in for Phase-3 slot-range semantics combined with the
``ZarrArraySpec`` fields added earlier in this branch (``shards``, ``attrs``,
``dimension_names``, ``time_indexed``) and the ``kind="static"`` write-intent
dispatch path on ``IndexedRegionStrategy``.

These tests simulate two sequential "pods" (same process, distinct
``slot_range`` ownership) writing into one pre-allocated Zarr store with a
mixed schema (one time-indexed array plus one static coordinate). They assert:

1. Disjoint slot writes from two pods do not interfere; all 10 slots end up
   populated and the shared static ``lat`` array is idempotent across them.
2. A second pod writing divergent static data on resume raises
   :class:`SchemaDriftError` instead of silently overwriting.
3. ``_compute_schema_hash`` is deterministic across pod runs (same schema
   inputs always produce the same 16-character hex digest).
"""

from __future__ import annotations

import tempfile
from typing import Any, cast

import numpy as np
import pytest
import zarr
from zarr.storage import LocalStore

from firecube.core.errors import SchemaDriftError
from firecube.core.zarr.region_writer import RegionZarrWriter
from firecube.ingestor.runtime.zarr.strategies.indexed_region import IndexedRegionStrategy
from firecube.ingestor.templates.direct_zarr import (
    WriteIntent,
    ZarrArraySpec,
    ZarrGroupSpec,
    _compute_schema_hash,
)

pytestmark = pytest.mark.integration


_GROUP = "g"
_TIME_LEN = 10
_Y = 4
_GLOBAL_EXPECTED: dict[str, int] = {_GROUP: _TIME_LEN}


def _make_schema() -> list[ZarrGroupSpec]:
    """Mixed-schema fixture: one time-indexed array + one static coordinate.

    Mirrors the ``direct_zarr_capable_test_plugin`` shape after T18: every
    new ``ZarrArraySpec`` field (``chunks``, ``shards``, ``attrs``,
    ``dimension_names``, ``time_indexed``) is exercised, plus
    ``expected_time_count`` on the time-indexed array.
    """
    return [
        ZarrGroupSpec(
            group=_GROUP,
            arrays=[
                ZarrArraySpec(
                    name="data",
                    shape=(_TIME_LEN, _Y),
                    dtype="float32",
                    chunks=(1, _Y),
                    expected_time_count=_TIME_LEN,
                    dimension_names=("timestamp", "y"),
                    attrs={"units": "K", "long_name": "test data"},
                ),
                ZarrArraySpec(
                    name="lat",
                    shape=(_Y,),
                    dtype="float64",
                    chunks=(_Y,),
                    time_indexed=False,
                    dimension_names=("y",),
                    attrs={"units": "degrees_north", "standard_name": "latitude"},
                ),
            ],
        )
    ]


def _preallocate(store_uri: str) -> None:
    """Pre-allocate arrays via :class:`RegionZarrWriter` honoring ``time_indexed``.

    Mirrors the parallel-mode pod-startup pre-allocation path so that
    ``write_groups(..., slot_range=...)`` enters parallel mode and skips the
    sequential-mode schema-augmentation path (notepad T16). Time-indexed
    arrays use ``expected_time_count`` for the leading axis; static arrays
    keep the declared shape verbatim.
    """
    writer = RegionZarrWriter(store_uri)
    for group_spec in _make_schema():
        for arr_spec in group_spec.arrays:
            if arr_spec.time_indexed:
                effective_shape: tuple[int, ...] = (
                    arr_spec.expected_time_count or _TIME_LEN,
                    *arr_spec.shape[1:],
                )
            else:
                effective_shape = arr_spec.shape
            writer.ensure_group(
                f"{group_spec.group}/{arr_spec.name}",
                shape=effective_shape,
                dtype=arr_spec.dtype,
                fill_value=arr_spec.fill_value,
                chunks=arr_spec.chunks,
                shards=arr_spec.shards,
                attrs=arr_spec.attrs,
                dimension_names=arr_spec.dimension_names,
            )


def _make_strategy(store_uri: str) -> IndexedRegionStrategy:
    return IndexedRegionStrategy(store_uri=store_uri, schema=_make_schema())


def _data_intent(ts_index: int) -> WriteIntent:
    return WriteIntent(
        group=_GROUP,
        array="data",
        ts_index=ts_index,
        data=np.full((_Y,), float(ts_index), dtype="float32"),
        kind="1d",
    )


def _static_intent(lat_values: np.ndarray) -> WriteIntent:
    return WriteIntent(
        group=_GROUP,
        array="lat",
        ts_index=0,
        data=lat_values,
        kind="static",
    )


def test_disjoint_slot_pods_with_static_array() -> None:
    """Two disjoint slot pods both write; static ``lat`` is idempotent across them.

    Pod A owns ``slot_range=[0, 5)`` and writes data slots 0..4 plus the
    static ``lat``. Pod B owns ``slot_range=[5, 10)`` and writes data slots
    5..9 plus the same static ``lat``. After both pods complete, the store
    must contain:

      * all 10 time-indexed slots populated with their distinct payloads
        (no bleed across the pod boundary)
      * the shared static ``lat`` round-tripped byte-for-byte
    """
    with tempfile.TemporaryDirectory() as tmp:
        store_uri = f"file://{tmp}"
        _preallocate(store_uri)
        lat_values = np.array([10.0, 20.0, 30.0, 40.0], dtype=np.float64)

        pod_a_intents: list[WriteIntent] = [_data_intent(i) for i in range(0, 5)]
        pod_a_intents.append(_static_intent(lat_values))
        _make_strategy(store_uri).write_groups(
            group_to_intents={_GROUP: pod_a_intents},
            slot_range=(0, 5),
        )

        pod_b_intents: list[WriteIntent] = [_data_intent(i) for i in range(5, 10)]
        pod_b_intents.append(_static_intent(lat_values))
        _make_strategy(store_uri).write_groups(
            group_to_intents={_GROUP: pod_b_intents},
            slot_range=(5, 10),
        )

        root = zarr.open_group(store=LocalStore(tmp), mode="r", zarr_format=3)
        data_arr = np.asarray(cast(Any, root[f"{_GROUP}/data"])[:])
        assert data_arr.shape == (_TIME_LEN, _Y)
        for ts in range(_TIME_LEN):
            np.testing.assert_array_equal(
                data_arr[ts],
                np.full((_Y,), float(ts), dtype="float32"),
                err_msg=f"slot {ts} did not round-trip across the pod boundary",
            )

        lat_arr = np.asarray(cast(Any, root[f"{_GROUP}/lat"])[:])
        np.testing.assert_array_equal(lat_arr, lat_values)


def test_divergent_static_on_resume_raises() -> None:
    """Pod B writing divergent static data over pod A's must raise ``SchemaDriftError``.

    The first write into a fresh array trips the fill-value idempotency
    short-circuit and succeeds. A subsequent write with different bytes
    must surface a drift error rather than silently overwriting the
    write-once coordinate.
    """
    with tempfile.TemporaryDirectory() as tmp:
        store_uri = f"file://{tmp}"
        _preallocate(store_uri)
        lat_values_a = np.array([10.0, 20.0, 30.0, 40.0], dtype=np.float64)
        lat_values_b = np.array([11.0, 22.0, 33.0, 44.0], dtype=np.float64)

        _make_strategy(store_uri).write_groups(
            group_to_intents={_GROUP: [_static_intent(lat_values_a)]},
            slot_range=(0, 5),
        )

        with pytest.raises(SchemaDriftError, match="diverged"):
            _make_strategy(store_uri).write_groups(
                group_to_intents={_GROUP: [_static_intent(lat_values_b)]},
                slot_range=(5, 10),
            )


def test_schema_hash_stable_across_pod_runs() -> None:
    """``_compute_schema_hash`` is deterministic regardless of pod order.

    Both pods construct the schema independently from the same
    :func:`_make_schema` factory; the recorded schema verification audit
    event must therefore carry an identical 16-character hex digest so the
    control plane treats both pods as agreeing on the layout.
    """
    schema_pod_a = _make_schema()
    schema_pod_b = _make_schema()

    hash_a = _compute_schema_hash(schema_pod_a, _GLOBAL_EXPECTED)
    hash_b = _compute_schema_hash(schema_pod_b, _GLOBAL_EXPECTED)

    assert hash_a == hash_b, (
        f"schema hash not stable across pod runs: pod_a={hash_a!r} pod_b={hash_b!r}"
    )
    assert len(hash_a) == 16, f"expected 16-character hex digest, got len={len(hash_a)}: {hash_a!r}"
    assert all(c in "0123456789abcdef" for c in hash_a), (
        f"schema hash is not lowercase hex: {hash_a!r}"
    )
