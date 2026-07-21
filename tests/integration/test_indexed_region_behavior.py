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

from contextlib import contextmanager
from pathlib import Path

import numpy as np
import pytest
import zarr

from firecube.core.controlplane import ChunkManager
from firecube.core.controlplane.types import WriteDomain
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
