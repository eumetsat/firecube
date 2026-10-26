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

"""N-D region writes and callable slot payloads.

``write_region`` writes ``arr[ts, y_slice, ...]`` for arrays of any rank >= 3,
so e.g. a ``(time, realization, period, level, lat, lon)`` ensemble array can
be written one member at a time; slot (``kind="1d"``) writes accept a zero-arg
callable resolved once at dispatch.
"""

from __future__ import annotations

from types import SimpleNamespace

import numpy as np
import pytest

from firecube.core.zarr.guard.chunk_geometry import physical_chunk_keys_for_region
from firecube.core.zarr.region_writer import RegionZarrWriter
from firecube.ingestor.runtime.zarr.strategies.indexed_region import IndexedRegionStrategy
from firecube.ingestor.templates.direct_zarr import WriteIntent

pytestmark = pytest.mark.unit


@pytest.fixture
def writer(tmp_path):
    return RegionZarrWriter(str(tmp_path / "store.zarr"))


@pytest.mark.parametrize("shape", [(3, 4, 2, 5, 6), (3, 4, 2, 3, 5, 6)], ids=["5d", "6d"])
def test_region_write_one_member_at_a_time(writer, shape):
    """Each member's write lands at its position along axis 1; other slots stay empty."""
    writer.ensure_group("grp/t", shape=shape, dtype=np.float32, fill_value=np.nan)
    member_values = 10.0 * np.arange(shape[1], dtype=np.float32)
    for member, value in enumerate(member_values):
        block = np.full((1, *shape[2:]), value, dtype=np.float32)
        writer.write_region("grp", "t", ts_index=1, y_slice=slice(member, member + 1), data=block)

    expected = np.full(shape, np.nan, dtype=np.float32)
    expected[1] = member_values.reshape(-1, *([1] * (len(shape) - 2)))
    np.testing.assert_array_equal(np.asarray(writer._open_root()["grp/t"][...]), expected)


@pytest.mark.parametrize(
    "y_slice", [slice(2, 3), slice(1, 3), slice(0, 4)], ids=["middle", "two", "all"]
)
def test_region_write_rank6_member_ranges(writer, y_slice):
    """One, several or all members per write; members outside the slice stay empty."""
    shape = (2, 4, 2, 3, 5, 6)
    writer.ensure_group("grp/t", shape=shape, dtype=np.float32, fill_value=np.nan)
    n_selected = y_slice.stop - y_slice.start
    block = np.arange(n_selected * 2 * 3 * 5 * 6, dtype=np.float32).reshape(n_selected, 2, 3, 5, 6)
    writer.write_region("grp", "t", ts_index=0, y_slice=y_slice, data=block)

    expected = np.full(shape, np.nan, dtype=np.float32)
    expected[0, y_slice] = block
    np.testing.assert_array_equal(np.asarray(writer._open_root()["grp/t"][...]), expected)


def test_region_write_rank5_rejects_channel_index(writer):
    writer.ensure_group("grp/t", shape=(1, 2, 2, 3, 3), dtype=np.float32, fill_value=0.0)
    with pytest.raises(ValueError, match="channel_index is only supported for rank-4"):
        writer.write_region(
            "grp",
            "t",
            ts_index=0,
            y_slice=slice(0, 1),
            data=np.ones((1, 2, 3, 3)),
            channel_index=0,
        )


def test_region_write_rank2_still_unsupported(writer):
    writer.ensure_group("grp/t", shape=(2, 4), dtype=np.float32, fill_value=0.0)
    with pytest.raises(ValueError, match="Unsupported array rank"):
        writer.write_region("grp", "t", ts_index=0, y_slice=slice(0, 2), data=np.ones(2))


def test_chunk_keys_rank5_cover_all_later_axes():
    keys, aligned = physical_chunk_keys_for_region(
        group="g",
        intent=SimpleNamespace(group="g", array="t"),
        shape=(2, 4, 3, 5, 6),
        chunks=(1, 1, 1, 5, 6),
        selection=SimpleNamespace(ts_index=1, y_start=2, y_stop=3, channel_index=None),
    )
    assert keys == {("g", "t", (1, 2, period, 0, 0)) for period in range(3)}
    assert aligned is True


def test_concurrent_selection_accepts_rank5_and_rejects_channel():
    intent = SimpleNamespace(
        group="g", array="t", ts_index=0, y_slice=slice(0, 1), channel_index=None
    )
    selection = IndexedRegionStrategy._validate_region_selection(
        intent, (2, 4, 3, 5, 6), source="test"
    )
    assert (selection.y_start, selection.y_stop) == (0, 1)
    intent.channel_index = 0
    with pytest.raises(ValueError, match="rank-4 arrays only"):
        IndexedRegionStrategy._validate_region_selection(intent, (2, 4, 3, 5, 6), source="test")


def test_concurrent_selection_rejects_rank5_slice_outside_the_array():
    intent = SimpleNamespace(
        group="g", array="t", ts_index=0, y_slice=slice(3, 5), channel_index=None
    )
    with pytest.raises(ValueError, match="outside the target array"):
        IndexedRegionStrategy._validate_region_selection(intent, (2, 4, 3, 5, 6), source="test")


def test_callable_slot_payload_resolved_once_at_dispatch(writer):
    writer.ensure_group("grp/t", shape=(2, 3, 4, 5), dtype=np.float32, fill_value=np.nan)
    calls = []

    def payload() -> np.ndarray:
        calls.append(1)
        return np.full((3, 4, 5), 7.0, dtype=np.float32)

    intent = WriteIntent.slot(group="grp", array="t", index=1, data=payload)
    assert calls == [], "callable must not run when the intent is built"
    IndexedRegionStrategy._dispatch_intent(writer, intent)
    assert calls == [1]
    assert np.all(np.asarray(writer._open_root()["grp/t"][1]) == 7.0)


def test_callable_slot_payload_error_propagates_and_writes_nothing(writer):
    """A failing lazy slot payload surfaces its own error; the slot stays empty."""
    writer.ensure_group("grp/t", shape=(2, 3, 4, 5), dtype=np.float32, fill_value=np.nan)

    def payload() -> np.ndarray:
        raise RuntimeError("source file unreadable")

    intent = WriteIntent.slot(group="grp", array="t", index=0, data=payload)
    with pytest.raises(RuntimeError, match="source file unreadable"):
        IndexedRegionStrategy._dispatch_intent(writer, intent)
    assert np.isnan(np.asarray(writer._open_root()["grp/t"][...])).all()
