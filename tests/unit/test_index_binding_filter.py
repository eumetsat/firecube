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

"""Tests for filter_items_by_index derived filter."""

from __future__ import annotations

import numpy as np
import pytest

from firecube.core.index_resolve import resolve_index_spec
from firecube.core.index_spec import IndexSpec, ItemInfo, RegularTimeAxis
from firecube.ingestor.errors import ConfigurationError
from firecube.ingestor.runtime.index_binding import filter_items_by_index

_EPOCH = "2024-01-01T00:00:00Z"
_EPOCH_S = 1704067200


def _make_resolved(cadence_s: int = 600, size: int = 10, group: str = "data"):
    spec = IndexSpec(
        name="v1",
        groups={
            group: RegularTimeAxis(
                coordinate="timestamp", epoch=_EPOCH, cadence_s=cadence_s, slot_count=size
            )
        },
    )
    return resolve_index_spec(spec, time_dim_name="timestamp")


def _coord(slot: int, cadence_s: int = 600) -> np.datetime64:
    return np.datetime64(_EPOCH_S + slot * cadence_s, "s")


class _Sentinel:
    pass


SENTINEL = _Sentinel()


def test_filter_drops_none_items():
    """Items for which inspect_item returns None are dropped."""
    resolved = _make_resolved()
    items = list(range(10))

    def inspect(item, ctx):
        assert ctx is SENTINEL
        if item % 2 == 0:
            return None
        return ItemInfo(coordinate=_coord(item))

    result = filter_items_by_index(items, resolved, 0, 10, None, inspect, SENTINEL)
    assert len(result) == 5
    assert all(i % 2 != 0 for i in result)


def test_filter_drops_out_of_range():
    """Items outside [slot_start, slot_end) are dropped."""
    resolved = _make_resolved()
    items = list(range(10))

    def inspect(item, ctx):
        return ItemInfo(coordinate=_coord(item))

    result = filter_items_by_index(items, resolved, 3, 7, None, inspect, SENTINEL)
    assert result == [3, 4, 5, 6]


def test_filter_threads_ctx():
    """ctx is passed to inspect_item for every item."""
    resolved = _make_resolved()
    received_ctxs = []

    def inspect(item, ctx):
        received_ctxs.append(ctx)
        return ItemInfo(coordinate=_coord(item))

    filter_items_by_index(list(range(5)), resolved, 0, 5, None, inspect, SENTINEL)
    assert all(c is SENTINEL for c in received_ctxs)
    assert len(received_ctxs) == 5


def test_filter_multi_group_requires_slot_group():
    """Multi-group resolved index requires explicit slot_group."""
    spec = IndexSpec(
        name="v1",
        groups={
            "fast": RegularTimeAxis(
                coordinate="timestamp", epoch=_EPOCH, cadence_s=300, slot_count=12
            ),
            "slow": RegularTimeAxis(
                coordinate="timestamp", epoch=_EPOCH, cadence_s=600, slot_count=6
            ),
        },
    )
    resolved = resolve_index_spec(spec, time_dim_name="timestamp")

    def inspect(item, ctx):
        return ItemInfo(coordinate=_coord(item, 300))

    with pytest.raises(ConfigurationError, match="slot_group"):
        filter_items_by_index(list(range(5)), resolved, 0, 5, None, inspect, SENTINEL)
