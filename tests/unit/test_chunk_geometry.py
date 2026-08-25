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

from types import SimpleNamespace

import pytest

from firecube.core.zarr.chunk_geometry import (
    axis_selection_is_chunk_aligned,
    chunk_axis_range,
    physical_chunk_keys_for_region,
)

pytestmark = pytest.mark.unit


def _intent(array: str = "values") -> SimpleNamespace:
    return SimpleNamespace(group="data", array=array)


def _selection(
    *,
    ts_index: int = 0,
    y_start: int = 0,
    y_stop: int = 2,
    channel_index: int | None = None,
) -> SimpleNamespace:
    return SimpleNamespace(
        ts_index=ts_index,
        y_start=y_start,
        y_stop=y_stop,
        channel_index=channel_index,
    )


def test_chunk_axis_range_known_and_boundary_cases() -> None:
    assert list(chunk_axis_range(0, 2, 2)) == [0]
    assert list(chunk_axis_range(2, 4, 2)) == [1]
    assert list(chunk_axis_range(1, 5, 2)) == [0, 1, 2]
    assert list(chunk_axis_range(0, 4, 4)) == [0]
    assert list(chunk_axis_range(2, 2, 2)) == []


def test_axis_selection_is_chunk_aligned_boundaries() -> None:
    assert axis_selection_is_chunk_aligned(0, 2, 4, 2) is True
    assert axis_selection_is_chunk_aligned(2, 4, 4, 2) is True
    assert axis_selection_is_chunk_aligned(0, 4, 4, 2) is True
    assert axis_selection_is_chunk_aligned(1, 3, 4, 2) is False
    assert axis_selection_is_chunk_aligned(2, 2, 4, 2) is True


def test_physical_chunk_keys_match_indexed_region_disjoint_cases() -> None:
    keys, aligned = physical_chunk_keys_for_region(
        group="data",
        intent=_intent(),
        shape=(1, 6, 4),
        chunks=(1, 2, 4),
        selection=_selection(y_start=2, y_stop=4),
    )

    assert keys == {("data", "values", (0, 1, 0))}
    assert aligned is True


def test_physical_chunk_keys_match_indexed_region_overlap_case() -> None:
    first, first_aligned = physical_chunk_keys_for_region(
        group="data",
        intent=_intent(),
        shape=(1, 4, 4),
        chunks=(1, 4, 4),
        selection=_selection(y_start=0, y_stop=2),
    )
    second, second_aligned = physical_chunk_keys_for_region(
        group="data",
        intent=_intent(),
        shape=(1, 4, 4),
        chunks=(1, 4, 4),
        selection=_selection(y_start=2, y_stop=4),
    )

    assert first == second == {("data", "values", (0, 0, 0))}
    assert first_aligned is False
    assert second_aligned is False


def test_physical_chunk_keys_match_indexed_region_time_chunk_case() -> None:
    first, first_aligned = physical_chunk_keys_for_region(
        group="data",
        intent=_intent(),
        shape=(2, 4, 4),
        chunks=(2, 4, 4),
        selection=_selection(ts_index=0, y_start=0, y_stop=4),
    )
    second, second_aligned = physical_chunk_keys_for_region(
        group="data",
        intent=_intent(),
        shape=(2, 4, 4),
        chunks=(2, 4, 4),
        selection=_selection(ts_index=1, y_start=0, y_stop=4),
    )

    assert first == second == {("data", "values", (0, 0, 0))}
    assert first_aligned is False
    assert second_aligned is False


def test_physical_chunk_keys_rank4_channel_selection() -> None:
    keys, aligned = physical_chunk_keys_for_region(
        group="data",
        intent=_intent(),
        shape=(1, 4, 4, 3),
        chunks=(1, 2, 4, 1),
        selection=_selection(y_start=2, y_stop=4, channel_index=1),
    )

    assert keys == {("data", "values", (0, 1, 0, 1))}
    assert aligned is True


def test_physical_chunk_keys_out_of_bounds_ts_raises() -> None:
    with pytest.raises(ValueError, match="ts_index is outside"):
        physical_chunk_keys_for_region(
            group="data",
            intent=_intent(),
            shape=(1, 4, 4),
            chunks=(1, 2, 4),
            selection=_selection(ts_index=1),
        )
