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

"""Tests for extracted slot-planning helpers.

Mirrors the existing test_plan_*.py tests but imports from the new
core.zarr.planning module directly (not through plan.py re-exports).
"""

from pathlib import Path
from typing import Any

import numpy as np
import pytest

from firecube.cli.zarr._slots import PLAN_SCHEMA_VERSION
from firecube.core.errors import ConfigurationError
from firecube.core.zarr import planning
from firecube.core.zarr.planning import (
    chunk_aligned_remaining,
    resolve_per_group_slot_sizes,
)
from tests.helpers.rows import table_row

pytestmark = pytest.mark.unit


def test_plan_schema_version() -> None:
    assert PLAN_SCHEMA_VERSION == "v1"


def test_chunk_aligned_remaining(subtests: Any, tmp_path: Path) -> None:
    # (row id, remaining intervals, slot size, total, expected aligned, expected blocked)
    rows = (
        # interval already aligned
        ("test_chunk_aligned_remaining_basic", [(0, 500)], 100, 500, [(0, 500)], []),
        # misaligned start is blocked up to the next slot boundary
        (
            "test_chunk_aligned_remaining_misaligned_start",
            [(73, 1000)],
            100,
            1000,
            [(100, 1000)],
            [(73, 100)],
        ),
        # aligned_start=1000 >= end=1000: no aligned range, fully blocked
        ("test_chunk_aligned_remaining_fully_blocked", [(950, 1000)], 100, 1000, [], [(950, 1000)]),
        ("test_chunk_aligned_remaining_empty", [], 100, 1000, [], []),
    )
    for row_id, intervals, slot_size, total, expected_aligned, expected_blocked in rows:
        with table_row(subtests, row_id, tmp_path):
            aligned, blocked = chunk_aligned_remaining(intervals, slot_size=slot_size, total=total)
            assert aligned == expected_aligned
            assert blocked == expected_blocked


def test_resolve_per_group_slot_sizes_explicit() -> None:
    """Explicit slot size must be multiple of all group time-chunk sizes."""
    from firecube.sdk import ZarrArraySpec, ZarrGroupSpec

    schema = [
        ZarrGroupSpec(
            group="group1",
            arrays=[
                ZarrArraySpec(name="arr1", shape=(1000, 100), chunks=(100, 100), dtype=np.float32)
            ],
        )
    ]
    result = resolve_per_group_slot_sizes(schema, explicit=200)
    assert result == {"group1": 200}


def test_resolve_per_group_slot_sizes_invalid_explicit() -> None:
    """Explicit size not divisible by group LCM should raise ConfigurationError."""
    from firecube.sdk import ZarrArraySpec, ZarrGroupSpec

    schema = [
        ZarrGroupSpec(
            group="group1",
            arrays=[
                ZarrArraySpec(name="arr1", shape=(1000, 100), chunks=(100, 100), dtype=np.float32)
            ],
        )
    ]
    with pytest.raises(ConfigurationError):
        resolve_per_group_slot_sizes(schema, explicit=75)  # 75 not divisible by 100


def test_resolve_per_group_slot_sizes_auto() -> None:
    """Without explicit, should compute LCM of time-chunks per group."""
    from firecube.sdk import ZarrArraySpec, ZarrGroupSpec

    schema = [
        ZarrGroupSpec(
            group="g1",
            arrays=[
                ZarrArraySpec(name="a", shape=(1000, 10), chunks=(100, 10), dtype=np.float32),
                ZarrArraySpec(name="b", shape=(1000, 5), chunks=(200, 5), dtype=np.float32),
            ],
        )
    ]
    result = resolve_per_group_slot_sizes(schema, explicit=None)
    assert result["g1"] == 200  # LCM(100, 200)


@pytest.mark.parametrize(
    ("remaining", "total", "unit", "ceiling", "expected"),
    [
        pytest.param([(0, 11)], 11, 1, 10, 2, id="fresh"),
        pytest.param([(1, 12)], 12, 1, 5, None, id="misaligned"),
        pytest.param([(i, i + 1) for i in range(0, 21, 2)], 21, 1, 10, None, id="fragmented"),
        pytest.param([(4, 100)], 100, 2, 30, 4, id="unit2"),
        # The interval (0, 251) ends inside the stored unit [250, 500): pod startup refuses
        # that window at any size, so no --slot-size may be suggested for it.
        pytest.param([(0, 251), (500, 750)], 3000, 250, 2, None, id="end_splits_unit"),
    ],
)
def test_smallest_executable_slot_size(
    remaining: list[tuple[int, int]], total: int, unit: int, ceiling: int, expected: int | None
) -> None:
    helper = getattr(planning, "smallest_executable_slot_size", None)
    assert helper is not None, "smallest_executable_slot_size is not defined"
    assert helper({"g": remaining}, {"g": total}, {"g": unit}, ceiling) == expected
