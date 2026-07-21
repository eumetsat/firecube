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
_slot_planning module directly (not through plan.py re-exports).
"""

import click
import numpy as np
import pytest

from firecube.cli._slot_planning import (
    PLAN_SCHEMA_VERSION,
    _chunk_aligned_remaining,
    _resolve_per_group_slot_sizes,
)

pytestmark = pytest.mark.unit


def test_plan_schema_version() -> None:
    assert PLAN_SCHEMA_VERSION == "v1"


def test_chunk_aligned_remaining_basic() -> None:
    """Test chunk-aligned remaining computation."""
    # interval already aligned: start=0, slot=100, total=500
    aligned, blocked = _chunk_aligned_remaining([(0, 500)], slot_size=100, total=500)
    assert aligned == [(0, 500)]
    assert blocked == []


def test_chunk_aligned_remaining_misaligned_start() -> None:
    """Misaligned start should be blocked."""
    # start=73, slot=100, total=1000
    aligned, blocked = _chunk_aligned_remaining([(73, 1000)], slot_size=100, total=1000)
    assert aligned == [(100, 1000)]
    assert blocked == [(73, 100)]


def test_chunk_aligned_remaining_fully_blocked() -> None:
    """Interval that produces no aligned range is fully blocked."""
    # (950, 1000) with slot=100, total=1000: aligned_start=1000 >= end=1000
    aligned, blocked = _chunk_aligned_remaining([(950, 1000)], slot_size=100, total=1000)
    assert aligned == []
    assert blocked == [(950, 1000)]


def test_chunk_aligned_remaining_empty() -> None:
    aligned, blocked = _chunk_aligned_remaining([], slot_size=100, total=1000)
    assert aligned == []
    assert blocked == []


def test_resolve_per_group_slot_sizes_explicit() -> None:
    """Explicit slot size must be multiple of all group time-chunk sizes."""
    from firecube.ingestor.api import ZarrArraySpec, ZarrGroupSpec

    schema = [
        ZarrGroupSpec(
            group="group1",
            arrays=[
                ZarrArraySpec(name="arr1", shape=(1000, 100), chunks=(100, 100), dtype=np.float32)
            ],
        )
    ]
    result = _resolve_per_group_slot_sizes(schema, explicit=200)
    assert result == {"group1": 200}


def test_resolve_per_group_slot_sizes_invalid_explicit() -> None:
    """Explicit size not divisible by group LCM should raise ClickException."""
    from firecube.ingestor.api import ZarrArraySpec, ZarrGroupSpec

    schema = [
        ZarrGroupSpec(
            group="group1",
            arrays=[
                ZarrArraySpec(name="arr1", shape=(1000, 100), chunks=(100, 100), dtype=np.float32)
            ],
        )
    ]
    with pytest.raises(click.ClickException):
        _resolve_per_group_slot_sizes(schema, explicit=75)  # 75 not divisible by 100


def test_resolve_per_group_slot_sizes_auto() -> None:
    """Without explicit, should compute LCM of time-chunks per group."""
    from firecube.ingestor.api import ZarrArraySpec, ZarrGroupSpec

    schema = [
        ZarrGroupSpec(
            group="g1",
            arrays=[
                ZarrArraySpec(name="a", shape=(1000, 10), chunks=(100, 10), dtype=np.float32),
                ZarrArraySpec(name="b", shape=(1000, 5), chunks=(200, 5), dtype=np.float32),
            ],
        )
    ]
    result = _resolve_per_group_slot_sizes(schema, explicit=None)
    assert result["g1"] == 200  # LCM(100, 200)
