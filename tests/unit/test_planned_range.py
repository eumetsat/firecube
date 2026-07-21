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

import pytest

from firecube.core.controlplane.types import SpanCoverage
from firecube.ingestor.types.planned_range import (
    PlannedRange,
    chunk_align_ranges,
    compute_covered_ranges,
)


def test_planned_range_valid() -> None:
    range_ = PlannedRange(group="g", slot_start=0, slot_end=100)
    assert range_.group == "g"
    assert range_.slot_start == 0
    assert range_.slot_end == 100


def test_planned_range_invalid_equal() -> None:
    with pytest.raises(ValueError, match="slot_start must be < slot_end"):
        PlannedRange(group="g", slot_start=10, slot_end=10)


def test_planned_range_inverted() -> None:
    with pytest.raises(ValueError, match="slot_start must be < slot_end"):
        PlannedRange(group="g", slot_start=11, slot_end=10)


def test_planned_range_negative_start() -> None:
    with pytest.raises(ValueError, match="slot_start must be >= 0"):
        PlannedRange(group="g", slot_start=-1, slot_end=10)


def test_compute_covered_ranges_inclusive_to_half_open() -> None:
    coverage = [SpanCoverage(group="g", arrays=[], time_index_ranges=[[0, 99]])]

    result = compute_covered_ranges(coverage)

    assert result == [PlannedRange(group="g", slot_start=0, slot_end=100)]


def test_compute_covered_ranges_empty() -> None:
    assert compute_covered_ranges([]) == []


def test_compute_covered_ranges_skips_missing_ranges() -> None:
    coverage = [SpanCoverage(group="g", arrays=[], time_index_ranges=None)]

    assert compute_covered_ranges(coverage) == []


def test_chunk_align_ranges_misaligned() -> None:
    result = chunk_align_ranges([PlannedRange(group="g", slot_start=5, slot_end=17)], 10)

    assert result == [PlannedRange(group="g", slot_start=0, slot_end=20)]


def test_chunk_align_ranges_already_aligned() -> None:
    range_ = PlannedRange(group="g", slot_start=0, slot_end=100)

    assert chunk_align_ranges([range_], 100) == [range_]
