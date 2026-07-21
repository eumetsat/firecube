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

"""Slot-range types and helpers for parallel ingestion (Phase 3)."""

from __future__ import annotations

import logging
import math
from dataclasses import dataclass
from typing import TYPE_CHECKING

from firecube.ingestor.errors import ConfigurationError

if TYPE_CHECKING:
    from firecube.core.controlplane.types import SpanCoverage


# Half-open range type alias: (slot_start, slot_end) where slot_start is inclusive, slot_end exclusive.
SlotRange = tuple[int, int]


@dataclass(frozen=True)
class PlannedRange:
    """A half-open slot range [slot_start, slot_end) for one Zarr group.

    Uses HALF-OPEN semantics: slot_start is inclusive, slot_end is exclusive.
    This matches Python slicing convention and engine array indexing.
    """

    group: str
    slot_start: int
    slot_end: int

    def __post_init__(self) -> None:
        validate_slot_range(self.slot_start, self.slot_end)


def validate_slot_range(slot_start: int, slot_end: int) -> None:
    """Validate that ``[slot_start, slot_end)`` is well-formed and non-negative."""
    if slot_start < 0:
        raise ValueError(f"slot_start must be >= 0, got {slot_start}")
    if slot_end < 0:
        raise ValueError(f"slot_end must be >= 0, got {slot_end}")
    if slot_start >= slot_end:
        raise ValueError(f"slot_start must be < slot_end, got [{slot_start}, {slot_end})")


def compute_covered_ranges(coverage: list[SpanCoverage]) -> list[PlannedRange]:
    """Convert inclusive coverage ranges into half-open ``PlannedRange`` values."""
    ranges: list[PlannedRange] = []
    for span in coverage:
        if not span.time_index_ranges:
            continue
        for slot_start, slot_end in span.time_index_ranges:
            ranges.append(
                PlannedRange(group=span.group, slot_start=slot_start, slot_end=slot_end + 1)
            )
    return ranges


def chunk_align_ranges(ranges: list[PlannedRange], chunk_size: int) -> list[PlannedRange]:
    """Expand ranges to chunk boundaries, then deduplicate and sort them."""
    if chunk_size <= 0:
        raise ValueError(f"chunk_size must be > 0, got {chunk_size}")

    aligned: dict[tuple[str, int, int], PlannedRange] = {}
    for range_ in ranges:
        aligned_start = (range_.slot_start // chunk_size) * chunk_size
        aligned_end = math.ceil(range_.slot_end / chunk_size) * chunk_size
        aligned_range = PlannedRange(
            group=range_.group, slot_start=aligned_start, slot_end=aligned_end
        )
        aligned[(aligned_range.group, aligned_range.slot_start, aligned_range.slot_end)] = (
            aligned_range
        )

    return sorted(aligned.values(), key=lambda r: (r.group, r.slot_start, r.slot_end))


def _chunk_alignment_message(
    slot_start: int,
    slot_end: int,
    group: str,
    chunk_shapes: list[tuple[int, ...]],
) -> str:
    chunk_sizes = [shape[0] for shape in chunk_shapes]
    alignment = math.lcm(*chunk_sizes)
    aligned_start = (slot_start // alignment) * alignment
    aligned_end = math.ceil(slot_end / alignment) * alignment
    return (
        f"Group '{group}' chunk_shapes={chunk_shapes}; slot range [{slot_start}, {slot_end}) misaligned. "
        f"Suggested aligned range: [{aligned_start}, {aligned_end})"
    )


def validate_chunk_alignment(
    slot_start: int,
    slot_end: int,
    chunk_shapes_per_group: dict[str, list[tuple[int, ...]]],
    global_expected: dict[str, int] | None = None,
) -> None:
    """Raise ``ConfigurationError`` when any group's slot range is misaligned.

    Terminal partial chunks are allowed when ``slot_end`` matches the group's
    global expected total.
    """
    for group, chunk_shapes in chunk_shapes_per_group.items():
        group_total = (global_expected or {}).get(group)
        misaligned_shapes = [
            shape
            for shape in chunk_shapes
            if slot_start % shape[0] != 0 or (slot_end % shape[0] != 0 and slot_end != group_total)
        ]
        if misaligned_shapes:
            raise ConfigurationError(
                _chunk_alignment_message(slot_start, slot_end, group, misaligned_shapes)
            )


def warn_if_misaligned(
    slot_start: int,
    slot_end: int,
    chunk_shapes_per_group: dict[str, list[tuple[int, ...]]],
    logger: logging.Logger,
    global_expected: dict[str, int] | None = None,
) -> None:
    """Log a warning for each group whose slot range is not chunk-aligned.

    Terminal partial chunks are allowed when ``slot_end`` matches the group's
    global expected total.
    """
    for group, chunk_shapes in chunk_shapes_per_group.items():
        group_total = (global_expected or {}).get(group)
        misaligned_shapes = [
            shape
            for shape in chunk_shapes
            if slot_start % shape[0] != 0 or (slot_end % shape[0] != 0 and slot_end != group_total)
        ]
        if misaligned_shapes:
            logger.warning(_chunk_alignment_message(slot_start, slot_end, group, misaligned_shapes))
