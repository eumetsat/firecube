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

"""Time-index range arithmetic for control-plane coverage records."""

from __future__ import annotations

from typing import Any

__all__ = [
    "intersect_index_ranges",
    "merge_index_ranges",
    "normalize_index_ranges",
    "subtract_index_ranges",
]


def normalize_index_ranges(value: Any) -> list[list[int]]:
    ranges: list[list[int]] = []
    if not isinstance(value, list):
        return ranges
    for item in value:
        if not isinstance(item, (list, tuple)) or len(item) != 2:
            continue
        start, end = int(item[0]), int(item[1])
        if end < start:
            start, end = end, start
        ranges.append([start, end])
    return merge_index_ranges(ranges)


def merge_index_ranges(ranges: list[list[int]]) -> list[list[int]]:
    if not ranges:
        return []
    merged: list[list[int]] = []
    for start, end in sorted((int(start), int(end)) for start, end in ranges):
        if not merged or start > merged[-1][1] + 1:
            merged.append([start, end])
            continue
        merged[-1][1] = max(merged[-1][1], end)
    return merged


def subtract_index_ranges(
    ranges: list[list[int]],
    subtract: list[list[int]],
) -> list[list[int]]:
    remaining = merge_index_ranges(ranges)
    for cut_start, cut_end in merge_index_ranges(subtract):
        next_remaining: list[list[int]] = []
        for start, end in remaining:
            if cut_end < start or cut_start > end:
                next_remaining.append([start, end])
                continue
            if start < cut_start:
                next_remaining.append([start, cut_start - 1])
            if cut_end < end:
                next_remaining.append([cut_end + 1, end])
        remaining = next_remaining
    return remaining


def intersect_index_ranges(
    left: list[list[int]],
    right: list[list[int]],
) -> list[list[int]]:
    intersections: list[list[int]] = []
    for left_start, left_end in merge_index_ranges(left):
        for right_start, right_end in merge_index_ranges(right):
            start = max(left_start, right_start)
            end = min(left_end, right_end)
            if start <= end:
                intersections.append([start, end])
    return merge_index_ranges(intersections)
