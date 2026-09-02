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

"""Reusable slot-planning helpers for firecube zarr slots and parallel ingestion.

Extracted from plan.py to be importable by firecube zarr slots without
coupling to the legacy plan command.
"""

from __future__ import annotations

import math
from collections.abc import Sequence

import click

from firecube.cli._ctx import get_storage_config
from firecube.cli._product import resolve_product_identity
from firecube.core.controlplane import ChunkManager
from firecube.core.storage.binding import StorageBinding
from firecube.core.storage.driver_config import StorageDriverConfig
from firecube.ingestor.api import ZarrGroupSpec

#: Plan JSON schema version. Independent of control-plane SCHEMA_VERSION.
PLAN_SCHEMA_VERSION = "v1"


def _resolve_per_group_slot_sizes(
    schema: Sequence[ZarrGroupSpec],
    explicit: int | None,
) -> dict[str, int]:
    if explicit is not None:
        if explicit <= 0:
            raise click.ClickException(f"--slot-size must be > 0, got {explicit}")
        explicit = int(explicit)

    per_group: dict[str, int] = {}
    for group_spec in schema:
        time_chunks = [
            int((arr_spec.shards if arr_spec.shards is not None else arr_spec.chunks)[0])
            for arr_spec in group_spec.arrays
            if arr_spec.time_indexed and arr_spec.chunks is not None and arr_spec.chunks
        ]
        if not time_chunks:
            continue
        per_group[group_spec.group] = math.lcm(*time_chunks)

    if explicit is not None:
        invalid = {g: req for g, req in per_group.items() if explicit % req != 0}
        if invalid:
            details = "; ".join(
                f"group '{g}' needs multiple of {req}" for g, req in sorted(invalid.items())
            )
            raise click.ClickException(f"--slot-size={explicit} not valid: {details}")
        return dict.fromkeys(per_group, explicit)
    return per_group


def _chunk_aligned_remaining(
    remaining: list[tuple[int, int]], slot_size: int, total: int
) -> tuple[list[tuple[int, int]], list[tuple[int, int]]]:
    """Split each ``remaining`` interval into chunk-aligned + blocked sub-intervals.

    Returns ``(aligned, blocked)`` where:
    - ``aligned`` is the executable list of chunk-aligned intervals (Phase 3.4 behavior preserved)
    - ``blocked`` is the list of sub-intervals that cannot produce any aligned executable range:
      * pre-alignment prefix when ``start`` is misaligned (e.g. ``[(73, 1000)]`` slot=100 →
        aligned=[(100, 1000)], blocked=[(73, 100)])
      * fully-dropped interval when ``aligned_start >= clamped_end`` (e.g. ``[(950, 1000)]``
        slot=100 total=1000 → aligned=[], blocked=[(950, 1000)])

    Callers MUST treat non-empty ``blocked`` as a fail-closed condition. Phase 3.5 introduced
    this signature change to close the silent-drop bug surfaced by external review.
    """
    aligned: list[tuple[int, int]] = []
    blocked: list[tuple[int, int]] = []
    for start, end in remaining:
        if start % slot_size != 0:
            aligned_start = ((start // slot_size) + 1) * slot_size
        else:
            aligned_start = start
        clamped_end = min(end, total)
        if aligned_start < clamped_end:
            aligned.append((aligned_start, clamped_end))
            if aligned_start > start:
                blocked.append((start, aligned_start))
        else:
            blocked.append((start, clamped_end))
    return aligned, blocked


def _query_slots_coverage(
    ctx: click.Context,
    *,
    target: str,
    product_name: str,
    storage_type: str,
    storage_driver: str,
    groups: list[str],
) -> dict[str, list[tuple[int, int]]]:
    """Read covered time ranges per group from ChunkManager."""
    coverage: dict[str, list[tuple[int, int]]] = {g: [] for g in groups}
    identity = resolve_product_identity(
        target, format="zarr", product_name=product_name, option_name="--target"
    )
    storage_config = get_storage_config(
        ctx,
        overrides={
            "storage_type": storage_type,
            "storage_driver": storage_driver,
        },
        cache=False,
    )
    driver = StorageDriverConfig.from_storage_config(storage_config)
    binding = StorageBinding(identity=identity, driver=driver)
    manager = ChunkManager(binding=binding)
    try:
        chunks = manager.list_chunks(
            product=product_name,
            chunk_type="span",
            include_replaced=False,
        )
    finally:
        manager.close()

    for chunk in chunks:
        meta = chunk.meta or {}
        group = meta.get("group")
        if group not in coverage:
            continue
        span_payload = chunk.record.get("span") if isinstance(chunk.record, dict) else None
        if not isinstance(span_payload, dict):
            continue
        time_index_ranges = span_payload.get("time_index_ranges") or []
        for entry in time_index_ranges:
            if not isinstance(entry, (list, tuple)) or len(entry) != 2:
                continue
            try:
                start = int(entry[0])
                end_inclusive = int(entry[1])
            except (TypeError, ValueError):
                continue
            coverage[group].append((start, end_inclusive + 1))

    return coverage


def _merge_intervals(intervals: list[tuple[int, int]]) -> list[tuple[int, int]]:
    if not intervals:
        return []
    sorted_iv = sorted(intervals)
    merged: list[tuple[int, int]] = [sorted_iv[0]]
    for start, end in sorted_iv[1:]:
        last_start, last_end = merged[-1]
        if start <= last_end:
            merged[-1] = (last_start, max(last_end, end))
        else:
            merged.append((start, end))
    return merged


def _complement_intervals(covered: list[tuple[int, int]], total: int) -> list[tuple[int, int]]:
    if total <= 0:
        return []
    result: list[tuple[int, int]] = []
    cursor = 0
    for start, end in covered:
        clamped_start = max(0, min(start, total))
        clamped_end = max(0, min(end, total))
        if clamped_start > cursor:
            result.append((cursor, clamped_start))
        cursor = max(cursor, clamped_end)
        if cursor >= total:
            break
    if cursor < total:
        result.append((cursor, total))
    return result


def _partition_remaining(remaining: list[tuple[int, int]], slot_size: int) -> list[tuple[int, int]]:
    """Partition each remaining interval into chunk-aligned slot_size chunks."""
    partitions: list[tuple[int, int]] = []
    for interval_start, interval_end in remaining:
        cursor = interval_start
        while cursor < interval_end:
            next_cursor = min(cursor + slot_size, interval_end)
            partitions.append((cursor, next_cursor))
            cursor = next_cursor
    return partitions
