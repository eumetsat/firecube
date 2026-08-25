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

"""Capability gate for slot-range parallel ingestion (Phase 3)."""

from __future__ import annotations

import logging
from collections.abc import Sequence
from typing import TYPE_CHECKING

from firecube.core.index_resolve import ExtentUnknownError
from firecube.ingestor.errors import (
    ConfigurationError,
    UnboundedAxisError,
)
from firecube.ingestor.runtime.index_binding import IndexBinding
from firecube.ingestor.templates.direct_zarr import DirectZarrIngestor

if TYPE_CHECKING:
    from firecube.ingestor.runtime.base import BaseIngestor
    from firecube.ingestor.templates.direct_zarr import ZarrGroupSpec
    from firecube.ingestor.types.context import PluginContext

log = logging.getLogger(__name__)


def validate_global_expected_subset_of_schema(
    global_expected: dict[str, int],
    schema: Sequence[ZarrGroupSpec],
) -> None:
    """Raise ConfigurationError if global_expected contains a group absent from zarr_schema().

    This catches the class of bugs where the caller-supplied ``global_expected``
    mapping declares a group that the plugin's zarr_schema() does not actually
    create. Without this check, the per-group filter in
    ``_verify_schema_at_pod_startup`` silently no-ops and writes a
    false-success audit record for a nonexistent group.

    This validator runs at capability-gate time, BEFORE any audit records are
    written; a mismatch between the expected-time mapping and the schema
    surfaces as a clean ``ConfigurationError`` rather than as a downstream
    audit-log discrepancy.
    """
    schema_groups = {spec.group for spec in schema}
    phantom_groups = set(global_expected.keys()) - schema_groups
    if phantom_groups:
        raise ConfigurationError(
            f"Expected-time mapping declares groups {sorted(phantom_groups)} "
            f"that are not in zarr_schema() (schema declares: {sorted(schema_groups)}). "
            f"Either add these groups to zarr_schema() or remove them from "
            f"the expected-time mapping."
        )


def warn_on_chunk_alignment(
    global_expected: dict[str, int],
    schema: Sequence[ZarrGroupSpec],
    logger: logging.Logger = log,
) -> None:
    """Warn when a time-indexed array's chunk size does not divide the expected count."""
    for group_spec in schema:
        group_name = group_spec.group
        expected = global_expected.get(group_name)
        if expected is None:
            continue
        for arr_spec in group_spec.arrays:
            if not arr_spec.time_indexed:
                continue
            alignment_shape = arr_spec.shards if arr_spec.shards is not None else arr_spec.chunks
            if alignment_shape and alignment_shape[0] > 0 and expected % alignment_shape[0] != 0:
                logger.warning(
                    "Group %r array %r: expected time count %d is not a multiple of "
                    "time-alignment size %d; the final alignment unit will be partially filled.",
                    group_name,
                    arr_spec.name,
                    expected,
                    alignment_shape[0],
                )


def validate_parallel_capability(
    ingestor: BaseIngestor,
    slot_start: int | None,
    slot_end: int | None,
    ctx: PluginContext,
    slot_group: str | None = None,
) -> IndexBinding | None:
    """Validate plugin supports slot-range parallelism.

    Returns the resolved index binding if applicable.

    Returns:
        IndexBinding if parallel mode is active
        None if single-pod mode (no slot flags provided)

    Raises:
        ConfigurationError: if slot flags used with non-capable plugin, or capability is
            declared but validation fails.
    """
    if slot_start is None and slot_end is None:
        return None

    if slot_start is None or slot_end is None:
        raise ConfigurationError("--slot-start and --slot-end must be provided together")

    if not isinstance(ingestor, DirectZarrIngestor):
        raise ConfigurationError(
            f"Slot ranges are only supported for DirectZarrIngestor plugins. "
            f"Plugin {type(ingestor).__name__} is not a DirectZarrIngestor."
        )

    if ingestor.index_spec(ctx) is None:
        raise ConfigurationError(
            "--slot-start/--slot-end require index_spec(); the plugin returned None"
        )

    if not hasattr(ingestor, "_index_binding"):
        ingestor._bind_index_at_startup(ctx)

    binding = getattr(ingestor, "_index_binding", None)
    if binding is None:
        raise ConfigurationError(
            "--slot-start/--slot-end require index_spec(); the plugin returned None"
        )

    for group in binding.resolved.groups:
        try:
            binding.resolved.size(group)
        except ExtentUnknownError as exc:
            raise UnboundedAxisError(group) from exc

    if type(ingestor).inspect_item is DirectZarrIngestor.inspect_item:
        raise ConfigurationError("--slot-start/--slot-end require inspect_item() override")

    global_expected = {group: binding.resolved.size(group) for group in binding.resolved.groups}

    if slot_start is not None and slot_end is not None:
        schema = ingestor.zarr_schema(ctx)
        if schema:
            validate_global_expected_subset_of_schema(global_expected, schema)
            warn_on_chunk_alignment(global_expected, schema)
        chunk_shapes_per_group: dict[str, list[tuple[int, ...]]] = {}
        shard_shapes_per_group: dict[str, list[tuple[int, ...]]] = {}
        for group_spec in schema:
            shapes = [
                arr_spec.chunks
                for arr_spec in group_spec.arrays
                if arr_spec.chunks is not None and arr_spec.time_indexed
            ]
            shard_shapes = [
                arr_spec.shards
                for arr_spec in group_spec.arrays
                if arr_spec.shards is not None and arr_spec.time_indexed
            ]
            if shapes:
                chunk_shapes_per_group[group_spec.group] = shapes
            if shard_shapes:
                shard_shapes_per_group[group_spec.group] = shard_shapes

        if slot_group is not None:
            schema_groups = {spec.group for spec in schema}
            if slot_group not in schema_groups:
                raise ConfigurationError(
                    f"--slot-group {slot_group!r} is not a group in "
                    f"{type(ingestor).__name__}.zarr_schema() "
                    f"(available: {sorted(schema_groups)}). "
                    "Check the group name or omit --slot-group to target all groups."
                )
            chunk_shapes_per_group = {
                group: shapes
                for group, shapes in chunk_shapes_per_group.items()
                if group == slot_group
            }
            shard_shapes_per_group = {
                group: shapes
                for group, shapes in shard_shapes_per_group.items()
                if group == slot_group
            }

        if chunk_shapes_per_group:
            from firecube.ingestor.types.planned_range import (
                validate_chunk_alignment,
                warn_if_misaligned,
            )

            warn_if_misaligned(
                slot_start,
                slot_end,
                chunk_shapes_per_group,
                log,
                global_expected=global_expected,
                shards_per_group=shard_shapes_per_group,
            )
            validate_chunk_alignment(
                slot_start,
                slot_end,
                chunk_shapes_per_group,
                global_expected=global_expected,
                shards_per_group=shard_shapes_per_group,
            )

    log.info(
        "Parallel capability validated: slot_range=[%s, %s), global_expected=%s",
        slot_start,
        slot_end,
        global_expected,
    )
    return binding
