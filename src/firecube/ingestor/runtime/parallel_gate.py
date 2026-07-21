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

from firecube.ingestor.errors import ConfigurationError

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

    This catches the class of bugs where global_expected_time_count() declares a group
    that the plugin's zarr_schema() does not actually create. Without this check, the
    per-group filter in _verify_schema_at_pod_startup silently no-ops and writes a
    false-success audit record for a nonexistent group.

    Phase 3.2 _process_batch already detected this via a DEBUG log (extras_in_global
    treated as sidecars), but deferred the audit-record damage. This validator catches
    it at capability-gate time, BEFORE any audit records are written.
    """
    schema_groups = {spec.group for spec in schema}
    phantom_groups = set(global_expected.keys()) - schema_groups
    if phantom_groups:
        raise ConfigurationError(
            f"global_expected_time_count() declares groups {sorted(phantom_groups)} "
            f"that are not in zarr_schema() (schema declares: {sorted(schema_groups)}). "
            f"Either add these groups to zarr_schema() or remove them from "
            f"global_expected_time_count()."
        )


def validate_parallel_capability(
    ingestor: BaseIngestor,
    slot_start: int | None,
    slot_end: int | None,
    ctx: PluginContext,
    slot_group: str | None = None,
) -> dict[str, int] | None:
    """Validate plugin supports slot-range parallelism.

    Returns cached global schema if applicable.

    Returns:
        dict[str, int] if parallel mode is active (global_expected_time_count() result)
        None if single-pod mode (no slot flags provided)

    Raises:
        ConfigurationError: if slot flags used with non-capable plugin, or capability is
            declared but validation fails.
    """
    if slot_start is None and slot_end is None:
        return None

    if slot_start is None or slot_end is None:
        raise ConfigurationError("--slot-start and --slot-end must be provided together")

    from firecube.ingestor.templates.direct_zarr import DirectZarrIngestor

    if not isinstance(ingestor, DirectZarrIngestor):
        raise ConfigurationError(
            f"Slot ranges are only supported for DirectZarrIngestor plugins. "
            f"Plugin {type(ingestor).__name__} is not a DirectZarrIngestor."
        )

    if not ingestor.SUPPORTS_SLOT_RANGE_PARALLELISM:
        raise ConfigurationError(
            f"Plugin {type(ingestor).__name__} has not opted into slot-range parallelism. "
            "Implement timestamp_to_ts_index(), global_expected_time_count(), "
            "and set SUPPORTS_SLOT_RANGE_PARALLELISM = True."
        )

    global_schema = ingestor.global_expected_time_count(ctx)
    if not global_schema:
        raise ConfigurationError(
            f"Plugin {type(ingestor).__name__}.global_expected_time_count() returned "
            f"{'None' if global_schema is None else 'empty dict'}. "
            "Must return a non-empty dict[str, int] with positive values when "
            "SUPPORTS_SLOT_RANGE_PARALLELISM=True."
        )
    for group, count in global_schema.items():
        if count <= 0:
            raise ConfigurationError(
                f"Plugin {type(ingestor).__name__}.global_expected_time_count() returned "
                f"non-positive count {count} for group '{group}'. All values must be positive."
            )

    if slot_start is not None and slot_end is not None:
        schema = ingestor.zarr_schema(ctx)
        validate_global_expected_subset_of_schema(global_schema, schema)
        chunk_shapes_per_group: dict[str, list[tuple[int, ...]]] = {}
        for group_spec in schema:
            shapes = [
                arr_spec.chunks
                for arr_spec in group_spec.arrays
                if arr_spec.chunks is not None and getattr(arr_spec, "time_indexed", True)
            ]
            if shapes:
                chunk_shapes_per_group[group_spec.group] = shapes

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

        if chunk_shapes_per_group:
            from firecube.ingestor.types.planned_range import (
                validate_chunk_alignment,
                warn_if_misaligned,
            )

            warn_if_misaligned(
                slot_start, slot_end, chunk_shapes_per_group, log, global_expected=global_schema
            )
            validate_chunk_alignment(
                slot_start,
                slot_end,
                chunk_shapes_per_group,
                global_expected=global_schema,
            )

    log.info(
        "Parallel capability validated: slot_range=[%s, %s), global_schema=%s",
        slot_start,
        slot_end,
        global_schema,
    )
    return global_schema
