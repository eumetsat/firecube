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

"""Safe parallel writes to one Zarr store.

Zarr leaves concurrent writers to the caller: a slot write reads, changes and
writes back a whole stored unit (the shard if sharded, else the chunk), and
``zarr.json`` is one document per node. This layer gives every writer one way
to do that without losing another writer's work: chunk geometry and layout,
unit locks and ownership checks, the ``guarded_write`` chokepoint, the metadata
authority, engine-owned arrays and the promotion plan. The modules take a zarr
root, array paths and index boxes; none of them knows a template.
``DirectZarrIngestor`` is the first writer on it.

Modules:
- ``chunk_geometry``: stored-unit geometry and the operator refusal messages.
- ``_layout``: the chunks and shards an array is created with (internal).
- ``_unit_locks``: the process-wide lock table per stored unit (internal).
- ``unit_ownership``: window, staged-ledger and promotion unit checks.
- ``guarded_write``: the chokepoint for payload writes (the function is not re-exported here).
- ``metadata_authority``: the only path that changes ``zarr.json``.
- ``engine_owned``: engine-owned per-slot arrays, collection and verification.
- ``promotion_plan``: the pure planner for a staged promotion delta.
- ``unit_buffer``: assembles whole stored units from pieces and writes each once (not re-exported here).
"""

from firecube.core.zarr.guard.chunk_geometry import (
    Bounds,
    axis_selection_is_chunk_aligned,
    bounds_cover,
    chunk_axis_range,
    chunk_index_to_region,
    incompletely_written_units,
    physical_chunk_keys_for_region,
    staged_partial_unit_refusal,
    unit_bounds,
    unit_ownership_refusal,
    units_touched,
    window_straddling_units,
    write_unit_shape,
)
from firecube.core.zarr.guard.engine_owned import (
    collect_engine_owned_values,
    engine_owned_adoption_error,
    engine_owned_creation_attrs,
    engine_owned_names_by_group,
    is_engine_owned_reserved,
    is_engine_owned_spec,
    is_pod_written_slot_array,
    materialize_engine_owned_array,
    require_engine_owned_materialized,
    require_engine_owned_ownership,
    require_engine_owned_slots,
    verify_engine_owned_writes,
)
from firecube.core.zarr.guard.guarded_write import (
    ArrayHold,
    Unit,
    WriteResult,
    array_with_write_empty_chunks,
    current_hold,
    hold_array,
    selection_box,
    unit_store_key,
)
from firecube.core.zarr.guard.metadata_authority import (
    AuthorityBasis,
    HeldClaim,
    MetadataAuthority,
    MetadataAuthorityError,
    MetadataScope,
    adopt_held_claim,
    create_or_open_array,
    ensure_group,
    hold_authority,
    publish_document,
    publish_document_if_absent,
    set_attributes,
    update_document,
)
from firecube.core.zarr.guard.promotion_plan import (
    PlannedObject,
    PromotionAction,
    PromotionPlan,
    build_promotion_plan,
    list_workspace_objects,
    unclaimed_data_objects,
)
from firecube.core.zarr.guard.unit_ownership import (
    StagedSlotWrite,
    WriteFootprint,
    require_planned_slots_fill_units,
    require_staged_units_fully_written,
    require_target_within_clipped_units,
    require_window_owns_stored_units,
    staged_unit_object_keys,
    stored_write_unit,
)

__all__ = [
    "ArrayHold",
    "AuthorityBasis",
    "Bounds",
    "HeldClaim",
    "MetadataAuthority",
    "MetadataAuthorityError",
    "MetadataScope",
    "PlannedObject",
    "PromotionAction",
    "PromotionPlan",
    "StagedSlotWrite",
    "Unit",
    "WriteFootprint",
    "WriteResult",
    "adopt_held_claim",
    "array_with_write_empty_chunks",
    "axis_selection_is_chunk_aligned",
    "bounds_cover",
    "build_promotion_plan",
    "chunk_axis_range",
    "chunk_index_to_region",
    "collect_engine_owned_values",
    "create_or_open_array",
    "current_hold",
    "engine_owned_adoption_error",
    "engine_owned_creation_attrs",
    "engine_owned_names_by_group",
    "ensure_group",
    "hold_array",
    "hold_authority",
    "incompletely_written_units",
    "is_engine_owned_reserved",
    "is_engine_owned_spec",
    "is_pod_written_slot_array",
    "list_workspace_objects",
    "materialize_engine_owned_array",
    "physical_chunk_keys_for_region",
    "publish_document",
    "publish_document_if_absent",
    "require_engine_owned_materialized",
    "require_engine_owned_ownership",
    "require_engine_owned_slots",
    "require_planned_slots_fill_units",
    "require_staged_units_fully_written",
    "require_target_within_clipped_units",
    "require_window_owns_stored_units",
    "selection_box",
    "set_attributes",
    "staged_partial_unit_refusal",
    "staged_unit_object_keys",
    "stored_write_unit",
    "unclaimed_data_objects",
    "unit_bounds",
    "unit_ownership_refusal",
    "unit_store_key",
    "units_touched",
    "update_document",
    "verify_engine_owned_writes",
    "window_straddling_units",
    "write_unit_shape",
]
