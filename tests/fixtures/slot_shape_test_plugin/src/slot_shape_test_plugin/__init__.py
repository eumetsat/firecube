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

"""Shape-only fixture plugins for slot-index model integration tests.

Two ingestors with deliberately different slot-index shapes:

* ``FixedEpochShapeIngestor`` — four nested groups, uniform floor cadence,
  hardcoded epoch.
* ``OptionEpochShapeIngestor`` — five groups with mixed exact cadences,
  epoch read from ``ctx.options["reference_epoch"]`` and normalized.

Neither implements write intents; they exist only to feed
``plugin.slot_index_model(ctx)`` into ``ChunkManager.ensure_slot_index_model``.
"""

from __future__ import annotations

from abc import abstractmethod
from collections.abc import Iterable, Sequence
from typing import Any, ClassVar

from firecube.core.api import SlotAxis, SlotIndexModel, normalize_epoch_iso
from firecube.ingestor.api import (
    DirectZarrIngestor,
    PipelineBatch,
    PluginContext,
    WriteIntent,
    ZarrArraySpec,
    ZarrGroupSpec,
    register_ingestor,
)

DEFAULT_REFERENCE_EPOCH = "2024-01-01T00:00:00Z"

_FIXED_EPOCH_GROUPS: dict[str, SlotAxis] = {
    "groupA/data_hi": SlotAxis(cadence_s=600, mode="floor"),
    "groupA/data_lo": SlotAxis(cadence_s=600, mode="floor"),
    "groupB/data_hi": SlotAxis(cadence_s=600, mode="floor"),
    "groupB/data_lo": SlotAxis(cadence_s=600, mode="floor"),
}

_OPTION_EPOCH_GROUPS: dict[str, SlotAxis] = {
    "fast_a/data": SlotAxis(cadence_s=300, mode="exact"),
    "fast_b/data": SlotAxis(cadence_s=300, mode="exact"),
    "slow_a/data": SlotAxis(cadence_s=900, mode="exact"),
    "slow_b/data": SlotAxis(cadence_s=900, mode="exact"),
    "slow_c/data": SlotAxis(cadence_s=900, mode="exact"),
}


class _ShapeOnlyIngestor(DirectZarrIngestor):
    SUPPORTS_SLOT_RANGE_PARALLELISM: ClassVar[bool] = True

    _groups: ClassVar[dict[str, SlotAxis]]

    @abstractmethod
    def slot_index_model(self, ctx: PluginContext) -> SlotIndexModel: ...

    def discover_source_files(self, ctx: PluginContext) -> Iterable[Any]:
        return []

    def timestamp_to_ts_index(self, group: str, timestamp_val: Any) -> int:
        return int(timestamp_val)

    def global_expected_time_count(self, ctx: PluginContext) -> dict[str, int]:
        return dict.fromkeys(self._groups, 0)

    def filter_items_to_slot_range(
        self,
        items: Sequence[Any],
        slot_start: int,
        slot_end: int,
        ctx: PluginContext,
    ) -> Sequence[Any]:
        return [it for it in items if slot_start <= int(it) < slot_end]

    def zarr_schema(self, ctx: PluginContext) -> list[ZarrGroupSpec]:
        groups: list[ZarrGroupSpec] = []
        for group_path in self._groups:
            arr = ZarrArraySpec(
                name="data",
                chunks=(1, 8),
                shape=(1, 8),
                dtype="float32",
                dimension_names=("timestamp", "x"),
                attrs={"long_name": f"{group_path} data", "units": "1"},
            )
            groups.append(ZarrGroupSpec(group=group_path, arrays=[arr]))
        return groups

    def build_write_intents(self, batch: PipelineBatch, ctx: PluginContext) -> list[WriteIntent]:
        raise NotImplementedError(
            "slot_shape_test_plugin ingestors are shape-only and do not implement write intents"
        )


@register_ingestor("fixed_epoch_shape_plugin")
class FixedEpochShapeIngestor(_ShapeOnlyIngestor):
    PRODUCT_NAME: ClassVar[str] = "fixed_epoch_shape"

    _groups: ClassVar[dict[str, SlotAxis]] = _FIXED_EPOCH_GROUPS

    def slot_index_model(self, ctx: PluginContext) -> SlotIndexModel:
        return SlotIndexModel(
            name="fixed_epoch_shape_v1",
            epoch="2024-09-24T00:00:00Z",
            groups=dict(self._groups),
        )


@register_ingestor("option_epoch_shape_plugin")
class OptionEpochShapeIngestor(_ShapeOnlyIngestor):
    PRODUCT_NAME: ClassVar[str] = "option_epoch_shape"

    _groups: ClassVar[dict[str, SlotAxis]] = _OPTION_EPOCH_GROUPS

    def slot_index_model(self, ctx: PluginContext) -> SlotIndexModel:
        raw_epoch = str(ctx.options.get("reference_epoch", DEFAULT_REFERENCE_EPOCH))
        return SlotIndexModel(
            name="option_epoch_shape_v1",
            epoch=normalize_epoch_iso(raw_epoch),
            groups=dict(self._groups),
        )
