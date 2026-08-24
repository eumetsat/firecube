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

"""Test fixture plugins for IrregularTimeAxis AUTO discovery.

Six ingestor variants covering safe operation and all failure modes exercised by
the A7-A12 integration tests for IrregularTimeAxis(coordinate=..., values=AUTO):

* ``IrregularAxisSafeIngestor`` -- happy path, five monotonically-ordered items.
* ``IrregularAxisReverseOrderIngestor`` -- same coordinates in reverse insertion
  order, verifies sort determinism.
* ``IrregularAxisDuplicateIngestor`` -- two items claim the same coordinate,
  triggers duplicate rejection during discovery.
* ``IrregularAxisEmptyIngestor`` -- zero items reach the discovery hook,
  triggers ``NoDiscoveredItemsError``.
* ``IrregularAxisMissingCoordIngestor`` -- one item's ``inspect_item`` returns
  ``ItemInfo(coordinate=None)``, triggers
  ``MissingIrregularCoordinateError``.
* ``IrregularAxisConcreteIngestor`` -- same five coordinates supplied as a
  concrete tuple instead of ``AUTO``, used for concrete-vs-AUTO equivalence
  parity tests.

Fixtures use pure in-memory integer items; no network I/O or private data is
required.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any, ClassVar

import numpy as np

from firecube.core.api import AUTO, IndexSpec, IrregularTimeAxis, ItemInfo
from firecube.ingestor.api import (
    DirectZarrIngestor,
    PipelineBatch,
    PluginContext,
    WriteIntent,
    ZarrArraySpec,
    ZarrGroupSpec,
    register_ingestor,
)

# Canonical fixture dataset: five items at 2026-01-01T00:00:00Z + i*600s
# (i in 0..4). All ingestors share this set so concrete-vs-AUTO parity tests
# compare like with like.
_BASE_TIMESTAMP: np.datetime64 = np.datetime64("2026-01-01T00:00:00", "ns")
_CADENCE_S: int = 600
_ITEM_COUNT: int = 5


def _canonical_timestamps() -> tuple[np.datetime64, ...]:
    """Return the five monotonically-ordered coordinate values as ``datetime64[ns]``."""
    step = np.timedelta64(_CADENCE_S, "s").astype("timedelta64[ns]")
    return tuple(_BASE_TIMESTAMP + i * step for i in range(_ITEM_COUNT))


def _build_zarr_schema(slot_count: int, group: str = "data") -> list[ZarrGroupSpec]:
    """Small (slot_count, 3, 4) float32 payload shell for all irregular-axis fixtures."""
    return [
        ZarrGroupSpec(
            group=group,
            arrays=[
                ZarrArraySpec(
                    name="values",
                    shape=(slot_count, 3, 4),
                    dtype="float32",
                    chunks=(1, 3, 4),
                    fill_value=0.0,
                    expected_time_count=slot_count,
                    time_indexed=True,
                    dimension_names=("timestamp", "y", "x"),
                )
            ],
        )
    ]


@register_ingestor("irregular_axis_safe")
class IrregularAxisSafeIngestor(DirectZarrIngestor):
    """Happy-path fixture: 5 items with monotonically-ordered timestamps and AUTO discovery."""

    PRODUCT_NAME: ClassVar[str] = "irregular_axis_safe"

    def discover_source_files(self, ctx: PluginContext) -> Iterable[Any]:
        return list(range(_ITEM_COUNT))

    def index_spec(self, ctx: PluginContext) -> IndexSpec:
        return IndexSpec(
            name="irregular_axis_safe_v1",
            groups={
                "data": IrregularTimeAxis(coordinate="timestamp", values=AUTO),
            },
        )

    def inspect_item(self, item: Any, ctx: PluginContext) -> ItemInfo | None:
        if not isinstance(item, int):
            return None
        return ItemInfo(coordinate=_canonical_timestamps()[item])

    def zarr_schema(self, ctx: PluginContext) -> list[ZarrGroupSpec]:
        return _build_zarr_schema(_ITEM_COUNT)

    def build_write_intents(self, batch: PipelineBatch, ctx: PluginContext) -> list[WriteIntent]:
        intents: list[WriteIntent] = []
        resolved = self.resolved_index(ctx)
        for item in batch.items:
            info = self.inspect_item(item, ctx)
            if info is None:
                continue
            slot = resolved.position("data", info.coordinate)
            intents.append(WriteIntent.slot(group="data", array="values", index=slot, data=None))
        return intents


@register_ingestor("irregular_axis_reverse_order")
class IrregularAxisReverseOrderIngestor(DirectZarrIngestor):
    """Sort-determinism fixture: 5 items presented in reverse (index 4..0) with AUTO discovery."""

    PRODUCT_NAME: ClassVar[str] = "irregular_axis_reverse_order"

    def discover_source_files(self, ctx: PluginContext) -> Iterable[Any]:
        return list(reversed(range(_ITEM_COUNT)))

    def index_spec(self, ctx: PluginContext) -> IndexSpec:
        return IndexSpec(
            name="irregular_axis_reverse_order_v1",
            groups={
                "data": IrregularTimeAxis(coordinate="timestamp", values=AUTO),
            },
        )

    def inspect_item(self, item: Any, ctx: PluginContext) -> ItemInfo | None:
        if not isinstance(item, int):
            return None
        return ItemInfo(coordinate=_canonical_timestamps()[item])

    def zarr_schema(self, ctx: PluginContext) -> list[ZarrGroupSpec]:
        return _build_zarr_schema(_ITEM_COUNT)

    def build_write_intents(self, batch: PipelineBatch, ctx: PluginContext) -> list[WriteIntent]:
        intents: list[WriteIntent] = []
        resolved = self.resolved_index(ctx)
        for item in batch.items:
            info = self.inspect_item(item, ctx)
            if info is None:
                continue
            slot = resolved.position("data", info.coordinate)
            intents.append(WriteIntent.slot(group="data", array="values", index=slot, data=None))
        return intents


@register_ingestor("irregular_axis_duplicate")
class IrregularAxisDuplicateIngestor(DirectZarrIngestor):
    """Duplicate-rejection fixture: item 1 reuses item 0's coordinate under AUTO discovery."""

    PRODUCT_NAME: ClassVar[str] = "irregular_axis_duplicate"

    def discover_source_files(self, ctx: PluginContext) -> Iterable[Any]:
        return [0, 0, 2, 3, 4]

    def index_spec(self, ctx: PluginContext) -> IndexSpec:
        return IndexSpec(
            name="irregular_axis_duplicate_v1",
            groups={
                "data": IrregularTimeAxis(coordinate="timestamp", values=AUTO),
            },
        )

    def inspect_item(self, item: Any, ctx: PluginContext) -> ItemInfo | None:
        if not isinstance(item, int):
            return None
        return ItemInfo(coordinate=_canonical_timestamps()[item])

    def zarr_schema(self, ctx: PluginContext) -> list[ZarrGroupSpec]:
        return _build_zarr_schema(_ITEM_COUNT)

    def build_write_intents(self, batch: PipelineBatch, ctx: PluginContext) -> list[WriteIntent]:
        intents: list[WriteIntent] = []
        resolved = self.resolved_index(ctx)
        for item in batch.items:
            info = self.inspect_item(item, ctx)
            if info is None:
                continue
            slot = resolved.position("data", info.coordinate)
            intents.append(WriteIntent.slot(group="data", array="values", index=slot, data=None))
        return intents


@register_ingestor("irregular_axis_empty")
class IrregularAxisEmptyIngestor(DirectZarrIngestor):
    """Empty-source fixture: zero items reach discovery, exercises ``NoDiscoveredItemsError``."""

    PRODUCT_NAME: ClassVar[str] = "irregular_axis_empty"

    def discover_source_files(self, ctx: PluginContext) -> Iterable[Any]:
        return []

    def index_spec(self, ctx: PluginContext) -> IndexSpec:
        return IndexSpec(
            name="irregular_axis_empty_v1",
            groups={
                "data": IrregularTimeAxis(coordinate="timestamp", values=AUTO),
            },
        )

    def inspect_item(self, item: Any, ctx: PluginContext) -> ItemInfo | None:
        return None

    def zarr_schema(self, ctx: PluginContext) -> list[ZarrGroupSpec]:
        # Schema declares one placeholder slot; the empty-source path fails before write.
        return _build_zarr_schema(1)

    def build_write_intents(self, batch: PipelineBatch, ctx: PluginContext) -> list[WriteIntent]:
        return []


@register_ingestor("irregular_axis_missing_coord")
class IrregularAxisMissingCoordIngestor(DirectZarrIngestor):
    """Missing-coordinate fixture: item 2 returns ``ItemInfo(coordinate=None)`` under AUTO.

    Exercises the ``MissingIrregularCoordinateError`` path when a plugin fails
    to resolve a coordinate for a discovered item.
    """

    PRODUCT_NAME: ClassVar[str] = "irregular_axis_missing_coord"
    _MISSING_INDEX: ClassVar[int] = 2

    def discover_source_files(self, ctx: PluginContext) -> Iterable[Any]:
        return list(range(_ITEM_COUNT))

    def index_spec(self, ctx: PluginContext) -> IndexSpec:
        return IndexSpec(
            name="irregular_axis_missing_coord_v1",
            groups={
                "data": IrregularTimeAxis(coordinate="timestamp", values=AUTO),
            },
        )

    def inspect_item(self, item: Any, ctx: PluginContext) -> ItemInfo | None:
        if not isinstance(item, int):
            return None
        if item == self._MISSING_INDEX:
            return ItemInfo(coordinate=None)
        return ItemInfo(coordinate=_canonical_timestamps()[item])

    def zarr_schema(self, ctx: PluginContext) -> list[ZarrGroupSpec]:
        return _build_zarr_schema(_ITEM_COUNT)

    def build_write_intents(self, batch: PipelineBatch, ctx: PluginContext) -> list[WriteIntent]:
        # Never reached in success paths -- resolution fails at discovery time.
        return []


@register_ingestor("irregular_axis_concrete")
class IrregularAxisConcreteIngestor(DirectZarrIngestor):
    """Concrete-values fixture: the same 5 coordinates supplied as a tuple, no AUTO.

    Used by concrete-vs-AUTO equivalence tests: this ingestor must produce a
    ``ResolvedIndex`` byte-identical to ``IrregularAxisSafeIngestor`` when its
    axis is discovered.
    """

    PRODUCT_NAME: ClassVar[str] = "irregular_axis_concrete"

    def discover_source_files(self, ctx: PluginContext) -> Iterable[Any]:
        return list(range(_ITEM_COUNT))

    def index_spec(self, ctx: PluginContext) -> IndexSpec:
        return IndexSpec(
            name="irregular_axis_concrete_v1",
            groups={
                "data": IrregularTimeAxis(
                    coordinate="timestamp",
                    values=_canonical_timestamps(),
                ),
            },
        )

    def inspect_item(self, item: Any, ctx: PluginContext) -> ItemInfo | None:
        if not isinstance(item, int):
            return None
        return ItemInfo(coordinate=_canonical_timestamps()[item])

    def zarr_schema(self, ctx: PluginContext) -> list[ZarrGroupSpec]:
        return _build_zarr_schema(_ITEM_COUNT)

    def build_write_intents(self, batch: PipelineBatch, ctx: PluginContext) -> list[WriteIntent]:
        intents: list[WriteIntent] = []
        resolved = self.resolved_index(ctx)
        for item in batch.items:
            info = self.inspect_item(item, ctx)
            if info is None:
                continue
            slot = resolved.position("data", info.coordinate)
            intents.append(WriteIntent.slot(group="data", array="values", index=slot, data=None))
        return intents
