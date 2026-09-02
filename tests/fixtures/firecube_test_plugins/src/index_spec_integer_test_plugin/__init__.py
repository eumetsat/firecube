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

"""Test fixture plugins for IntegerAxis index-spec coverage."""

from __future__ import annotations

import datetime as dt
from typing import Any, ClassVar

from firecube.core.api import IndexSpec, IntegerAxis, ItemInfo, RegularTimeAxis
from firecube.ingestor.api import (
    DirectZarrIngestor,
    PipelineBatch,
    PluginContext,
    WriteIntent,
    ZarrArraySpec,
    ZarrGroupSpec,
    register_ingestor,
)

_EPOCH = "2024-01-01T00:00:00Z"


def _is_coordinate(item: Any) -> bool:
    return isinstance(item, (int, dt.datetime, str)) and not isinstance(item, bool)


@register_ingestor("index_spec_integer_test")
class IntegerAxisIngestor(DirectZarrIngestor):
    """Integer-axis fixture: 144 slots backed by a fixed-size integer axis."""

    PRODUCT_NAME: ClassVar[str] = "index_spec_integer_test"

    def index_spec(self, ctx: PluginContext) -> IndexSpec:
        return IndexSpec(name="integer_test", groups={"data": IntegerAxis(slot_count=144)})

    def inspect_item(self, item: Any, ctx: PluginContext) -> ItemInfo | None:
        if not _is_coordinate(item):
            return None
        return ItemInfo(coordinate=item)

    def zarr_schema(self, ctx: PluginContext) -> list[ZarrGroupSpec]:
        return [
            ZarrGroupSpec(
                group="data",
                arrays=[
                    ZarrArraySpec(
                        name="values",
                        shape=(144, 8),
                        dtype="float32",
                        chunks=(1, 8),
                        fill_value=0.0,
                        expected_time_count=144,
                        time_indexed=True,
                    )
                ],
            )
        ]

    def build_write_intents(self, batch: PipelineBatch, ctx: PluginContext) -> list[WriteIntent]:
        intents: list[WriteIntent] = []
        for item in batch.items:
            info = self.inspect_item(item, ctx)
            if info is None:
                continue
            slot = self.resolved_index(ctx).position("data", info.coordinate)
            intents.append(WriteIntent.slot(group="data", array="values", index=slot, data=None))
        return intents


@register_ingestor("index_spec_integer_mixed_test")
class MixedAxisIngestor(DirectZarrIngestor):
    """Mixed-axis fixture: integer and regular-time groups in one spec."""

    PRODUCT_NAME: ClassVar[str] = "index_spec_integer_mixed_test"

    def index_spec(self, ctx: PluginContext) -> IndexSpec:
        return IndexSpec(
            name="integer_mixed_test",
            groups={
                "data": IntegerAxis(slot_count=144),
                "timestamped": RegularTimeAxis(
                    coordinate="timestamp",
                    epoch=_EPOCH,
                    cadence_s=600,
                    mode="exact",
                    slot_count=24,
                ),
            },
        )

    def inspect_item(self, item: Any, ctx: PluginContext) -> ItemInfo | None:
        if not _is_coordinate(item):
            return None
        return ItemInfo(coordinate=item)

    def zarr_schema(self, ctx: PluginContext) -> list[ZarrGroupSpec]:
        return [
            ZarrGroupSpec(
                group="data",
                arrays=[
                    ZarrArraySpec(
                        name="values",
                        shape=(144, 4),
                        dtype="float32",
                        chunks=(1, 4),
                        fill_value=0.0,
                        expected_time_count=144,
                        time_indexed=True,
                    )
                ],
            ),
            ZarrGroupSpec(
                group="timestamped",
                arrays=[
                    ZarrArraySpec(
                        name="values",
                        shape=(24, 4),
                        dtype="float32",
                        chunks=(1, 4),
                        fill_value=0.0,
                        expected_time_count=24,
                        time_indexed=True,
                    )
                ],
            ),
        ]

    def build_write_intents(self, batch: PipelineBatch, ctx: PluginContext) -> list[WriteIntent]:
        intents: list[WriteIntent] = []
        for item in batch.items:
            info = self.inspect_item(item, ctx)
            if info is None:
                continue
            group = (
                "data"
                if isinstance(info.coordinate, int) and not isinstance(info.coordinate, bool)
                else "timestamped"
            )
            slot = self.resolved_index(ctx).position(group, info.coordinate)
            intents.append(WriteIntent.slot(group=group, array="values", index=slot, data=None))
        return intents
