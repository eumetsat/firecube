"""Test fixture plugins for the index_spec + inspect_item API."""

from __future__ import annotations

import datetime as dt
from typing import Any, ClassVar

from firecube.core.api import IndexSpec, ItemInfo, RegularTimeAxis
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


@register_ingestor("index_spec_single")
class IndexSpecSingleGroupIngestor(DirectZarrIngestor):
    """Single-group fixture: 12 slots at 300s cadence (exact mode)."""

    PRODUCT_NAME: ClassVar[str] = "index_spec_single"

    def index_spec(self, ctx: PluginContext) -> IndexSpec:
        return IndexSpec(
            name="index_spec_single_v1",
            groups={
                "data": RegularTimeAxis(
                    coordinate="timestamp",
                    epoch=_EPOCH,
                    cadence_s=300,
                    mode="exact",
                    size=12,
                )
            },
        )

    def inspect_item(self, item: Any, ctx: PluginContext) -> ItemInfo | None:
        if not isinstance(item, (str, dt.datetime)):
            return None
        return ItemInfo(coordinate=item)

    def zarr_schema(self, ctx: PluginContext) -> list[ZarrGroupSpec]:
        return [
            ZarrGroupSpec(
                group="data",
                arrays=[
                    ZarrArraySpec(
                        name="values",
                        shape=(12, 10),
                        dtype="float32",
                        chunks=(1, 10),
                        fill_value=0.0,
                        expected_time_count=12,
                        time_indexed=True,
                    ),
                    ZarrArraySpec(
                        name="lat",
                        shape=(10,),
                        dtype="float32",
                        chunks=(10,),
                        fill_value=0.0,
                        time_indexed=False,
                    ),
                ],
            )
        ]

    def build_write_intents(self, batch: PipelineBatch, ctx: PluginContext) -> list[WriteIntent]:
        intents: list[WriteIntent] = []
        for item in batch.items:
            info = self.inspect_item(item, ctx)
            if info is None:
                continue
            ts_index = self.resolved_index(ctx).position("data", info.coordinate)
            intents.append(
                WriteIntent.slot(group="data", array="values", index=ts_index, data=None)
            )
        return intents


@register_ingestor("index_spec_multi")
class IndexSpecMultiGroupIngestor(DirectZarrIngestor):
    """Multi-group fixture: two groups with different cadences (300s + 600s)."""

    PRODUCT_NAME: ClassVar[str] = "index_spec_multi"

    def index_spec(self, ctx: PluginContext) -> IndexSpec:
        return IndexSpec(
            name="index_spec_multi_v1",
            groups={
                "fast": RegularTimeAxis(
                    coordinate="timestamp",
                    epoch=_EPOCH,
                    cadence_s=300,
                    mode="exact",
                    size=12,
                ),
                "slow": RegularTimeAxis(
                    coordinate="timestamp",
                    epoch=_EPOCH,
                    cadence_s=600,
                    mode="floor",
                    size=6,
                ),
            },
        )

    def inspect_item(self, item: Any, ctx: PluginContext) -> ItemInfo | None:
        if not isinstance(item, (str, dt.datetime)):
            return None
        return ItemInfo(coordinate=item)

    def zarr_schema(self, ctx: PluginContext) -> list[ZarrGroupSpec]:
        return [
            ZarrGroupSpec(
                group="fast",
                arrays=[
                    ZarrArraySpec(
                        name="values",
                        shape=(12, 5),
                        dtype="float32",
                        chunks=(1, 5),
                        fill_value=0.0,
                        expected_time_count=12,
                        time_indexed=True,
                    )
                ],
            ),
            ZarrGroupSpec(
                group="slow",
                arrays=[
                    ZarrArraySpec(
                        name="values",
                        shape=(6, 5),
                        dtype="float32",
                        chunks=(1, 5),
                        fill_value=0.0,
                        expected_time_count=6,
                        time_indexed=True,
                    )
                ],
            ),
        ]

    def build_write_intents(self, batch: PipelineBatch, ctx: PluginContext) -> list[WriteIntent]:
        intents: list[WriteIntent] = []
        for offset, item in enumerate(batch.items):
            info = self.inspect_item(item, ctx)
            if info is None:
                continue
            group = "fast" if offset % 2 == 0 else "slow"
            ts_index = self.resolved_index(ctx).position(group, info.coordinate)
            intents.append(WriteIntent.slot(group=group, array="values", index=ts_index, data=None))
        return intents


@register_ingestor("index_spec_custom_dim")
class IndexSpecCustomTimeDimIngestor(DirectZarrIngestor):
    """Custom time_dim_name fixture: uses 'time' instead of default 'timestamp'."""

    PRODUCT_NAME: ClassVar[str] = "index_spec_custom_dim"
    time_dim_name: ClassVar[str] = "time"

    def index_spec(self, ctx: PluginContext) -> IndexSpec:
        return IndexSpec(
            name="index_spec_custom_dim_v1",
            groups={
                "data": RegularTimeAxis(
                    coordinate="time",
                    epoch=_EPOCH,
                    cadence_s=60,
                    mode="exact",
                    size=5,
                )
            },
        )

    def inspect_item(self, item: Any, ctx: PluginContext) -> ItemInfo | None:
        if not isinstance(item, (str, dt.datetime)):
            return None
        return ItemInfo(coordinate=item)

    def zarr_schema(self, ctx: PluginContext) -> list[ZarrGroupSpec]:
        return [
            ZarrGroupSpec(
                group="data",
                arrays=[
                    ZarrArraySpec(
                        name="values",
                        shape=(5, 3),
                        dtype="float32",
                        chunks=(1, 3),
                        fill_value=0.0,
                        expected_time_count=5,
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
            ts_index = self.resolved_index(ctx).position("data", info.coordinate)
            intents.append(
                WriteIntent.slot(group="data", array="values", index=ts_index, data=None)
            )
        return intents
