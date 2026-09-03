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

from __future__ import annotations

import datetime as dt
from collections.abc import Iterable
from typing import Any, ClassVar

import numpy as np

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

PRODUCT_NAME = "direct_zarr_capable_test_product"


@register_ingestor("direct_zarr_capable_test_plugin")
class DirectZarrCapableTestIngestor(DirectZarrIngestor):
    PRODUCT_NAME: ClassVar[str] = PRODUCT_NAME

    def discover_source_files(self, ctx: PluginContext) -> Iterable[Any]:
        return list(range(200))

    def index_spec(self, ctx: PluginContext) -> IndexSpec:
        return IndexSpec(
            name="direct_zarr_capable_fixture_v2",
            groups={
                "data": RegularTimeAxis(
                    coordinate="timestamp",
                    epoch="2024-01-01T00:00:00Z",
                    cadence_s=1,
                    mode="exact",
                    slot_count=1000,
                )
            },
        )

    def inspect_item(self, item: Any, ctx: PluginContext) -> ItemInfo | None:
        if not isinstance(item, int):
            return None
        return ItemInfo(
            coordinate=dt.datetime(2024, 1, 1, tzinfo=dt.UTC) + dt.timedelta(seconds=item)
        )

    def zarr_schema(self, ctx: PluginContext) -> list[ZarrGroupSpec]:
        data_spec = ZarrArraySpec(
            name="data",
            chunks=(100, 10),
            shape=(1000, 10),
            dtype="float32",
            dimension_names=("timestamp", "x"),
            attrs={"long_name": "test data", "units": "K"},
        )
        lat_spec = ZarrArraySpec(
            name="lat",
            chunks=(10,),
            shape=(10,),
            dtype="float64",
            time_indexed=False,
            dimension_names=("lat",),
            attrs={"units": "degrees_north", "standard_name": "latitude"},
        )
        return [
            ZarrGroupSpec(
                group="data",
                arrays=[data_spec, lat_spec],
            )
        ]

    def build_write_intents(self, batch: PipelineBatch, ctx: PluginContext) -> list[WriteIntent]:
        lat_values = np.arange(10, dtype=np.float64)
        resolved = self.resolved_index(ctx)
        intents: list[WriteIntent] = []
        for item in batch.items:
            info = self.inspect_item(item, ctx)
            if info is None:
                continue
            intents.append(
                WriteIntent(
                    group="data",
                    array="data",
                    ts_index=resolved.position("data", info.coordinate),
                    data=np.full((10,), float(item), dtype="float32"),
                    kind="1d",
                )
            )
        intents.append(
            WriteIntent(
                group="data",
                array="lat",
                ts_index=0,
                data=lat_values,
                kind="static",
            )
        )
        return intents
