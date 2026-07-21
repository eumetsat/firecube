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

from collections.abc import Iterable, Sequence
from typing import Any, ClassVar

import numpy as np

from firecube.core.api import SlotAxis, SlotIndexModel
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
    SUPPORTS_SLOT_RANGE_PARALLELISM: ClassVar[bool] = True

    def discover_source_files(self, ctx: PluginContext) -> Iterable[Any]:
        return list(range(200))

    def timestamp_to_ts_index(self, group: str, timestamp_val: Any) -> int:
        return int(timestamp_val)

    def global_expected_time_count(self, ctx: PluginContext) -> dict[str, int]:
        return {"data": 1000}

    def slot_index_model(self, ctx: PluginContext) -> SlotIndexModel:
        return SlotIndexModel(
            name="direct_zarr_capable_fixture_v1",
            epoch="2024-01-01T00:00:00Z",
            groups={"data": SlotAxis(cadence_s=1, mode="exact")},
        )

    def filter_items_to_slot_range(
        self, items: Sequence[Any], slot_start: int, slot_end: int, ctx: PluginContext
    ) -> Sequence[Any]:
        return [it for it in items if slot_start <= int(it) < slot_end]

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
        return [
            WriteIntent(
                group="data",
                array="data",
                ts_index=int(item),
                data=np.full((10,), float(item), dtype="float32"),
                kind="1d",
            )
            for item in batch.items
        ] + [
            WriteIntent(
                group="data",
                array="lat",
                ts_index=0,
                data=lat_values,
                kind="static",
            )
        ]
