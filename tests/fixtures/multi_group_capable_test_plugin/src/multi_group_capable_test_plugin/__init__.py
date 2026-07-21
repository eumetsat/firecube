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

PRODUCT_NAME = "multi_group_capable_test_product"


@register_ingestor("multi_group_capable_test_plugin")
class MultiGroupCapableTestIngestor(DirectZarrIngestor):
    """Test fixture: 2 writable groups with heterogeneous chunks for Phase 3.1 testing.

    group_a: primary=(100,10), calibration=(50,4) — heterogeneous chunks WITHIN group
    group_b: primary=(50,5) — different chunks ACROSS groups vs group_a

    400 source items: 200 for group_a (ts_index 0..199), 200 for group_b (ts_index 0..199)
    """

    PRODUCT_NAME: ClassVar[str] = PRODUCT_NAME
    SUPPORTS_SLOT_RANGE_PARALLELISM: ClassVar[bool] = True

    def discover_source_files(self, ctx: PluginContext) -> Iterable[Any]:
        return [("group_a", i) for i in range(200)] + [("group_b", i) for i in range(200)]

    def timestamp_to_ts_index(self, group: str, timestamp_val: Any) -> int:
        return int(timestamp_val)

    def global_expected_time_count(self, ctx: PluginContext) -> dict[str, int]:
        return {"group_a": 1000, "group_b": 500}

    def slot_index_model(self, ctx: PluginContext) -> SlotIndexModel:
        return SlotIndexModel(
            name="multi_group_capable_fixture_v1",
            epoch="2024-01-01T00:00:00Z",
            groups={
                "group_a": SlotAxis(cadence_s=1, mode="exact"),
                "group_b": SlotAxis(cadence_s=1, mode="exact"),
            },
        )

    def filter_items_to_slot_range(
        self,
        items: Sequence[Any],
        slot_start: int,
        slot_end: int,
        ctx: PluginContext,
    ) -> Sequence[Any]:
        return [it for it in items if slot_start <= int(it[1]) < slot_end]

    def zarr_schema(self, ctx: PluginContext) -> list[ZarrGroupSpec]:
        return [
            ZarrGroupSpec(
                group="group_a",
                arrays=[
                    ZarrArraySpec(
                        name="primary",
                        chunks=(100, 10),
                        shape=(1000, 10),
                        dtype=np.float32,
                        dimension_names=("timestamp", "x"),
                        attrs={"role": "primary", "units": "1"},
                    ),
                    ZarrArraySpec(
                        name="calibration",
                        chunks=(50, 4),
                        shape=(1000, 4),
                        dtype=np.float32,
                        dimension_names=("timestamp", "x"),
                        attrs={"role": "calibration", "units": "1"},
                    ),
                    ZarrArraySpec(
                        name="lat",
                        shape=(10,),
                        dtype=np.float64,
                        time_indexed=False,
                        dimension_names=("lat",),
                        attrs={"units": "degrees_north"},
                    ),
                    ZarrArraySpec(
                        name="lon",
                        shape=(10,),
                        dtype=np.float64,
                        time_indexed=False,
                        dimension_names=("lon",),
                        attrs={"units": "degrees_east"},
                    ),
                ],
            ),
            ZarrGroupSpec(
                group="group_b",
                arrays=[
                    ZarrArraySpec(
                        name="primary",
                        chunks=(50, 5),
                        shape=(500, 5),
                        dtype=np.float32,
                        dimension_names=("timestamp", "x"),
                        attrs={"role": "primary", "units": "1"},
                    ),
                ],
            ),
        ]

    def build_write_intents(self, batch: PipelineBatch, ctx: PluginContext) -> list[WriteIntent]:
        intents: list[WriteIntent] = []
        for item in batch.items:
            group, ts_idx = item
            ts_idx_int = int(ts_idx)
            if group == "group_a":
                intents.append(
                    WriteIntent(
                        group="group_a",
                        array="primary",
                        ts_index=ts_idx_int,
                        data=np.full((10,), float(ts_idx_int), dtype="float32"),
                        kind="1d",
                    )
                )
                intents.append(
                    WriteIntent(
                        group="group_a",
                        array="lat",
                        ts_index=0,
                        data=np.arange(10, dtype=np.float64),
                        kind="static",
                    )
                )
                intents.append(
                    WriteIntent(
                        group="group_a",
                        array="lon",
                        ts_index=0,
                        data=np.arange(10, dtype=np.float64),
                        kind="static",
                    )
                )
                intents.append(
                    WriteIntent(
                        group="group_a",
                        array="calibration",
                        ts_index=ts_idx_int,
                        data=np.full((4,), float(ts_idx_int), dtype="float32"),
                        kind="1d",
                    )
                )
            elif group == "group_b":
                intents.append(
                    WriteIntent(
                        group="group_b",
                        array="primary",
                        ts_index=ts_idx_int,
                        data=np.full((5,), float(ts_idx_int), dtype="float32"),
                        kind="1d",
                    )
                )
        return intents
