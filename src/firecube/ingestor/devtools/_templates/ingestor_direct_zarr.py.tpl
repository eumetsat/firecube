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

"""Direct Zarr ingestor for {plugin_name}."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, ClassVar

import numpy as np

from firecube.ingestor.api import (
    DirectZarrIngestor,
    IndexSpec,
    ItemInfo,
    PipelineBatch,
    PluginConfig,
    PluginContext,
    RegularTimeAxis,
    WriteIntent,
    ZarrArraySpec,
    ZarrGroupSpec,
    register_ingestor,
)


@dataclass
class {class_name}Config(PluginConfig):
    """Plugin configuration.

    Adjust the default epoch and horizon to match the product.
    """

    time_epoch: str = "2024-01-01T00:00:00Z"
    horizon_end_iso: str = "2024-01-02T00:00:00Z"


@register_ingestor("{plugin_name}")
class {class_name}(DirectZarrIngestor):
    PRODUCT_NAME: ClassVar[str] = "{plugin_name}"
    plugin_config_class = {class_name}Config

    def index_spec(self, ctx: PluginContext) -> IndexSpec:
        config = self.plugin_config
        return IndexSpec(
            name="{plugin_name}_v1",
            groups={{
                "data": RegularTimeAxis(
                    coordinate="timestamp",
                    epoch=config.time_epoch,
                    cadence_s=1,
                    mode="exact",
                    end=config.horizon_end_iso,
                )
            }},
        )

    def inspect_item(self, item: Any, ctx: PluginContext) -> ItemInfo | None:
        return ItemInfo(coordinate=item)

    def zarr_schema(self, ctx: PluginContext) -> list[ZarrGroupSpec]:
        raise NotImplementedError(
            "{class_name}.zarr_schema(): implement this hook to declare the Zarr store "
            "layout. Return a list[ZarrGroupSpec] describing groups, arrays, shapes, "
            "dtypes, and chunks. "
            "See the Firecube DirectZarrIngestor guide for examples."
        )
        time_size = self.resolved_index(ctx).size("data")
        return [
            ZarrGroupSpec(
                group="data",
                arrays=[
                    ZarrArraySpec(
                        name="values",
                        shape=(time_size,),
                        chunks=(1,),
                        dtype="float32",
                        dimension_names=("timestamp",),
                    ),
                    ZarrArraySpec(
                        name="image",
                        shape=(time_size, 1, 1),
                        chunks=(1, 1, 1),
                        dtype="float32",
                        dimension_names=("timestamp", "y", "x"),
                    ),
                    ZarrArraySpec(
                        name="latitude",
                        shape=(1,),
                        chunks=(1,),
                        dtype="float32",
                        time_indexed=False,
                        dimension_names=("latitude",),
                    ),
                    ZarrArraySpec(
                        name="longitude",
                        shape=(1,),
                        chunks=(1,),
                        dtype="float32",
                        time_indexed=False,
                        dimension_names=("longitude",),
                    ),
                ],
            )
        ]

    def build_write_intents(self, batch: PipelineBatch, ctx: PluginContext) -> list[WriteIntent]:
        raise NotImplementedError(
            "{class_name}.build_write_intents(): implement this hook to convert a batch "
            "into a list[WriteIntent] describing region writes, 1-D writes, or timestamp "
            "writes. Return an empty list to intentionally skip a batch. "
            "See the Firecube DirectZarrIngestor guide for the WriteIntent API."
        )
        resolved = self.resolved_index(ctx)
        intents: list[WriteIntent] = []

        for item in batch.items:
            info = self.inspect_item(item, ctx)
            if info is None:
                continue

            ts_index = resolved.position("data", info.coordinate)
            intents.append(
                WriteIntent.coordinate(
                    group="data",
                    index=ts_index,
                    value=info.coordinate,
                )
            )
            intents.append(
                WriteIntent.slot(
                    group="data",
                    array="values",
                    index=ts_index,
                    data=np.asarray([0.0], dtype=np.float32),
                )
            )
            intents.append(
                WriteIntent.region(
                    group="data",
                    array="image",
                    index=ts_index,
                    data=np.asarray([[0.0]], dtype=np.float32),
                    y_slice=slice(0, 1),
                )
            )

        intents.append(
            WriteIntent.static(
                group="data",
                array="latitude",
                data=np.asarray([0.0], dtype=np.float32),
            )
        )
        intents.append(
            WriteIntent.static(
                group="data",
                array="longitude",
                data=np.asarray([0.0], dtype=np.float32),
            )
        )
        return intents
