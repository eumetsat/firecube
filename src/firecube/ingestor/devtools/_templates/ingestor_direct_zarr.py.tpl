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

"""Direct Zarr ingestor for {plugin_name}.

Only ``read_product_item`` knows the source format. Implement it, then adapt
``index_spec`` and ``zarr_schema`` to the product's real time axis and arrays.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, ClassVar

import numpy as np

from firecube.ingestor.api import (
    DirectZarrIngestor,
    IndexedWrite,
    IndexSpec,
    ItemInfo,
    PipelineBatch,
    PluginContext,
    TimeAxis,
    WriteIntent,
    ZarrArraySpec,
    ZarrGroupSpec,
    register_ingestor,
)


def read_product_item(path: Path) -> tuple[np.datetime64, np.ndarray]:
    """Read one source file: its observation time and its four sample values."""
    raise NotImplementedError(
        f"read_product_item() is not implemented (called for {{path}}). Read the file and "
        "return (timestamp, values): a numpy.datetime64 observation time and a float32 "
        "array of shape (4,). Every hook below is wired to this function."
    )


@register_ingestor("{plugin_name}")
class {class_name}(DirectZarrIngestor):
    PRODUCT_NAME: ClassVar[str] = "{plugin_name}"
    # To accept ``--option key=value`` flags, attach a PluginConfig subclass;
    # see the Firecube "Add Plugin Configuration Options" guide.

    def index_spec(self, ctx: PluginContext) -> IndexSpec | None:
        # The axis is a product constant: it sets the index identity of every
        # store this plugin writes, and it must be resolvable without source
        # data because ``firecube zarr slots`` and ``preallocate`` call it first.
        # Other shapes: ``TimeAxis.grid`` (timestamps exactly on the grid),
        # ``TimeAxis.explicit`` (known list), ``TimeAxis.discovered`` (read from
        # items), ``IntegerAxis(slot_count=N)`` (integer positions).
        _ = ctx
        return IndexSpec(
            name="{plugin_name}_v1",
            groups={{
                "data": TimeAxis.observed(
                    coordinate="timestamp",
                    epoch="2024-01-01T00:00:00Z",
                    cadence_s=600,
                    end_date="2024-01-08T00:00:00Z",
                ),
            }},
        )

    def inspect_item(self, item: Any, ctx: PluginContext) -> ItemInfo | None:
        timestamp, values = read_product_item(ctx.materialize(item))
        if values.shape != (4,):
            raise ValueError(f"Expected four sample values, got {{values.shape}}")
        return ItemInfo(coordinate=timestamp)

    def zarr_schema(self, ctx: PluginContext) -> list[ZarrGroupSpec]:
        n_times = self.resolved_index(ctx).size("data")
        return [
            ZarrGroupSpec(
                group="data",
                coord_names=frozenset({{"timestamp"}}),
                arrays=[
                    # The time chunk must divide the slot count (1008 here).
                    ZarrArraySpec(
                        name="timestamp",
                        shape=(n_times,),
                        dtype="datetime64[ns]",
                        chunks=(24,),
                        dimension_names=("timestamp",),
                    ),
                    ZarrArraySpec(
                        name="value",
                        shape=(n_times, 4),
                        dtype=np.float32,
                        chunks=(1, 4),
                        dimension_names=("timestamp", "sample"),
                    ),
                ],
            )
        ]

    def build_write_intents(
        self, batch: PipelineBatch, ctx: PluginContext
    ) -> list[WriteIntent | IndexedWrite]:
        out: list[WriteIntent | IndexedWrite] = []
        for item in batch.items:
            timestamp, values = read_product_item(ctx.materialize(item))
            out.append(
                IndexedWrite.slot(
                    group="data",
                    array="value",
                    coordinate=timestamp,
                    data=values,
                )
            )
        # ``IndexedWrite.region`` writes 2-D tiles; ``WriteIntent.static`` writes
        # arrays that do not share the time axis (latitude, longitude).
        return out
