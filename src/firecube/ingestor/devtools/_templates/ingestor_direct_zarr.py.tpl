# FIRECUBE_TEMPLATE_LICENSE_HEADER_BEGIN
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
# FIRECUBE_TEMPLATE_LICENSE_HEADER_END
{copyright_header}
"""Direct Zarr ingestor for {plugin_name}.

Only ``read_product_item`` knows the source format. Implement it, then adapt
``index_spec`` and ``zarr_schema`` to the product's real time axis and arrays.
"""

from __future__ import annotations

import dataclasses
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
    ZarrTemplateConfig,
    register_ingestor,
)


def read_product_item(path: Path) -> tuple[np.datetime64, np.ndarray]:
    """Read one source file: its observation time and its four sample values.

    Returns:
        ``(timestamp, values)``. ``timestamp`` is a ``numpy.datetime64`` in UTC
        without a timezone; ``values`` is a ``float32`` array of shape ``(4,)``,
        matching the ``value`` array in ``zarr_schema``. Add the library that
        reads the format to ``dependencies`` in ``pyproject.toml``.
    """
    raise NotImplementedError(
        f"read_product_item() is not implemented (called for {{path}}). Read the file and "
        "return (timestamp, values): a numpy.datetime64 observation time and a float32 "
        "array of shape (4,). Every hook below is wired to this function."
    )


@dataclasses.dataclass
class ZarrStorageConfig(ZarrTemplateConfig):
    """Plugin-wide compression defaults for the arrays this plugin writes.

    Nothing is overridden, so arrays are compressed with zstd. Uncomment a setting
    to make it this plugin's default; an operator's ``--option`` or config file
    still wins. Chunk and shard shapes come from each ``ZarrArraySpec`` in
    ``zarr_schema``; the ``zarr_chunk_shape`` and ``zarr_shard_shape`` options have
    no effect on this template. See the Firecube guides "Configure Zarr
    Chunking", "Configure Zarr Compression", and "Configure Zarr Sharding".
    """

    # Store arrays uncompressed. Arrays that set ``compressors`` are then refused.
    # zarr_compression: bool = False

    # Or keep compression and choose the codecs. Every codec needs both "name" and
    # "configuration"; use {{}} for the codec's own defaults.
    # zarr_codecs: list[dict] | None = dataclasses.field(
    #     default_factory=lambda: [{{"name": "zstd", "configuration": {{"level": 3}}}}]
    # )


@register_ingestor("{plugin_name}")
class {class_name}(DirectZarrIngestor):
    PRODUCT_NAME: ClassVar[str] = "{plugin_name}"
    template_config_class = ZarrStorageConfig
    # To accept your own ``--option key=value`` flags, attach a PluginConfig
    # subclass; see the Firecube "Add Plugin Configuration Options" guide.

    def index_spec(self, ctx: PluginContext) -> IndexSpec | None:
        # The axis is a product constant: it sets the index identity of every
        # store this plugin writes, and it must be resolvable without source
        # data because ``firecube zarr slots`` and ``preallocate`` call it first.
        # Stores written with one axis refuse a plugin that declares another.
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
        # Firecube calls this for every discovered item: in a serial run, in each
        # slot-range worker, and during ``firecube zarr preallocate --input-data``.
        # Keep it cheap. Read only what the coordinate needs, not the full data.
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
                    # Slot-range workers must cover whole multiples of the time chunk
                    # (or shard) of every time-indexed array: 24 slots, four hours,
                    # here. ``firecube zarr slots`` uses it as the default range size.
                    # Keep the slot count (1008) a multiple too, so no chunk is partial.
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
                        # One chunk per slot, holding its four samples.
                        chunks=(1, 4),
                        # NaN marks slots no item has written yet; the Zarr default
                        # of 0.0 would read like real data.
                        fill_value=np.nan,
                        dimension_names=("timestamp", "sample"),
                        # Store 24 slot chunks per file. Each shard length must be a
                        # whole number of chunks and keep the slot-range rule above.
                        # shards=(24, 4),
                        # Override ZarrStorageConfig for this array only; () stores it
                        # uncompressed. Needs zarr_compression left on.
                        # compressors=({{"name": "zstd", "configuration": {{"level": 3}}}},),
                    ),
                ],
            )
        ]

    def build_write_intents(
        self, batch: PipelineBatch, ctx: PluginContext
    ) -> list[WriteIntent | IndexedWrite]:
        """Return the writes for one batch; Firecube places each at its slot.

        ``IndexedWrite.slot`` writes one item's values at the slot that matches its
        coordinate.
        """
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
