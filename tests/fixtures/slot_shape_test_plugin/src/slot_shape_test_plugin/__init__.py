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

"""Shape-only fixture plugins for index-spec integration tests.

Two ingestors with deliberately different slot-index shapes:

* ``FixedEpochShapeIngestor`` - four nested groups, uniform floor cadence,
  hardcoded epoch.
* ``OptionEpochShapeIngestor`` - five groups with mixed exact cadences,
  epoch read from ``ctx.options["reference_epoch"]`` and normalized.

Neither implements write intents; they exist only to feed
``plugin.index_spec(ctx)`` into the parallel gate and index resolver tests.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any, ClassVar

from firecube.core.api import IndexSpec, RegularTimeAxis, normalize_epoch_iso
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

_FIXED_EPOCH_GROUPS: dict[str, RegularTimeAxis] = {
    "groupA/data_hi": RegularTimeAxis(
        coordinate="timestamp",
        epoch="2024-09-24T00:00:00Z",
        cadence_s=600,
        mode="floor",
        size=1,
    ),
    "groupA/data_lo": RegularTimeAxis(
        coordinate="timestamp",
        epoch="2024-09-24T00:00:00Z",
        cadence_s=600,
        mode="floor",
        size=1,
    ),
    "groupB/data_hi": RegularTimeAxis(
        coordinate="timestamp",
        epoch="2024-09-24T00:00:00Z",
        cadence_s=600,
        mode="floor",
        size=1,
    ),
    "groupB/data_lo": RegularTimeAxis(
        coordinate="timestamp",
        epoch="2024-09-24T00:00:00Z",
        cadence_s=600,
        mode="floor",
        size=1,
    ),
}


def _option_epoch_groups(epoch: str) -> dict[str, RegularTimeAxis]:
    return {
        "fast_a/data": RegularTimeAxis(
            coordinate="timestamp",
            epoch=epoch,
            cadence_s=300,
            mode="exact",
            size=1,
        ),
        "fast_b/data": RegularTimeAxis(
            coordinate="timestamp",
            epoch=epoch,
            cadence_s=300,
            mode="exact",
            size=1,
        ),
        "slow_a/data": RegularTimeAxis(
            coordinate="timestamp",
            epoch=epoch,
            cadence_s=900,
            mode="exact",
            size=1,
        ),
        "slow_b/data": RegularTimeAxis(
            coordinate="timestamp",
            epoch=epoch,
            cadence_s=900,
            mode="exact",
            size=1,
        ),
        "slow_c/data": RegularTimeAxis(
            coordinate="timestamp",
            epoch=epoch,
            cadence_s=900,
            mode="exact",
            size=1,
        ),
    }


class _ShapeOnlyIngestor(DirectZarrIngestor):
    PRODUCT_NAME: ClassVar[str] = "_shape_only_ingestor"
    _groups: ClassVar[dict[str, RegularTimeAxis]]

    def discover_source_files(self, ctx: PluginContext) -> Iterable[Any]:
        return []

    def index_spec(self, ctx: PluginContext) -> IndexSpec:
        return IndexSpec(name=self._index_name, groups=dict(self._groups))

    _index_name: ClassVar[str]

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
    _index_name: ClassVar[str] = "fixed_epoch_shape_v1"

    _groups: ClassVar[dict[str, RegularTimeAxis]] = _FIXED_EPOCH_GROUPS


@register_ingestor("option_epoch_shape_plugin")
class OptionEpochShapeIngestor(_ShapeOnlyIngestor):
    PRODUCT_NAME: ClassVar[str] = "option_epoch_shape"
    _index_name: ClassVar[str] = "option_epoch_shape_v1"

    _groups: ClassVar[dict[str, RegularTimeAxis]] = _option_epoch_groups(DEFAULT_REFERENCE_EPOCH)

    def index_spec(self, ctx: PluginContext) -> IndexSpec:
        raw_epoch = str(ctx.options.get("reference_epoch", DEFAULT_REFERENCE_EPOCH))
        epoch = normalize_epoch_iso(raw_epoch)
        return IndexSpec(name=self._index_name, groups=_option_epoch_groups(epoch))
