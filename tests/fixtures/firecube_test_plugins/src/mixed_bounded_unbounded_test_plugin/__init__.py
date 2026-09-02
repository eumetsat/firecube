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

"""Mixed bounded + unbounded IndexSpec fixture.

Two groups sharing the ``timestamp`` dimension name:

* ``data``: bounded ``RegularTimeAxis(slot_count=100)`` — receives the
  ``firecube_group_identity_hash`` stamp at preallocate and is verified at
  mixed-spec ingest startup.
* ``aux``: unbounded ``RegularTimeAxis`` (no ``slot_count``, no ``end_date``)
  — intentionally has no stamped hash so that missing-attr behavior is the
  correctness signal for the per-group verification pass.
"""

from __future__ import annotations

import datetime as dt
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

_EPOCH = "2024-01-01T00:00:00Z"
_CADENCE_S = 600
_BOUNDED_SLOT_COUNT = 100
_COORD = "timestamp"
_BOUNDED_GROUP = "data"
_UNBOUNDED_GROUP = "aux"


@register_ingestor("mixed_bounded_unbounded_test")
class MixedBoundedUnboundedTestIngestor(DirectZarrIngestor):
    """Mixed spec: ``data`` bounded, ``aux`` unbounded."""

    PRODUCT_NAME: ClassVar[str] = "mixed_bounded_unbounded_test"
    time_dim_name: ClassVar[str] = _COORD

    def index_spec(self, ctx: PluginContext) -> IndexSpec:
        return IndexSpec(
            name="mixed_bounded_unbounded_test_v1",
            groups={
                _BOUNDED_GROUP: RegularTimeAxis(
                    coordinate=_COORD,
                    epoch=_EPOCH,
                    cadence_s=_CADENCE_S,
                    mode="exact",
                    slot_count=_BOUNDED_SLOT_COUNT,
                ),
                _UNBOUNDED_GROUP: RegularTimeAxis(
                    coordinate=_COORD,
                    epoch=_EPOCH,
                    cadence_s=_CADENCE_S,
                    mode="exact",
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
                group=_BOUNDED_GROUP,
                arrays=[
                    ZarrArraySpec(
                        name=_COORD,
                        shape=(_BOUNDED_SLOT_COUNT,),
                        dtype="datetime64[ns]",
                        chunks=None,
                        fill_value=np.datetime64("NaT", "ns"),
                        expected_time_count=_BOUNDED_SLOT_COUNT,
                        time_indexed=True,
                        dimension_names=(_COORD,),
                    ),
                    ZarrArraySpec(
                        name="values",
                        shape=(_BOUNDED_SLOT_COUNT, 4),
                        dtype="float32",
                        chunks=(1, 4),
                        fill_value=0.0,
                        expected_time_count=_BOUNDED_SLOT_COUNT,
                        time_indexed=True,
                        dimension_names=(_COORD, "x"),
                    ),
                ],
            ),
            ZarrGroupSpec(
                group=_UNBOUNDED_GROUP,
                arrays=[
                    ZarrArraySpec(
                        name="values",
                        shape=(_BOUNDED_SLOT_COUNT, 4),
                        dtype="float32",
                        chunks=(1, 4),
                        fill_value=0.0,
                        expected_time_count=_BOUNDED_SLOT_COUNT,
                        time_indexed=True,
                        dimension_names=(_COORD, "x"),
                    ),
                ],
            ),
        ]

    def build_write_intents(self, batch: PipelineBatch, ctx: PluginContext) -> list[WriteIntent]:
        return []
