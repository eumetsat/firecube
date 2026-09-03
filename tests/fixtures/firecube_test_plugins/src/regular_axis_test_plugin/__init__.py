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

"""Test fixture plugins for dense RegularTimeAxis preallocation.

Three fixtures cover the spec-shape and axis-mode variants exercised by
``tests/integration/test_preallocate_regular_axis_dense.py``:

* ``RegularAxisDenseCoordIngestor`` (``mode="exact"``) -- declares a
  ``(time,)`` ``ZarrArraySpec`` with ``chunks=None``. Exercises the spec-loop
  chunk resolution via ``resolve_coord_chunks(spec, slot_count)``
  plus the fill-and-stamp branch of ``_materialize_regular_coord_array``.
* ``RegularAxisNoCoordSpecIngestor`` (``mode="exact"``) -- declares NO coord
  ``ZarrArraySpec``. Exercises the no-spec fallback:
  ``_materialize_regular_coord_array`` creates the coord array from scratch via
  ``writer.ensure_group`` with ``resolve_coord_chunks(None, slot_count)``
  chunks.
* ``RegularAxisFloorCoordIngestor`` (``mode="floor"``) -- same spec shape as
  the dense-coord fixture. Exercises the unsealed branch: the coord array is
  created dense-chunked but left NaT with no ``firecube_preallocated`` marker,
  so ingest writes real (off-grid) observation times.
* ``RegularAxisEndDateIngestor`` (``mode="exact"``) -- same spec shape as the
  dense-coord fixture, but the axis horizon is expressed via ``end_date``
  instead of ``slot_count``. Exercises the end-date-bounded extent path.

Only ``"exact"`` axes are prefilled and sealed: their nominal grid is the
coordinate. A ``"floor"`` axis stores real observation times that are only
knowable at ingest, so prefilling and sealing it would make every real write
fail the drift check (the MTG FCI blocker).

All fixtures share epoch, cadence, slot count, coord name, and group so tests
compare like with like.
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
_SLOT_COUNT = 1000
_END_DATE = "2024-01-07T22:40:00Z"
_COORD = "time"
_GROUP = "data"


@register_ingestor("regular_axis_dense_coord")
class RegularAxisDenseCoordIngestor(DirectZarrIngestor):
    """RegularTimeAxis with a ``(time,)`` ZarrArraySpec that has ``chunks=None``."""

    PRODUCT_NAME: ClassVar[str] = "regular_axis_dense_coord"
    time_dim_name: ClassVar[str] = _COORD

    def index_spec(self, ctx: PluginContext) -> IndexSpec:
        return IndexSpec(
            name="regular_axis_dense_coord_v1",
            groups={
                _GROUP: RegularTimeAxis(
                    coordinate=_COORD,
                    epoch=_EPOCH,
                    cadence_s=_CADENCE_S,
                    mode="exact",
                    slot_count=_SLOT_COUNT,
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
                group=_GROUP,
                arrays=[
                    ZarrArraySpec(
                        name=_COORD,
                        shape=(_SLOT_COUNT,),
                        dtype="datetime64[ns]",
                        chunks=None,
                        fill_value=np.datetime64("NaT", "ns"),
                        expected_time_count=_SLOT_COUNT,
                        time_indexed=True,
                        dimension_names=(_COORD,),
                    ),
                    ZarrArraySpec(
                        name="values",
                        shape=(_SLOT_COUNT, 5),
                        dtype="float32",
                        chunks=(1, 5),
                        fill_value=0.0,
                        expected_time_count=_SLOT_COUNT,
                        time_indexed=True,
                        dimension_names=(_COORD, "x"),
                    ),
                ],
            )
        ]

    def build_write_intents(self, batch: PipelineBatch, ctx: PluginContext) -> list[WriteIntent]:
        return []


@register_ingestor("regular_axis_no_coord_spec")
class RegularAxisNoCoordSpecIngestor(DirectZarrIngestor):
    """RegularTimeAxis WITHOUT a ``(time,)`` coord ``ZarrArraySpec``."""

    PRODUCT_NAME: ClassVar[str] = "regular_axis_no_coord_spec"
    time_dim_name: ClassVar[str] = _COORD

    def index_spec(self, ctx: PluginContext) -> IndexSpec:
        return IndexSpec(
            name="regular_axis_no_coord_spec_v1",
            groups={
                _GROUP: RegularTimeAxis(
                    coordinate=_COORD,
                    epoch=_EPOCH,
                    cadence_s=_CADENCE_S,
                    mode="exact",
                    slot_count=_SLOT_COUNT,
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
                group=_GROUP,
                arrays=[
                    ZarrArraySpec(
                        name="values",
                        shape=(_SLOT_COUNT, 5),
                        dtype="float32",
                        chunks=(1, 5),
                        fill_value=0.0,
                        expected_time_count=_SLOT_COUNT,
                        time_indexed=True,
                        dimension_names=(_COORD, "x"),
                    ),
                ],
            )
        ]

    def build_write_intents(self, batch: PipelineBatch, ctx: PluginContext) -> list[WriteIntent]:
        return []


@register_ingestor("regular_axis_floor_coord")
class RegularAxisFloorCoordIngestor(DirectZarrIngestor):
    """``mode="floor"`` RegularTimeAxis: real sensing times, unsealed coord."""

    PRODUCT_NAME: ClassVar[str] = "regular_axis_floor_coord"
    time_dim_name: ClassVar[str] = _COORD

    def index_spec(self, ctx: PluginContext) -> IndexSpec:
        return IndexSpec(
            name="regular_axis_floor_coord_v1",
            groups={
                _GROUP: RegularTimeAxis(
                    coordinate=_COORD,
                    epoch=_EPOCH,
                    cadence_s=_CADENCE_S,
                    mode="floor",
                    slot_count=_SLOT_COUNT,
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
                group=_GROUP,
                arrays=[
                    ZarrArraySpec(
                        name=_COORD,
                        shape=(_SLOT_COUNT,),
                        dtype="datetime64[ns]",
                        chunks=None,
                        fill_value=np.datetime64("NaT", "ns"),
                        expected_time_count=_SLOT_COUNT,
                        time_indexed=True,
                        dimension_names=(_COORD,),
                    ),
                    ZarrArraySpec(
                        name="values",
                        shape=(_SLOT_COUNT, 5),
                        dtype="float32",
                        chunks=(1, 5),
                        fill_value=0.0,
                        expected_time_count=_SLOT_COUNT,
                        time_indexed=True,
                        dimension_names=(_COORD, "x"),
                    ),
                ],
            )
        ]

    def build_write_intents(self, batch: PipelineBatch, ctx: PluginContext) -> list[WriteIntent]:
        return []


@register_ingestor("regular_axis_unbounded")
class RegularAxisUnboundedIngestor(DirectZarrIngestor):
    """RegularTimeAxis with no fixed extent (no slot_count, no end_date)."""

    PRODUCT_NAME: ClassVar[str] = "regular_axis_unbounded"
    time_dim_name: ClassVar[str] = _COORD

    def index_spec(self, ctx: PluginContext) -> IndexSpec:
        return IndexSpec(
            name="regular_axis_unbounded_v1",
            groups={
                _GROUP: RegularTimeAxis(
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
                group=_GROUP,
                arrays=[
                    ZarrArraySpec(
                        name="values",
                        shape=(_SLOT_COUNT, 5),
                        dtype="float32",
                        chunks=(1, 5),
                        fill_value=0.0,
                        expected_time_count=_SLOT_COUNT,
                        time_indexed=True,
                        dimension_names=(_COORD, "x"),
                    ),
                ],
            )
        ]

    def build_write_intents(self, batch: PipelineBatch, ctx: PluginContext) -> list[WriteIntent]:
        return []


@register_ingestor("regular_axis_end_date")
class RegularAxisEndDateIngestor(DirectZarrIngestor):
    """RegularTimeAxis with ``end_date`` instead of ``slot_count``."""

    PRODUCT_NAME: ClassVar[str] = "regular_axis_end_date"
    time_dim_name: ClassVar[str] = _COORD

    def index_spec(self, ctx: PluginContext) -> IndexSpec:
        return IndexSpec(
            name="regular_axis_end_date_v1",
            groups={
                _GROUP: RegularTimeAxis(
                    coordinate=_COORD,
                    epoch=_EPOCH,
                    cadence_s=_CADENCE_S,
                    mode="exact",
                    end_date=_END_DATE,
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
                group=_GROUP,
                arrays=[
                    ZarrArraySpec(
                        name=_COORD,
                        shape=(_SLOT_COUNT,),
                        dtype="datetime64[ns]",
                        chunks=None,
                        fill_value=np.datetime64("NaT", "ns"),
                        expected_time_count=_SLOT_COUNT,
                        time_indexed=True,
                        dimension_names=(_COORD,),
                    ),
                    ZarrArraySpec(
                        name="values",
                        shape=(_SLOT_COUNT, 5),
                        dtype="float32",
                        chunks=(1, 5),
                        fill_value=0.0,
                        expected_time_count=_SLOT_COUNT,
                        time_indexed=True,
                        dimension_names=(_COORD, "x"),
                    ),
                ],
            )
        ]

    def build_write_intents(self, batch: PipelineBatch, ctx: PluginContext) -> list[WriteIntent]:
        return []
