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

"""Test fixture plugins for the ``build_indexed_write`` compile path.

Five ingestor variants exercise the B4 ``DirectZarrIngestor.build_indexed_write``
hook end-to-end without touching disk (items are pure integers 0..N-1):

* ``IndexedWriteSingleIngestor`` -- happy path, one ``IndexedWrite.region`` per
  item, five slots on a 300s ``RegularTimeAxis``.
* ``IndexedWriteFanOutIngestor`` -- fan-out: each item returns a sequence of
  two ``IndexedWrite.region`` writes to different row slices at the same slot.
* ``IndexedWriteWithStaticsIngestor`` -- overrides both ``build_indexed_write``
  (region writes) and ``build_write_intents`` (calls ``super()`` for indexed
  compilation, then appends a ``WriteIntent.static`` for a ``lat`` grid).
* ``IndexedWriteErrorIngestor`` -- returns an ``IndexedWrite`` whose
  ``coordinate`` is a bogus string that cannot be resolved on the declared
  axis; used by tests to assert compilation raises
  ``IndexedWriteCompilationError``.
* ``IndexedWriteDropIngestor`` -- returns ``None`` for item 2 to exercise the
  drop-item branch of the default ``build_write_intents`` implementation.

The fixtures use pure in-memory integer items; no network I/O or private data
is required.
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from typing import Any, ClassVar

import numpy as np

from firecube.core.api import IndexedWrite, IndexSpec, ItemInfo, RegularTimeAxis
from firecube.ingestor.api import (
    DirectZarrIngestor,
    PipelineBatch,
    PluginContext,
    WriteIntent,
    ZarrArraySpec,
    ZarrGroupSpec,
    register_ingestor,
)

_EPOCH: str = "2026-01-01T00:00:00Z"
_CADENCE_S: int = 300
_ITEM_COUNT: int = 5
_FAN_OUT_ITEM_COUNT: int = 3
_BASE_TIMESTAMP: np.datetime64 = np.datetime64("2026-01-01T00:00:00", "ns")
_Y_ROWS: int = 3
_X_COLS: int = 4
_FAN_OUT_Y_ROWS: int = 2


def _canonical_timestamps(count: int = _ITEM_COUNT) -> tuple[np.datetime64, ...]:
    step = np.timedelta64(_CADENCE_S, "s").astype("timedelta64[ns]")
    return tuple(_BASE_TIMESTAMP + i * step for i in range(count))


def _values_group(slot_count: int, y_rows: int) -> ZarrGroupSpec:
    return ZarrGroupSpec(
        group="data",
        arrays=[
            ZarrArraySpec(
                name="values",
                shape=(slot_count, y_rows, _X_COLS),
                dtype="float32",
                chunks=(1, y_rows, _X_COLS),
                fill_value=0.0,
                expected_time_count=slot_count,
                time_indexed=True,
                dimension_names=("timestamp", "y", "x"),
            )
        ],
    )


def _regular_axis(slot_count: int) -> RegularTimeAxis:
    return RegularTimeAxis(
        coordinate="timestamp",
        epoch=_EPOCH,
        cadence_s=_CADENCE_S,
        mode="exact",
        slot_count=slot_count,
    )


@register_ingestor("indexed_write_single")
class IndexedWriteSingleIngestor(DirectZarrIngestor):
    """Happy-path fixture: one ``IndexedWrite.region`` per item, five slots total."""

    PRODUCT_NAME: ClassVar[str] = "indexed_write_single"

    def discover_source_files(self, ctx: PluginContext) -> Iterable[Any]:
        return list(range(_ITEM_COUNT))

    def index_spec(self, ctx: PluginContext) -> IndexSpec:
        return IndexSpec(
            name="indexed_write_single_v1",
            groups={"data": _regular_axis(_ITEM_COUNT)},
        )

    def inspect_item(self, item: Any, ctx: PluginContext) -> ItemInfo | None:
        if not isinstance(item, int):
            return None
        return ItemInfo(coordinate=_canonical_timestamps()[item])

    def zarr_schema(self, ctx: PluginContext) -> list[ZarrGroupSpec]:
        return [_values_group(_ITEM_COUNT, _Y_ROWS)]

    def build_indexed_write(
        self, item: Any, ctx: PluginContext
    ) -> IndexedWrite | Sequence[IndexedWrite] | None:
        assert isinstance(item, int)
        return IndexedWrite.region(
            group="data",
            array="values",
            coordinate=_canonical_timestamps()[item],
            data=np.zeros((_Y_ROWS, _X_COLS), dtype=np.float32),
            y_slice=slice(0, _Y_ROWS),
        )


@register_ingestor("indexed_write_fan_out")
class IndexedWriteFanOutIngestor(DirectZarrIngestor):
    """Fan-out fixture: each item emits two ``IndexedWrite.region`` writes to different rows."""

    PRODUCT_NAME: ClassVar[str] = "indexed_write_fan_out"

    def discover_source_files(self, ctx: PluginContext) -> Iterable[Any]:
        return list(range(_FAN_OUT_ITEM_COUNT))

    def index_spec(self, ctx: PluginContext) -> IndexSpec:
        return IndexSpec(
            name="indexed_write_fan_out_v1",
            groups={"data": _regular_axis(_FAN_OUT_ITEM_COUNT)},
        )

    def inspect_item(self, item: Any, ctx: PluginContext) -> ItemInfo | None:
        if not isinstance(item, int):
            return None
        return ItemInfo(coordinate=_canonical_timestamps(_FAN_OUT_ITEM_COUNT)[item])

    def zarr_schema(self, ctx: PluginContext) -> list[ZarrGroupSpec]:
        return [_values_group(_FAN_OUT_ITEM_COUNT, _FAN_OUT_Y_ROWS)]

    def build_indexed_write(
        self, item: Any, ctx: PluginContext
    ) -> IndexedWrite | Sequence[IndexedWrite] | None:
        assert isinstance(item, int)
        coord = _canonical_timestamps(_FAN_OUT_ITEM_COUNT)[item]
        # Two region writes per item, same slot, different row slices.
        row_payload = np.zeros((1, _X_COLS), dtype=np.float32)
        return [
            IndexedWrite.region(
                group="data",
                array="values",
                coordinate=coord,
                data=row_payload,
                y_slice=slice(0, 1),
            ),
            IndexedWrite.region(
                group="data",
                array="values",
                coordinate=coord,
                data=row_payload,
                y_slice=slice(1, 2),
            ),
        ]


_STATIC_LAT_GRID: np.ndarray = np.zeros((_Y_ROWS,), dtype=np.float32)


@register_ingestor("indexed_write_with_statics")
class IndexedWriteWithStaticsIngestor(DirectZarrIngestor):
    """Fixture overriding both hooks: indexed region writes plus a static lat grid.

    ``build_indexed_write`` returns one region ``IndexedWrite`` per item, and
    ``build_write_intents`` calls ``super().build_write_intents(...)`` for the
    indexed compilation, then appends a ``WriteIntent.static`` for the static
    ``lat`` array. This mirrors the documented pattern in the
    ``build_indexed_write`` docstring on ``DirectZarrIngestor``.
    """

    PRODUCT_NAME: ClassVar[str] = "indexed_write_with_statics"

    def discover_source_files(self, ctx: PluginContext) -> Iterable[Any]:
        return list(range(_ITEM_COUNT))

    def index_spec(self, ctx: PluginContext) -> IndexSpec:
        return IndexSpec(
            name="indexed_write_with_statics_v1",
            groups={"data": _regular_axis(_ITEM_COUNT)},
        )

    def inspect_item(self, item: Any, ctx: PluginContext) -> ItemInfo | None:
        if not isinstance(item, int):
            return None
        return ItemInfo(coordinate=_canonical_timestamps()[item])

    def zarr_schema(self, ctx: PluginContext) -> list[ZarrGroupSpec]:
        return [
            ZarrGroupSpec(
                group="data",
                arrays=[
                    ZarrArraySpec(
                        name="values",
                        shape=(_ITEM_COUNT, _Y_ROWS, _X_COLS),
                        dtype="float32",
                        chunks=(1, _Y_ROWS, _X_COLS),
                        fill_value=0.0,
                        expected_time_count=_ITEM_COUNT,
                        time_indexed=True,
                        dimension_names=("timestamp", "y", "x"),
                    ),
                    ZarrArraySpec(
                        name="lat",
                        shape=(_Y_ROWS,),
                        dtype="float32",
                        chunks=(_Y_ROWS,),
                        fill_value=0.0,
                        time_indexed=False,
                        dimension_names=("y",),
                    ),
                ],
            )
        ]

    def build_indexed_write(
        self, item: Any, ctx: PluginContext
    ) -> IndexedWrite | Sequence[IndexedWrite] | None:
        assert isinstance(item, int)
        return IndexedWrite.region(
            group="data",
            array="values",
            coordinate=_canonical_timestamps()[item],
            data=np.zeros((_Y_ROWS, _X_COLS), dtype=np.float32),
            y_slice=slice(0, _Y_ROWS),
        )

    def build_write_intents(self, batch: PipelineBatch, ctx: PluginContext) -> list[WriteIntent]:
        intents = super().build_write_intents(batch, ctx)
        intents.append(
            WriteIntent.static(
                group="data",
                array="lat",
                data=_STATIC_LAT_GRID,
            )
        )
        return intents


@register_ingestor("indexed_write_error")
class IndexedWriteErrorIngestor(DirectZarrIngestor):
    """Failure-mode fixture: emits an ``IndexedWrite`` whose coordinate is not on the axis.

    The declared axis is a ``RegularTimeAxis`` expecting UTC timestamps; the
    ``build_indexed_write`` return uses a bogus ``"NOT_IN_INDEX"`` string that
    cannot be positioned. Compilation via ``_compile_indexed_write`` raises
    ``IndexedWriteCompilationError`` (asserted by the fixture-owned test).
    """

    PRODUCT_NAME: ClassVar[str] = "indexed_write_error"
    _BOGUS_COORDINATE: ClassVar[str] = "NOT_IN_INDEX"

    def discover_source_files(self, ctx: PluginContext) -> Iterable[Any]:
        return list(range(_ITEM_COUNT))

    def index_spec(self, ctx: PluginContext) -> IndexSpec:
        return IndexSpec(
            name="indexed_write_error_v1",
            groups={"data": _regular_axis(_ITEM_COUNT)},
        )

    def inspect_item(self, item: Any, ctx: PluginContext) -> ItemInfo | None:
        if not isinstance(item, int):
            return None
        # Return a resolvable coordinate for inspect_item so the discovery
        # gate does not short-circuit; the failure is deferred to the
        # build_indexed_write / _compile_indexed_write path.
        return ItemInfo(coordinate=_canonical_timestamps()[item])

    def zarr_schema(self, ctx: PluginContext) -> list[ZarrGroupSpec]:
        return [_values_group(_ITEM_COUNT, _Y_ROWS)]

    def build_indexed_write(
        self, item: Any, ctx: PluginContext
    ) -> IndexedWrite | Sequence[IndexedWrite] | None:
        assert isinstance(item, int)
        return IndexedWrite.region(
            group="data",
            array="values",
            coordinate=self._BOGUS_COORDINATE,
            data=np.zeros((_Y_ROWS, _X_COLS), dtype=np.float32),
            y_slice=slice(0, _Y_ROWS),
        )


@register_ingestor("indexed_write_drop")
class IndexedWriteDropIngestor(DirectZarrIngestor):
    """Drop-item fixture: ``build_indexed_write`` returns ``None`` for item 2."""

    PRODUCT_NAME: ClassVar[str] = "indexed_write_drop"
    _DROP_INDEX: ClassVar[int] = 2

    def discover_source_files(self, ctx: PluginContext) -> Iterable[Any]:
        return list(range(_ITEM_COUNT))

    def index_spec(self, ctx: PluginContext) -> IndexSpec:
        return IndexSpec(
            name="indexed_write_drop_v1",
            groups={"data": _regular_axis(_ITEM_COUNT)},
        )

    def inspect_item(self, item: Any, ctx: PluginContext) -> ItemInfo | None:
        if not isinstance(item, int):
            return None
        return ItemInfo(coordinate=_canonical_timestamps()[item])

    def zarr_schema(self, ctx: PluginContext) -> list[ZarrGroupSpec]:
        return [_values_group(_ITEM_COUNT, _Y_ROWS)]

    def build_indexed_write(
        self, item: Any, ctx: PluginContext
    ) -> IndexedWrite | Sequence[IndexedWrite] | None:
        assert isinstance(item, int)
        if item == self._DROP_INDEX:
            return None
        return IndexedWrite.region(
            group="data",
            array="values",
            coordinate=_canonical_timestamps()[item],
            data=np.zeros((_Y_ROWS, _X_COLS), dtype=np.float32),
            y_slice=slice(0, _Y_ROWS),
        )
