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

"""Contracts for the merged ``build_write_intents`` compilation path.

``build_write_intents`` returns one flat list mixing ``WriteIntent`` and
``IndexedWrite`` elements; ``_compile_write_intents`` resolves the
coordinate-keyed elements against the resolved index after the hook returns.
Locked here:

- Mixed lists compile: ``IndexedWrite`` elements resolve to slot indexes,
  plain ``WriteIntent`` elements pass through untouched, order preserved,
  and one coordinate verify-write is auto-emitted per compiled slot
  (suppressed when the plugin emitted an explicit one for that slot).
- A list with no ``IndexedWrite`` never consults ``resolved_index`` (serial
  plugins stay free to emit plain intents).
- Not overriding the hook raises ``NotImplementedError`` naming it.
- An unresolvable coordinate raises ``IndexedWriteCompilationError``.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, cast

import numpy as np
import pytest

from firecube.core.errors import IndexedWriteCompilationError
from firecube.core.index_resolve import ResolvedIndex, resolve_index_spec
from firecube.core.index_spec import IndexSpec, IntegerAxis, RegularTimeAxis
from firecube.ingestor.templates.direct_zarr import (
    DirectZarrIngestor,
    IndexedWrite,
    WriteIntent,
)
from firecube.ingestor.types.context import PipelineBatch

pytestmark = pytest.mark.unit


def _integer_index(slot_count: int = 5) -> ResolvedIndex:
    spec = IndexSpec(name="merged_v1", groups={"data": IntegerAxis(slot_count=slot_count)})
    return resolve_index_spec(spec, time_dim_name="timestamp")


def _unbounded_regular_index() -> ResolvedIndex:
    spec = IndexSpec(
        name="merged_unbounded_regular_v1",
        groups={
            "data": RegularTimeAxis(
                coordinate="timestamp",
                epoch="2024-01-01T00:00:00Z",
                cadence_s=600,
                mode="floor",
            )
        },
    )
    return resolve_index_spec(spec, time_dim_name="timestamp")


def _batch(items: list[Any]) -> PipelineBatch:
    return PipelineBatch(batch_id="merged-test", data_path=Path("/tmp"), items=items)


class _MixedIngestor(DirectZarrIngestor):
    PRODUCT_NAME = "test-merged-mixed"

    def __init__(self, resolved: ResolvedIndex) -> None:
        self._resolved_test = resolved

    def zarr_schema(self, ctx: Any) -> list:
        _ = ctx
        return []

    def resolved_index(self, ctx: Any) -> ResolvedIndex:
        _ = ctx
        return self._resolved_test

    def build_write_intents(
        self, batch: PipelineBatch, ctx: Any
    ) -> list[WriteIntent | IndexedWrite]:
        _ = ctx
        out: list[WriteIntent | IndexedWrite] = [
            IndexedWrite.slot(
                group="data",
                array="counts",
                coordinate=int(item),
                data=np.full((1,), float(item), dtype=np.float32),
            )
            for item in batch.items
        ]
        out.append(
            WriteIntent.static(group="data", array="lat", data=np.zeros((3,), dtype=np.float32))
        )
        return out


class _PlainIntentsIngestor(DirectZarrIngestor):
    PRODUCT_NAME = "test-merged-plain"

    def zarr_schema(self, ctx: Any) -> list:
        _ = ctx
        return []

    def resolved_index(self, ctx: Any) -> ResolvedIndex:
        raise AssertionError("plain WriteIntent lists must not consult resolved_index")

    def build_write_intents(
        self, batch: PipelineBatch, ctx: Any
    ) -> list[WriteIntent | IndexedWrite]:
        _ = batch, ctx
        return [
            WriteIntent.static(group="data", array="lat", data=np.zeros((3,), dtype=np.float32))
        ]


class _NotOverridden(DirectZarrIngestor):
    PRODUCT_NAME = "test-merged-neither"

    def zarr_schema(self, ctx: Any) -> list:
        _ = ctx
        return []


def test_mixed_list_compiles_indexed_writes_and_passes_intents_through() -> None:
    ingestor = _MixedIngestor(_integer_index())
    raw = ingestor.build_write_intents(_batch([0, 3]), ctx=cast(Any, None))
    intents = ingestor._compile_write_intents(raw, ctx=cast(Any, None))
    assert all(isinstance(i, WriteIntent) for i in intents)
    kinds = [i.kind for i in intents]
    assert kinds == ["1d", "1d", "static", "timestamp", "timestamp"]
    assert intents[0].ts_index == 0
    assert intents[1].ts_index == 3
    coord_by_slot = {i.ts_index: i.timestamp_val for i in intents if i.kind == "timestamp"}
    assert coord_by_slot == {0: 0, 3: 3}, (
        "one coordinate verify-write must be auto-emitted per compiled slot, "
        "carrying the raw coordinate value"
    )


def test_explicit_coordinate_intent_suppresses_auto_emission() -> None:
    ingestor = _MixedIngestor(_integer_index())
    raw = list(ingestor.build_write_intents(_batch([0]), ctx=cast(Any, None)))
    raw.append(WriteIntent.coordinate(group="data", index=0, value=0))
    intents = ingestor._compile_write_intents(raw, ctx=cast(Any, None))
    coordinate_intents = [i for i in intents if i.kind == "timestamp"]
    assert len(coordinate_intents) == 1, (
        "an explicit coordinate intent for a slot must suppress the "
        "auto-emitted one; duplicates indicate double emission"
    )


def test_indexed_write_compiles_with_unbounded_regular_axis() -> None:
    ingestor = _MixedIngestor(_unbounded_regular_index())
    raw = [
        IndexedWrite.slot(
            group="data",
            array="counts",
            coordinate="2024-01-01T00:25:00Z",
            data=np.zeros((1,), dtype=np.float32),
        )
    ]

    intents = ingestor._compile_write_intents(raw, ctx=cast(Any, None))

    assert [intent.kind for intent in intents] == ["1d", "timestamp"]
    assert [intent.ts_index for intent in intents] == [2, 2]
    assert intents[1].timestamp_val == "2024-01-01T00:25:00Z"


def test_plain_intent_list_never_consults_resolved_index() -> None:
    ingestor = _PlainIntentsIngestor()
    raw = ingestor.build_write_intents(_batch([0]), ctx=cast(Any, None))
    intents = ingestor._compile_write_intents(raw, ctx=cast(Any, None))
    assert len(intents) == 1
    assert intents[0].kind == "static"


def test_not_overriding_the_hook_raises_not_implemented() -> None:
    ingestor = _NotOverridden()
    with pytest.raises(NotImplementedError, match="build_write_intents"):
        ingestor.build_write_intents(_batch([0]), ctx=cast(Any, None))


def test_unresolvable_coordinate_raises_compilation_error() -> None:
    ingestor = _MixedIngestor(_integer_index(slot_count=2))
    raw = [
        IndexedWrite.slot(
            group="data",
            array="counts",
            coordinate=99,
            data=np.zeros((1,), dtype=np.float32),
        )
    ]
    with pytest.raises(IndexedWriteCompilationError) as excinfo:
        ingestor._compile_write_intents(raw, ctx=cast(Any, None))
    assert excinfo.value.coordinate == 99
