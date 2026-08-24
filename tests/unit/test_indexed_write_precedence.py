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

"""Precedence tests for ``DirectZarrIngestor`` indexed-write hooks.

Behavior under test:

- If both hooks are overridden, ``build_write_intents`` wins and the engine
  never reaches ``build_indexed_write``.
- If only ``build_indexed_write`` is overridden, the default
  ``build_write_intents`` fallback calls it once per batch item.
- If only ``build_write_intents`` is overridden, the direct override wins and
  ``build_indexed_write`` is not consulted.
- If neither hook is overridden, ``build_write_intents`` raises
  ``NotImplementedError``.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, cast

import numpy as np
import pytest

from firecube.core.index_resolve import ResolvedIndex, resolve_index_spec
from firecube.core.index_spec import IndexSpec, IntegerAxis
from firecube.ingestor.templates.direct_zarr import DirectZarrIngestor, IndexedWrite, WriteIntent
from firecube.ingestor.types.context import PipelineBatch


def _integer_index(slot_count: int = 5) -> ResolvedIndex:
    spec = IndexSpec(name="prec_v1", groups={"data": IntegerAxis(slot_count=slot_count)})
    return resolve_index_spec(spec, time_dim_name="timestamp")


def _batch(items: list[Any]) -> PipelineBatch:
    return PipelineBatch(batch_id="precedence-test", data_path=Path("/tmp"), items=items)


class _DualOverride(DirectZarrIngestor):
    PRODUCT_NAME = "test-precedence-dual"

    def __init__(self) -> None:
        self.write_intents_calls = 0
        self.indexed_write_calls = 0

    def zarr_schema(self, ctx: Any) -> list:
        _ = ctx
        return []

    def build_write_intents(self, batch: PipelineBatch, ctx: Any) -> list[WriteIntent]:
        _ = batch, ctx
        self.write_intents_calls += 1
        return [
            WriteIntent.slot(
                group="data",
                array="counts",
                index=0,
                data=np.zeros((1,), dtype=np.float32),
            )
        ]

    def build_indexed_write(self, item: Any, ctx: Any) -> IndexedWrite:
        _ = item, ctx
        self.indexed_write_calls += 1
        return IndexedWrite.slot(
            group="data",
            array="counts",
            coordinate=0,
            data=np.zeros((1,), dtype=np.float32),
        )


class _IndexedWriteOnly(DirectZarrIngestor):
    PRODUCT_NAME = "test-precedence-indexed-only"

    def __init__(self, resolved: ResolvedIndex) -> None:
        self._resolved = resolved
        self.indexed_write_calls = 0

    def zarr_schema(self, ctx: Any) -> list:
        _ = ctx
        return []

    def resolved_index(self, ctx: Any) -> ResolvedIndex:
        _ = ctx
        return self._resolved

    def build_indexed_write(self, item: Any, ctx: Any) -> IndexedWrite:
        _ = ctx
        self.indexed_write_calls += 1
        return IndexedWrite.slot(
            group="data",
            array="counts",
            coordinate=int(item),
            data=np.full((1,), float(item), dtype=np.float32),
        )


class _BuildWriteIntentsOnly(DirectZarrIngestor):
    PRODUCT_NAME = "test-precedence-write-intents-only"

    def __init__(self) -> None:
        self.write_intents_calls = 0

    def zarr_schema(self, ctx: Any) -> list:
        _ = ctx
        return []

    def build_write_intents(self, batch: PipelineBatch, ctx: Any) -> list[WriteIntent]:
        _ = batch, ctx
        self.write_intents_calls += 1
        return [
            WriteIntent.slot(
                group="data",
                array="fixed",
                index=0,
                data=np.zeros((1,), dtype=np.float32),
            )
        ]


class _NeitherOverridden(DirectZarrIngestor):
    PRODUCT_NAME = "test-precedence-neither"

    def zarr_schema(self, ctx: Any) -> list:
        _ = ctx
        return []


def test_dual_override_write_intents_wins() -> None:
    ingestor = _DualOverride()
    batch = _batch([0])

    intents = ingestor.build_write_intents(batch, cast(Any, None))

    assert len(intents) == 1
    assert ingestor.write_intents_calls == 1
    assert ingestor.indexed_write_calls == 0


def test_only_indexed_write_uses_default_fallback() -> None:
    ingestor = _IndexedWriteOnly(_integer_index())
    batch = _batch([0, 1])

    intents = ingestor.build_write_intents(batch, cast(Any, None))

    assert type(ingestor).build_write_intents is DirectZarrIngestor.build_write_intents
    assert len(intents) == 2
    assert [wi.ts_index for wi in intents] == [0, 1]
    assert ingestor.indexed_write_calls == 2


def test_only_build_write_intents_overridden() -> None:
    ingestor = _BuildWriteIntentsOnly()
    batch = _batch([0])

    intents = ingestor.build_write_intents(batch, None)

    assert len(intents) == 1
    assert ingestor.write_intents_calls == 1
    assert type(ingestor).build_indexed_write is DirectZarrIngestor.build_indexed_write


def test_neither_overridden_raises_not_implemented() -> None:
    ingestor = _NeitherOverridden()
    batch = _batch([0])

    with pytest.raises(NotImplementedError) as excinfo:
        ingestor.build_write_intents(batch, cast(Any, None))

    message = str(excinfo.value)
    assert "build_indexed_write" in message or "build_write_intents" in message
