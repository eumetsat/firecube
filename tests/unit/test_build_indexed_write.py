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

"""Contract tests for ``DirectZarrIngestor.build_indexed_write`` (B4).

Behavior under test:

- Overriding only :meth:`build_indexed_write` invokes the default
  :meth:`build_write_intents` fallback: iterate ``batch.items``, compile
  via :func:`_compile_indexed_write`, concatenate.
- ``Sequence[IndexedWrite]`` return fans out; ``None`` return drops the
  item.
- Empty batch returns ``[]`` without touching :meth:`resolved_index`.
- Plugins overriding only :meth:`build_write_intents` (the pre-B4 pattern)
  are unaffected by the default fallback.
- Overriding neither hook still raises ``NotImplementedError`` from
  :meth:`build_write_intents`.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, cast

import numpy as np
import pytest

from firecube.core.index_resolve import ResolvedIndex, resolve_index_spec
from firecube.core.index_spec import IndexSpec, IntegerAxis
from firecube.ingestor.templates.direct_zarr import (
    DirectZarrIngestor,
    IndexedWrite,
    WriteIntent,
)
from firecube.ingestor.types.context import PipelineBatch


def _integer_index(slot_count: int = 5) -> ResolvedIndex:
    spec = IndexSpec(name="b4_v1", groups={"data": IntegerAxis(slot_count=slot_count)})
    return resolve_index_spec(spec, time_dim_name="timestamp")


def _batch(items: list[Any]) -> PipelineBatch:
    return PipelineBatch(batch_id="b4-test", data_path=Path("/tmp"), items=items)


class _IndexedWriteOnly(DirectZarrIngestor):
    PRODUCT_NAME = "test-b4-indexed-only"

    def __init__(self, resolved: ResolvedIndex) -> None:
        self._resolved_test = resolved

    def zarr_schema(self, ctx: Any) -> list:
        _ = ctx
        return []

    def resolved_index(self, ctx: Any) -> ResolvedIndex:
        _ = ctx
        return self._resolved_test

    def build_indexed_write(self, item: Any, ctx: Any) -> IndexedWrite:
        _ = ctx
        return IndexedWrite.slot(
            group="data",
            array="counts",
            coordinate=int(item),
            data=np.full((1,), float(item), dtype=np.float32),
        )


class _FanOutAndDrop(DirectZarrIngestor):
    PRODUCT_NAME = "test-b4-fanout"

    def __init__(self, resolved: ResolvedIndex) -> None:
        self._resolved_test = resolved

    def zarr_schema(self, ctx: Any) -> list:
        _ = ctx
        return []

    def resolved_index(self, ctx: Any) -> ResolvedIndex:
        _ = ctx
        return self._resolved_test

    def build_indexed_write(self, item: Any, ctx: Any) -> IndexedWrite | list[IndexedWrite] | None:
        _ = ctx
        coord = int(item["coord"])
        mode = item["mode"]
        if mode == "fanout":
            return [
                IndexedWrite.slot(
                    group="data",
                    array="a",
                    coordinate=coord,
                    data=np.zeros((1,), dtype=np.float32),
                ),
                IndexedWrite.slot(
                    group="data",
                    array="b",
                    coordinate=coord,
                    data=np.zeros((1,), dtype=np.float32),
                ),
            ]
        if mode == "drop":
            return None
        return IndexedWrite.slot(
            group="data",
            array="c",
            coordinate=coord,
            data=np.zeros((1,), dtype=np.float32),
        )


class _EmptyBatchProbe(DirectZarrIngestor):
    """Records whether resolved_index was consulted; batch iteration must
    return an empty list without demanding a resolved index."""

    PRODUCT_NAME = "test-b4-empty"

    def __init__(self) -> None:
        self.resolved_index_calls = 0

    def zarr_schema(self, ctx: Any) -> list:
        _ = ctx
        return []

    def resolved_index(self, ctx: Any) -> ResolvedIndex:
        _ = ctx
        self.resolved_index_calls += 1
        raise AssertionError("empty batch must not trigger resolved_index lookup")

    def build_indexed_write(self, item: Any, ctx: Any) -> IndexedWrite:
        _ = item, ctx
        raise AssertionError("empty batch must not call build_indexed_write")


_FIXED_WRITE_INTENTS: list[WriteIntent] = [
    WriteIntent.slot(
        group="data",
        array="fixed",
        index=0,
        data=np.zeros((1,), dtype=np.float32),
    )
]


class _LegacyBuildWriteIntentsOnly(DirectZarrIngestor):
    """Pre-B4 plugin pattern: override build_write_intents directly.

    Must NOT invoke the default fallback (which would call
    build_indexed_write and raise NotImplementedError).
    """

    PRODUCT_NAME = "test-b4-legacy"

    def zarr_schema(self, ctx: Any) -> list:
        _ = ctx
        return []

    def build_write_intents(self, batch: PipelineBatch, ctx: Any) -> list[WriteIntent]:
        _ = batch, ctx
        return _FIXED_WRITE_INTENTS


class _NeitherOverridden(DirectZarrIngestor):
    PRODUCT_NAME = "test-b4-neither"

    def zarr_schema(self, ctx: Any) -> list:
        _ = ctx
        return []


def test_only_indexed_write_hook_uses_default_fallback() -> None:
    ingestor = _IndexedWriteOnly(_integer_index())
    batch = _batch([0, 1, 2])

    intents = ingestor.build_write_intents(batch, cast(Any, None))

    assert len(intents) == 3
    assert [wi.ts_index for wi in intents] == [0, 1, 2]
    assert all(wi.kind == "1d" for wi in intents)
    assert all(wi.group == "data" and wi.array == "counts" for wi in intents)


def test_fan_out_and_drop_behavior() -> None:
    ingestor = _FanOutAndDrop(_integer_index())
    batch = _batch(
        [
            {"coord": 0, "mode": "fanout"},
            {"coord": 1, "mode": "drop"},
            {"coord": 2, "mode": "single"},
        ]
    )

    intents = ingestor.build_write_intents(batch, cast(Any, None))

    assert len(intents) == 3
    assert [wi.array for wi in intents] == ["a", "b", "c"]
    assert [wi.ts_index for wi in intents] == [0, 0, 2]


def test_empty_batch_returns_empty_list() -> None:
    ingestor = _EmptyBatchProbe()
    batch = _batch([])

    intents = ingestor.build_write_intents(batch, cast(Any, None))

    assert intents == []
    assert ingestor.resolved_index_calls == 0


def test_existing_build_write_intents_override_unaffected() -> None:
    ingestor = _LegacyBuildWriteIntentsOnly()
    batch = _batch([{"whatever": True}])

    intents = ingestor.build_write_intents(batch, cast(Any, None))

    assert intents is _FIXED_WRITE_INTENTS
    assert len(intents) == 1
    assert intents[0].array == "fixed"


def test_abstract_error_when_neither_hook_overridden() -> None:
    ingestor = _NeitherOverridden()
    batch = _batch([object()])

    with pytest.raises(NotImplementedError) as excinfo:
        ingestor.build_write_intents(batch, cast(Any, None))

    message = str(excinfo.value)
    assert "build_indexed_write" in message
    assert "build_write_intents" in message
