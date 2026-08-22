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

from __future__ import annotations

import datetime as dt
from collections.abc import Sequence
from pathlib import Path
from typing import Any, ClassVar
from unittest.mock import MagicMock

import pytest

from firecube.core.index_spec import IndexSpec, ItemInfo, RegularTimeAxis
from firecube.ingestor.api import (
    DirectZarrIngestor,
    EngineConfig,
    IngestContext,
    IngestResult,
    OutputPaths,
    PipelineBatch,
    PipelineResult,
    PluginContext,
    RuntimeIngestContext,
)
from firecube.ingestor.runtime import engine as engine_module
from firecube.ingestor.runtime.batching import BatchPlanner

pytestmark = pytest.mark.unit


def _runtime_ctx() -> RuntimeIngestContext:
    return RuntimeIngestContext(source="/tmp/source", target="/tmp/target")


def _batch() -> PipelineBatch:
    return PipelineBatch(batch_id="batch-1", data_path=Path("/tmp/batch"), items=[1])


def _host(result: PipelineResult) -> MagicMock:
    host = MagicMock()
    host.name = "mock"
    host._log = MagicMock()
    host._process_batch = MagicMock(return_value=result)
    host.on_pipeline_start = MagicMock()
    host.on_batch_success = MagicMock()
    host.on_batch_failure = MagicMock()
    return host


class _SlotFilteringHost(DirectZarrIngestor):
    PRODUCT_NAME: ClassVar[str] = "slot_filtering"
    name = "slot-filtering"

    def __init__(self, result_batch: PipelineBatch) -> None:
        super().__init__(name="slot-filtering")
        self._batch_planner = BatchPlanner()
        self._log = MagicMock()
        self._result_batch = result_batch
        self.on_pipeline_start = MagicMock()
        self.on_batch_success = MagicMock()
        self.on_batch_failure = MagicMock()

    def index_spec(self, ctx: PluginContext) -> IndexSpec:
        _ = ctx
        return IndexSpec(
            name="slot_filtering_v1",
            groups={
                "product": RegularTimeAxis(
                    coordinate="timestamp",
                    epoch="2026-01-01T00:00:00Z",
                    cadence_s=1,
                    mode="exact",
                    slot_count=100,
                )
            },
        )

    def inspect_item(self, item: Any, ctx: PluginContext) -> ItemInfo | None:
        _ = ctx
        return ItemInfo(
            coordinate=dt.datetime(2026, 1, 1, tzinfo=dt.UTC) + dt.timedelta(seconds=int(item))
        )

    def discover_source_files(self, ctx: PluginContext):
        return iter([0, 1, 2, 3, 4])

    def zarr_schema(self, ctx: PluginContext) -> list[Any]:
        _ = ctx
        return []

    def build_write_intents(self, batch: PipelineBatch, ctx: PluginContext) -> list[Any]:
        _ = (batch, ctx)
        return []

    def _create_batches(self, ctx: Any, batch_size: int):
        _ = (ctx, batch_size)
        return iter([self._result_batch])

    def filter_item(self, item: Any, ctx: PluginContext) -> bool:
        return True

    def item_size_bytes(self, item: Any) -> int:
        return 0

    def get_batch_groups(self, items: Any, ctx: PluginContext) -> list[str]:
        return ["product"]

    def _verify_existing_cube_batch_groups(
        self, ctx: RuntimeIngestContext, group_paths: Sequence[str]
    ):
        return None

    def _process_batch(self, batch: PipelineBatch, ctx: PluginContext) -> PipelineResult:
        return PipelineResult(
            batch=batch,
            outputs=OutputPaths(primary=Path("/tmp/out")),
            success=True,
        )

    def ingest(self, ctx: IngestContext) -> IngestResult:  # pragma: no cover - protocol only
        raise NotImplementedError

    def run(self, ctx: IngestContext) -> IngestResult:  # pragma: no cover - protocol only
        raise NotImplementedError


def test_run_sequential_filters_batches_to_configured_slot_range() -> None:
    ctx = _runtime_ctx()
    host = _SlotFilteringHost(_batch())
    cfg = EngineConfig(slot_start=1, slot_end=4)
    host._bind_index_at_startup(PluginContext(ctx))

    state = engine_module.run_sequential(
        ctx=ctx,
        host=host,  # type: ignore[arg-type]
        product="product",
        batch_size=2,
        engine_config=cfg,
        log=host._log,
    )

    assert [batch.items for batch in state.batches] == [[1, 2], [3]]
    assert [result.batch.items for result in state.results] == [[1, 2], [3]]


def test_run_sequential_no_slot_falls_through() -> None:
    ctx = _runtime_ctx()
    batch = _batch()
    result = PipelineResult(
        batch=batch,
        outputs=OutputPaths(primary=Path("/tmp/out")),
        success=True,
    )
    host = _host(result)
    host._create_batches = MagicMock(return_value=iter([batch]))

    state = engine_module.run_sequential(
        ctx=ctx,
        host=host,
        product="product",
        batch_size=10,
        engine_config=EngineConfig(),
        log=host._log,
    )

    host._create_batches.assert_called_once_with(ctx, 10)
    assert state.batches == (batch,)
