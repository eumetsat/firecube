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

from pathlib import Path
from typing import Any, ClassVar
from unittest.mock import MagicMock

import pytest

from firecube.ingestor.api import (
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


class _SlotFilteringHost:
    SUPPORTS_SLOT_RANGE_PARALLELISM: ClassVar[bool] = True
    name = "slot-filtering"
    batch_id_prefix = "slot_"

    def __init__(self, result_batch: PipelineBatch) -> None:
        self._batch_planner = BatchPlanner()
        self._log = MagicMock()
        self._result_batch = result_batch
        self.on_pipeline_start = MagicMock()
        self.on_batch_success = MagicMock()
        self.on_batch_failure = MagicMock()

    def timestamp_to_ts_index(self, group: str, timestamp_val: Any) -> int:
        return int(timestamp_val)

    def global_expected_time_count(self, ctx: PluginContext) -> dict[str, int]:
        return {"product": 100}

    def filter_items_to_slot_range(
        self,
        items: list[int],
        slot_start: int,
        slot_end: int,
        ctx: PluginContext,
    ) -> list[int]:
        return [item for item in items if slot_start <= item < slot_end]

    def discover_source_files(self, ctx: PluginContext):
        return iter([0, 1, 2, 3, 4])

    def filter_item(self, item: Any, ctx: PluginContext) -> bool:
        return True

    def item_size_bytes(self, item: Any) -> int:
        return 0

    def get_batch_groups(self, items: list[Any], ctx: PluginContext) -> list[str]:
        return ["product"]

    def _verify_existing_cube_batch_groups(self, ctx: RuntimeIngestContext, groups: list[str]):
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
