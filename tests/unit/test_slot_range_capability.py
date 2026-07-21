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

import logging
from collections.abc import Iterable, Sequence
from typing import Any, ClassVar

import pytest

from firecube.ingestor.api import (
    EngineConfig,
    PipelineBatch,
    PipelineResult,
    PipelineRunState,
    PluginContext,
    RuntimeIngestContext,
    SlotRangeCapable,
)
from firecube.ingestor.runtime.base import BaseIngestor
from firecube.ingestor.runtime.engine import _create_batches_with_parallel_filter

pytestmark = pytest.mark.unit


class _RecordingBatchPlanner:
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    def create_batches(self, host: Any, ctx: Any, batch_size: int) -> Iterable[PipelineBatch]:
        items = list(host.discover_source_files(ctx))
        self.calls.append({"items": items, "batch_size": batch_size})
        return iter([])


def _runtime_ctx() -> RuntimeIngestContext:
    return RuntimeIngestContext(source="/tmp/source", target="/tmp/target")


def _log() -> logging.Logger:
    return logging.getLogger("firecube.test.slot_range_capability")


class _ProtocolOnlyHost(BaseIngestor):
    PRODUCT_NAME: ClassVar[str] = "protocol_only"
    SUPPORTS_SLOT_RANGE_PARALLELISM: ClassVar[bool] = True

    def __init__(self) -> None:
        super().__init__(name="protocol_only")
        self.discover_calls = 0
        self.filter_calls: list[tuple[int, int]] = []
        self._create_batches_calls = 0

    def timestamp_to_ts_index(self, group: str, timestamp_val: Any) -> int:
        return int(timestamp_val)

    def global_expected_time_count(self, ctx: PluginContext) -> dict[str, int] | None:
        return {"data": 100}

    def filter_items_to_slot_range(
        self,
        items: Sequence[Any],
        slot_start: int,
        slot_end: int,
        ctx: PluginContext,
    ) -> Sequence[Any]:
        self.filter_calls.append((slot_start, slot_end))
        return [i for i in items if slot_start <= i < slot_end]

    def discover_source_files(self, ctx: PluginContext) -> Iterable[Any]:
        self.discover_calls += 1
        return list(range(200))

    def _create_batches(self, ctx: Any, batch_size: int) -> Iterable[PipelineBatch]:
        self._create_batches_calls += 1
        return iter([])

    def _process_batch(self, batch: PipelineBatch, ctx: PluginContext) -> PipelineResult:
        raise NotImplementedError

    def _aggregate_metrics(
        self, ctx: RuntimeIngestContext, state: PipelineRunState
    ) -> dict[str, Any]:
        return {}

    def ingest(self, *args: Any, **kwargs: Any) -> Any:
        raise NotImplementedError


class _NonCapableHost(BaseIngestor):
    PRODUCT_NAME: ClassVar[str] = "non_capable"
    SUPPORTS_SLOT_RANGE_PARALLELISM: ClassVar[bool] = False

    def __init__(self) -> None:
        super().__init__(name="non_capable")
        self.discover_calls = 0
        self.filter_calls = 0
        self._create_batches_calls = 0

    def timestamp_to_ts_index(self, group: str, timestamp_val: Any) -> int:
        return int(timestamp_val)

    def global_expected_time_count(self, ctx: PluginContext) -> dict[str, int] | None:
        return None

    def filter_items_to_slot_range(
        self,
        items: Sequence[Any],
        slot_start: int,
        slot_end: int,
        ctx: PluginContext,
    ) -> Sequence[Any]:
        self.filter_calls += 1
        return list(items)

    def discover_source_files(self, ctx: PluginContext) -> Iterable[Any]:
        self.discover_calls += 1
        return list(range(50))

    def _create_batches(self, ctx: Any, batch_size: int) -> Iterable[PipelineBatch]:
        self._create_batches_calls += 1
        return iter([])

    def _process_batch(self, batch: PipelineBatch, ctx: PluginContext) -> PipelineResult:
        raise NotImplementedError

    def _aggregate_metrics(
        self, ctx: RuntimeIngestContext, state: PipelineRunState
    ) -> dict[str, Any]:
        return {}

    def ingest(self, *args: Any, **kwargs: Any) -> Any:
        raise NotImplementedError


def test_protocol_satisfying_host_is_dispatched_via_slot_range() -> None:
    host = _ProtocolOnlyHost()
    planner = _RecordingBatchPlanner()
    host._batch_planner = planner  # type: ignore[assignment]
    cfg = EngineConfig(slot_start=0, slot_end=100)

    _create_batches_with_parallel_filter(
        host=host, ctx=_runtime_ctx(), batch_size=10, engine_config=cfg, log=_log()
    )

    assert isinstance(host, SlotRangeCapable)
    assert host.discover_calls == 1
    assert host.filter_calls == [(0, 100)]
    assert host._create_batches_calls == 0
    assert len(planner.calls) == 1
    assert planner.calls[0]["items"] == list(range(100))


def test_non_capable_host_skips_slot_range_dispatch() -> None:
    host = _NonCapableHost()
    planner = _RecordingBatchPlanner()
    host._batch_planner = planner  # type: ignore[assignment]
    cfg = EngineConfig(slot_start=0, slot_end=100)

    _create_batches_with_parallel_filter(
        host=host, ctx=_runtime_ctx(), batch_size=10, engine_config=cfg, log=_log()
    )

    assert isinstance(host, SlotRangeCapable)
    assert host._create_batches_calls == 1
    assert host.discover_calls == 0
    assert host.filter_calls == 0
    assert planner.calls == []


def test_direct_zarr_ingestor_satisfies_protocol_structurally() -> None:
    from direct_zarr_capable_test_plugin import DirectZarrCapableTestIngestor

    assert isinstance(DirectZarrCapableTestIngestor(), SlotRangeCapable)


def test_protocol_runtime_check_requires_capability_surface() -> None:
    class _MissingSurface:
        SUPPORTS_SLOT_RANGE_PARALLELISM: ClassVar[bool] = True

    assert not isinstance(_MissingSurface(), SlotRangeCapable)
