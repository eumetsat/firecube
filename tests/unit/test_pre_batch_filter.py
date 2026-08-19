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
import logging
from typing import Any, ClassVar
from unittest.mock import MagicMock

import pytest

from firecube.core.index_spec import IndexSpec, ItemInfo, RegularTimeAxis
from firecube.ingestor.api import (
    DirectZarrIngestor,
    EngineConfig,
    PipelineBatch,
    PluginContext,
    RuntimeIngestContext,
)
from firecube.ingestor.errors import ConfigurationError
from firecube.ingestor.runtime import engine as engine_module
from firecube.ingestor.runtime.engine import _create_batches_with_parallel_filter

pytestmark = pytest.mark.unit


class _RecordingBatchPlanner:
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []
        self.return_value: list[PipelineBatch] = []

    def create_batches(self, host: Any, ctx: Any, batch_size: int) -> Any:
        recorded_items = list(host.discover_source_files(ctx))
        self.calls.append(
            {"host": host, "ctx": ctx, "batch_size": batch_size, "items": recorded_items}
        )
        return iter(self.return_value)


def _make_runtime_ctx() -> RuntimeIngestContext:
    return RuntimeIngestContext(source="/tmp/source", target="/tmp/target")


class _MockCapableIngestor(DirectZarrIngestor):
    PRODUCT_NAME: ClassVar[str] = "mock_cap"

    def __init__(self, *, items: list[int], inspect_impl: Any = None) -> None:
        super().__init__(name="mock_cap")
        self._items = items
        self._inspect_impl = inspect_impl
        self.discover_calls = 0
        self.inspect_calls: list[Any] = []
        self._create_batches_calls = 0

    def index_spec(self, ctx: PluginContext) -> IndexSpec:
        return IndexSpec(
            name="pre_batch_filter_v1",
            groups={
                "data": RegularTimeAxis(
                    coordinate="timestamp",
                    epoch="2026-01-01T00:00:00Z",
                    cadence_s=1,
                    mode="exact",
                    size=100,
                )
            },
        )

    def inspect_item(self, item: Any, ctx: PluginContext) -> ItemInfo | None:
        self.inspect_calls.append(item)
        if self._inspect_impl is not None:
            return self._inspect_impl(item, ctx)
        return ItemInfo(
            coordinate=dt.datetime(2026, 1, 1, tzinfo=dt.UTC) + dt.timedelta(seconds=int(item))
        )

    def discover_source_files(self, ctx: PluginContext) -> Any:
        self.discover_calls += 1
        return list(self._items)

    def zarr_schema(self, ctx: PluginContext) -> list[Any]:
        return []

    def build_write_intents(self, batch: PipelineBatch, ctx: PluginContext) -> list[Any]:
        return []

    def ingest(self, *args: Any, **kwargs: Any) -> Any:
        raise NotImplementedError

    def _create_batches(self, ctx: Any, batch_size: int) -> Any:
        self._create_batches_calls += 1
        return iter([])


class _MockNonCapableIngestor(DirectZarrIngestor):
    PRODUCT_NAME: ClassVar[str] = "mock_nc"

    def __init__(self) -> None:
        super().__init__(name="mock_nc")
        self.inspect_calls = 0
        self._create_batches_calls = 0

    def discover_source_files(self, ctx: PluginContext) -> Any:
        return list(range(50))

    def zarr_schema(self, ctx: PluginContext) -> list[Any]:
        return []

    def build_write_intents(self, batch: PipelineBatch, ctx: PluginContext) -> list[Any]:
        return []

    def ingest(self, *args: Any, **kwargs: Any) -> Any:
        raise NotImplementedError

    def _create_batches(self, ctx: Any, batch_size: int) -> Any:
        self._create_batches_calls += 1
        return iter([])


def _log() -> logging.Logger:
    return logging.getLogger("firecube.test.pre_batch_filter")


def test_no_slot_flags_uses_standard_path() -> None:
    host = _MockCapableIngestor(items=list(range(50)))
    host._batch_planner = _RecordingBatchPlanner()  # type: ignore[assignment]
    ctx = _make_runtime_ctx()
    cfg = EngineConfig()

    result = _create_batches_with_parallel_filter(
        host=host, ctx=ctx, batch_size=10, engine_config=cfg, log=_log()
    )

    assert result == []
    assert host._create_batches_calls == 1
    assert host.inspect_calls == []
    assert host.discover_calls == 0
    assert host._batch_planner.calls == []  # type: ignore[attr-defined]


def test_parallel_mode_filters_items() -> None:
    host = _MockCapableIngestor(items=list(range(200)))
    planner = _RecordingBatchPlanner()
    host._batch_planner = planner  # type: ignore[assignment]
    ctx = _make_runtime_ctx()
    cfg = EngineConfig(slot_start=0, slot_end=100)
    host._bind_index_at_startup(PluginContext(ctx))

    result = _create_batches_with_parallel_filter(
        host=host, ctx=ctx, batch_size=10, engine_config=cfg, log=_log()
    )

    assert host._create_batches_calls == 0
    assert host.discover_calls == 1
    assert host.inspect_calls == list(range(200))

    assert len(planner.calls) == 1
    call = planner.calls[0]
    assert call["batch_size"] == 10
    assert call["items"] == list(range(100))
    assert result == []


def test_filter_returns_empty_gives_empty_batches() -> None:
    host = _MockCapableIngestor(
        items=list(range(200)),
        inspect_impl=lambda item, ctx: None,
    )
    planner = _RecordingBatchPlanner()
    host._batch_planner = planner  # type: ignore[assignment]
    ctx = _make_runtime_ctx()
    cfg = EngineConfig(slot_start=0, slot_end=100)
    host._bind_index_at_startup(PluginContext(ctx))

    result = _create_batches_with_parallel_filter(
        host=host, ctx=ctx, batch_size=10, engine_config=cfg, log=_log()
    )

    assert result == []
    assert planner.calls == []
    assert host._create_batches_calls == 0


def test_filter_raises_wrapped_as_config_error() -> None:
    def _boom(*args: Any, **kwargs: Any) -> Any:
        raise ValueError("kaboom")

    host = _MockCapableIngestor(items=list(range(10)))
    host._batch_planner = _RecordingBatchPlanner()  # type: ignore[assignment]
    ctx = _make_runtime_ctx()
    cfg = EngineConfig(slot_start=0, slot_end=5)
    host._bind_index_at_startup(PluginContext(ctx))

    original = engine_module.filter_items_by_index
    engine_module.filter_items_by_index = _boom  # type: ignore[method-assign]
    try:
        with pytest.raises(ConfigurationError) as exc_info:
            _create_batches_with_parallel_filter(
                host=host, ctx=ctx, batch_size=10, engine_config=cfg, log=_log()
            )
    finally:
        engine_module.filter_items_by_index = original  # type: ignore[method-assign]

    message = str(exc_info.value)
    assert "filter_items_by_index raised an error" in message
    assert "kaboom" in message
    assert isinstance(exc_info.value.__cause__, ValueError)


def test_filter_returns_wrong_type_raises_typeerror() -> None:
    host = _MockCapableIngestor(items=list(range(10)))
    host._batch_planner = _RecordingBatchPlanner()  # type: ignore[assignment]
    ctx = _make_runtime_ctx()
    cfg = EngineConfig(slot_start=0, slot_end=5)
    host._bind_index_at_startup(PluginContext(ctx))

    original = engine_module.filter_items_by_index
    engine_module.filter_items_by_index = lambda *args, **kwargs: 42  # type: ignore[method-assign]

    try:
        with pytest.raises(TypeError, match="must return a Sequence"):
            _create_batches_with_parallel_filter(
                host=host, ctx=ctx, batch_size=10, engine_config=cfg, log=_log()
            )
    finally:
        engine_module.filter_items_by_index = original  # type: ignore[method-assign]


def test_non_capable_plugin_uses_standard_path() -> None:
    host = _MockNonCapableIngestor()
    host._batch_planner = _RecordingBatchPlanner()  # type: ignore[assignment]
    ctx = _make_runtime_ctx()
    cfg = EngineConfig()

    result = _create_batches_with_parallel_filter(
        host=host, ctx=ctx, batch_size=10, engine_config=cfg, log=_log()
    )

    assert result == []
    assert host._create_batches_calls == 1
    assert host.inspect_calls == 0
    assert host._batch_planner.calls == []  # type: ignore[attr-defined]


def test_non_direct_zarr_host_uses_standard_path() -> None:
    """A non-DirectZarrIngestor host should always take the standard path,
    even with slot flags set (defensive guard; capability gate enforces upstream)."""
    host = MagicMock()
    host._create_batches = MagicMock(return_value=iter([]))
    host.discover_source_files = MagicMock()
    ctx = _make_runtime_ctx()
    cfg = EngineConfig(slot_start=0, slot_end=100)

    result = _create_batches_with_parallel_filter(
        host=host, ctx=ctx, batch_size=10, engine_config=cfg, log=_log()
    )

    assert result == []
    host._create_batches.assert_called_once_with(ctx, 10)
    host.discover_source_files.assert_not_called()
