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
from typing import Any
from unittest.mock import patch

import pytest

from firecube.ingestor.config.engine import EngineConfig
from firecube.ingestor.errors import ConfigurationError
from firecube.ingestor.runtime.base import BaseIngestor
from firecube.ingestor.types.context import (
    OutputPaths,
    PipelineBatch,
    PipelineResult,
    PluginContext,
    RuntimeIngestContext,
)

pytestmark = pytest.mark.unit

_DISCOVERY_TARGET = "firecube.ingestor.runtime.base.discover_input_files"


class _MinimalIngestor(BaseIngestor):
    PRODUCT_NAME = "test_empty_source"
    name = "test_empty_source"

    def _process_batch(self, batch: PipelineBatch, ctx: PluginContext) -> PipelineResult:
        return PipelineResult(batch=batch, outputs=OutputPaths(primary=""), success=True)

    def _aggregate_metrics(self, ctx: Any, state: Any) -> dict[str, Any]:
        return {}


def _make_plugin_ctx(source: str) -> PluginContext:
    runtime_ctx = RuntimeIngestContext(source=source, target="/tmp/target")
    return PluginContext(runtime_ctx)


@pytest.mark.unit
def test_empty_discovery_with_explicit_pattern_raises(tmp_path: Path) -> None:
    source = tmp_path / "source"
    source.mkdir()
    ingestor = _MinimalIngestor()
    ingestor.engine_config = EngineConfig(allow_empty_source=False, input_filters=["*.nc"])
    ctx = _make_plugin_ctx(str(source))

    with (
        patch(_DISCOVERY_TARGET, return_value=[]),
        pytest.raises(ConfigurationError, match="input-filters"),
    ):
        list(ingestor.discover_source_files(ctx))


@pytest.mark.unit
def test_empty_discovery_without_explicit_pattern_raises(tmp_path: Path) -> None:
    source = tmp_path / "source"
    source.mkdir()
    ingestor = _MinimalIngestor()
    ingestor.engine_config = EngineConfig(allow_empty_source=False)
    ctx = _make_plugin_ctx(str(source))

    with (
        patch(_DISCOVERY_TARGET, return_value=[]),
        pytest.raises(ConfigurationError, match=r"\.zip, \.h5, \.nc, \.nc4, \.hdf, \.he5"),
    ):
        list(ingestor.discover_source_files(ctx))


@pytest.mark.unit
def test_allow_empty_source_bypasses_check_even_with_pattern(tmp_path: Path) -> None:
    source = tmp_path / "source"
    source.mkdir()
    ingestor = _MinimalIngestor()
    ingestor.engine_config = EngineConfig(allow_empty_source=True, input_filters=["*.nc"])
    ctx = _make_plugin_ctx(str(source))

    with patch(_DISCOVERY_TARGET, return_value=[]):
        result = list(ingestor.discover_source_files(ctx))

    assert result == []


@pytest.mark.unit
def test_slot_range_context_bypasses_empty_source_check(tmp_path: Path) -> None:
    source = tmp_path / "source"
    source.mkdir()
    ingestor = _MinimalIngestor()
    ingestor.engine_config = EngineConfig(
        allow_empty_source=False, slot_start=0, slot_end=10, input_filters=["*.nc"]
    )
    ctx = _make_plugin_ctx(str(source))

    with patch(_DISCOVERY_TARGET, return_value=[]):
        result = list(ingestor.discover_source_files(ctx))

    assert result == []


@pytest.mark.unit
def test_non_empty_discovery_does_not_raise(tmp_path: Path) -> None:
    source = tmp_path / "source"
    source.mkdir()
    ingestor = _MinimalIngestor()
    ingestor.engine_config = EngineConfig(allow_empty_source=False, input_filters=["*.nc"])
    ctx = _make_plugin_ctx(str(source))

    files = [str(source / "a.nc"), str(source / "b.nc")]
    with patch(_DISCOVERY_TARGET, return_value=files):
        result = list(ingestor.discover_source_files(ctx))

    assert result == files


@pytest.mark.unit
def test_no_engine_config_raises_on_empty(tmp_path: Path) -> None:
    source = tmp_path / "source"
    source.mkdir()
    ingestor = _MinimalIngestor()
    ctx = _make_plugin_ctx(str(source))

    with (
        patch(_DISCOVERY_TARGET, return_value=[]),
        pytest.raises(ConfigurationError, match="allow_empty_source=true"),
    ):
        list(ingestor.discover_source_files(ctx))


class _OverridesAndCallsSuper(BaseIngestor):
    PRODUCT_NAME = "test_override_calls_super"
    name = "test_override_calls_super"

    def discover_source_files(self, ctx: PluginContext) -> Any:
        return list(super().discover_source_files(ctx))

    def _process_batch(self, batch: PipelineBatch, ctx: PluginContext) -> PipelineResult:
        return PipelineResult(batch=batch, outputs=OutputPaths(primary=""), success=True)

    def _aggregate_metrics(self, ctx: Any, state: Any) -> dict[str, Any]:
        return {}


@pytest.mark.unit
def test_override_without_opt_in_still_raises(tmp_path: Path) -> None:
    source = tmp_path / "source"
    source.mkdir()
    ingestor = _OverridesAndCallsSuper()
    ingestor.engine_config = EngineConfig(allow_empty_source=False)
    ctx = _make_plugin_ctx(str(source))

    with (
        patch(_DISCOVERY_TARGET, return_value=[]),
        pytest.raises(ConfigurationError, match="allow_empty_source=true"),
    ):
        ingestor.discover_source_files(ctx)


@pytest.mark.unit
def test_override_with_opt_in_downgrades_to_warning(tmp_path: Path) -> None:
    source = tmp_path / "source"
    source.mkdir()
    ingestor = _OverridesAndCallsSuper()
    ingestor.engine_config = EngineConfig(allow_empty_source=True)
    ctx = _make_plugin_ctx(str(source))

    with patch(_DISCOVERY_TARGET, return_value=[]):
        result = ingestor.discover_source_files(ctx)

    assert result == []
