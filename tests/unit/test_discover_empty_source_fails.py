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
from tests.helpers.rows import table_row

pytestmark = pytest.mark.unit

# The file-tree walk of a local source, below the default discovery.
_DISCOVERY_TARGET = "firecube.core.discovery._walk._list_input_files"


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


class _OverridesAndCallsSuper(BaseIngestor):
    PRODUCT_NAME = "test_override_calls_super"
    name = "test_override_calls_super"

    def discover_source_files(self, ctx: PluginContext) -> Any:
        return list(super().discover_source_files(ctx))

    def _process_batch(self, batch: PipelineBatch, ctx: PluginContext) -> PipelineResult:
        return PipelineResult(batch=batch, outputs=OutputPaths(primary=""), success=True)

    def _aggregate_metrics(self, ctx: Any, state: Any) -> dict[str, Any]:
        return {}


def _discover(
    ingestor_cls: type[BaseIngestor], config: EngineConfig | None, source: Path, found: list[str]
) -> list[Any]:
    """Discover from an existing *source* while the filesystem walk returns *found*."""
    source.mkdir()
    ingestor = ingestor_cls()
    if config is not None:  # None: the attribute stays unset, as on a bare ingestor
        ingestor.engine_config = config
    with patch(_DISCOVERY_TARGET, return_value=found):
        return list(ingestor.discover_source_files(_make_plugin_ctx(str(source))))


# (row id, ingestor class, engine config, ConfigurationError message pattern)
_EMPTY_REFUSED: tuple[tuple[str, type[BaseIngestor], EngineConfig | None, str], ...] = (
    (
        "test_empty_discovery_with_explicit_pattern_raises",
        _MinimalIngestor,
        EngineConfig(allow_empty_source=False, input_filters=["*.nc"]),
        "pass input_filters=",
    ),
    (
        "test_empty_discovery_without_explicit_pattern_raises",
        _MinimalIngestor,
        EngineConfig(allow_empty_source=False),
        r"\.zip, \.h5, \.nc, \.nc4, \.hdf, \.he5",
    ),
    ("test_no_engine_config_raises_on_empty", _MinimalIngestor, None, "allow_empty_source=true"),
    (
        "test_override_without_opt_in_still_raises",
        _OverridesAndCallsSuper,
        EngineConfig(allow_empty_source=False),
        "allow_empty_source=true",
    ),
)


@pytest.mark.unit
def test_empty_discovery_is_refused(subtests: Any, tmp_path: Path) -> None:
    for row_id, ingestor_cls, config, message in _EMPTY_REFUSED:
        with (
            table_row(subtests, row_id, tmp_path) as (workdir, _),
            pytest.raises(ConfigurationError, match=message),
        ):
            _discover(ingestor_cls, config, workdir / "source", [])


# (row id, ingestor class, engine config, files the walk finds)
_DISCOVERY_PASSES: tuple[tuple[str, type[BaseIngestor], EngineConfig, list[str]], ...] = (
    (
        "test_allow_empty_source_bypasses_check_even_with_pattern",
        _MinimalIngestor,
        EngineConfig(allow_empty_source=True, input_filters=["*.nc"]),
        [],
    ),
    (
        "test_slot_range_context_bypasses_empty_source_check",
        _MinimalIngestor,
        EngineConfig(allow_empty_source=False, slot_start=0, slot_end=10, input_filters=["*.nc"]),
        [],
    ),
    (
        "test_non_empty_discovery_does_not_raise",
        _MinimalIngestor,
        EngineConfig(allow_empty_source=False, input_filters=["*.nc"]),
        ["a.nc", "b.nc"],
    ),
    (
        "test_override_with_opt_in_downgrades_to_warning",
        _OverridesAndCallsSuper,
        EngineConfig(allow_empty_source=True),
        [],
    ),
)


@pytest.mark.unit
def test_discovery_returns_what_was_found(subtests: Any, tmp_path: Path) -> None:
    for row_id, ingestor_cls, config, names in _DISCOVERY_PASSES:
        with table_row(subtests, row_id, tmp_path) as (workdir, _):
            files = [str(workdir / "source" / name) for name in names]

            assert _discover(ingestor_cls, config, workdir / "source", files) == files
