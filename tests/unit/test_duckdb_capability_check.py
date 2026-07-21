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
from typing import Any, cast

import pytest

from firecube.ingestor.config.engine import EngineConfig
from firecube.ingestor.errors import ConfigurationError
from firecube.ingestor.extensions.duck import DuckDbMixin
from firecube.ingestor.templates.generic import GenericZarrIngestor
from firecube.ingestor.types.context import (
    IngestContext,
    PipelineRunState,
    PluginContext,
    RuntimeIngestContext,
)


class _DuckAwareZarrIngestor(DuckDbMixin, GenericZarrIngestor):
    PRODUCT_NAME = "duck_aware"
    name = "duck_aware"

    def __init__(self) -> None:
        super().__init__(name="duck_aware")
        self.calls: list[str] = []

    def setup_duckdb(self, workspace=None, options=None, in_memory=True) -> None:
        _ = (workspace, options, in_memory)
        self.calls.append("setup")
        self.con = cast(Any, object())

    def prepare_duckdb_schema(self, con, ctx) -> None:
        _ = (con, ctx)
        self.calls.append("prepare")

    def teardown_duckdb(self) -> None:
        self.calls.append("teardown")

    def build_dataset(self, group, items, ctx):
        _ = (group, items, ctx)
        return None

    def ingest(self, ctx):
        _ = ctx
        raise NotImplementedError


class _CoincidentalDuckHooksIngestor(GenericZarrIngestor):
    PRODUCT_NAME = "coincidental_duck"
    name = "coincidental_duck"

    def __init__(self) -> None:
        super().__init__(name="coincidental_duck")
        self.calls: list[str] = []

    def setup_duckdb(self, workspace=None, options=None, in_memory=True) -> None:
        _ = (workspace, options, in_memory)
        self.calls.append("setup")

    def prepare_duckdb_schema(self, con, ctx) -> None:
        _ = (con, ctx)
        self.calls.append("prepare")

    def teardown_duckdb(self) -> None:
        self.calls.append("teardown")

    def build_dataset(self, group, items, ctx):
        _ = (group, items, ctx)
        return None

    def ingest(self, ctx):
        _ = ctx
        raise NotImplementedError


def _plugin_context(tmp_path: Path, *, persist: bool, in_memory: bool) -> PluginContext:
    ingest_ctx = IngestContext(
        source=".",
        target="out.zarr",
        in_memory=in_memory,
        output_format="zarr",
        options={"duckdb_persist_batches": persist},
    )
    runtime_ctx = RuntimeIngestContext.from_ingest_context(
        ingest_ctx,
        run_id="run-1",
        temp_root=tmp_path,
        materializer=lambda source: Path(source),
    )
    return PluginContext(runtime_ctx)


def _state() -> PipelineRunState:
    return PipelineRunState(
        product="duck",
        pipeline_workers=1,
        batch_size=1,
        batches=(),
        ingestion_start_time=0.0,
        batch_creation_duration=0.0,
        processing_start_time=0.0,
    )


@pytest.mark.unit
def test_duckdb_persistence_initializes_for_mixin_host(tmp_path) -> None:
    ingestor = _DuckAwareZarrIngestor()
    ingestor.engine_config = EngineConfig(duckdb_persist_batches=True)

    ingestor.on_pipeline_start(_plugin_context(tmp_path, persist=True, in_memory=False), _state())

    assert ingestor.calls == ["setup", "prepare", "teardown"]


@pytest.mark.unit
def test_duckdb_persistence_skips_for_non_mixin_host(tmp_path) -> None:
    ingestor = _CoincidentalDuckHooksIngestor()
    ingestor.engine_config = EngineConfig(duckdb_persist_batches=False)

    ingestor.on_pipeline_start(_plugin_context(tmp_path, persist=False, in_memory=False), _state())

    assert ingestor.calls == []


@pytest.mark.unit
def test_duckdb_persistence_rejects_coincidental_hooks_without_mixin(tmp_path) -> None:
    ingestor = _CoincidentalDuckHooksIngestor()
    ingestor.engine_config = EngineConfig(duckdb_persist_batches=True)

    with pytest.raises(ConfigurationError, match="DuckDbMixin-compatible hooks"):
        ingestor.on_pipeline_start(
            _plugin_context(tmp_path, persist=True, in_memory=False), _state()
        )

    assert ingestor.calls == []
