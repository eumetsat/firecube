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
from pathlib import Path

import pytest

from firecube.ingestor.config.engine import EngineConfig
from firecube.ingestor.errors import ConfigurationError
from firecube.ingestor.templates.generic import GenericZarrIngestor
from firecube.ingestor.types.context import (
    IngestContext,
    IngestResult,
    PipelineRunState,
    PluginContext,
    RuntimeIngestContext,
)


class _DummyZarrIngestor(GenericZarrIngestor):
    PRODUCT_NAME = "dummy_zarr"

    def build_dataset(self, group, items, ctx):  # pragma: no cover - not needed in this unit test
        _ = (group, items, ctx)
        return None

    def ingest(self, ctx: IngestContext) -> IngestResult:
        _ = ctx
        raise NotImplementedError


@pytest.mark.unit
def test_on_pipeline_start_warns_when_parallel_writes_are_serialized(caplog, tmp_path):
    ingestor = _DummyZarrIngestor(name="dummy_zarr")
    ingestor.engine_config = EngineConfig(duckdb_persist_batches=False)

    state = PipelineRunState(
        product="dummy",
        pipeline_workers=4,
        batch_size=8,
        batches=(),
        ingestion_start_time=0.0,
        batch_creation_duration=0.0,
        processing_start_time=0.0,
    )
    input_ctx = IngestContext(source=".", target="out.zarr", output_format="zarr")
    runtime_ctx = RuntimeIngestContext.from_ingest_context(
        input_ctx,
        run_id="run-1",
        temp_root=tmp_path,
        materializer=lambda source: Path(source),
    )
    ctx = PluginContext(runtime_ctx)

    with caplog.at_level(logging.WARNING, logger="firecube.ingestor.dummy_zarr"):
        ingestor.on_pipeline_start(ctx, state)

    assert any(
        "Zarr writes are serialized by a global lock" in rec.message for rec in caplog.records
    )


@pytest.mark.unit
def test_on_pipeline_start_requires_duckdb_hooks_when_persist_enabled(tmp_path):
    ingestor = _DummyZarrIngestor(name="dummy_zarr_no_duck")
    ingestor.engine_config = EngineConfig(duckdb_persist_batches=True)

    state = PipelineRunState(
        product="dummy",
        pipeline_workers=1,
        batch_size=8,
        batches=(),
        ingestion_start_time=0.0,
        batch_creation_duration=0.0,
        processing_start_time=0.0,
    )
    input_ctx = IngestContext(source=".", target="out.zarr", output_format="zarr")
    runtime_ctx = RuntimeIngestContext.from_ingest_context(
        input_ctx,
        run_id="run-1",
        temp_root=tmp_path,
        materializer=lambda source: Path(source),
    )
    runtime_ctx.in_memory = False
    ctx = PluginContext(runtime_ctx)

    with pytest.raises(ConfigurationError, match="duckdb_persist_batches=true requires"):
        ingestor.on_pipeline_start(ctx, state)
