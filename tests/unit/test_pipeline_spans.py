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

from contextlib import nullcontext
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import MagicMock

import pytest
import xarray as xr

from firecube.ingestor.config.engine import EngineConfig
from firecube.ingestor.contracts.interfaces import PipelineHost
from firecube.ingestor.runtime.engine import PipelineExecutor, _process_batch_timed
from firecube.ingestor.templates import generic as generic_module
from firecube.ingestor.templates.generic import GenericZarrIngestor
from firecube.ingestor.types.context import (
    IngestContext,
    OutputPaths,
    PipelineBatch,
    PipelineResult,
    PipelineRunState,
    PluginContext,
    RuntimeIngestContext,
)


class MockTelemetry:
    def __init__(self):
        self.spans: list[tuple[str, dict[str, object] | None]] = []
        self._run_id = "run-1"

    @property
    def run_id(self) -> str:
        return self._run_id

    def emit(
        self, name: str, value: float, *, kind: str = "gauge", meta: dict[str, Any] | None = None
    ) -> None:
        _ = (name, value, kind, meta)

    def flush(self) -> None:
        return None

    def collect_memory_stats(self) -> None:
        return None

    def span(self, name: str, attributes: dict[str, Any] | None = None):
        telemetry = self

        class _Span:
            def __enter__(self):
                telemetry.spans.append((name, attributes))
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

        return _Span()

    @property
    def names(self) -> list[str]:
        return [name for name, _ in self.spans]


class _DummyZarrIngestor(GenericZarrIngestor):
    PRODUCT_NAME = "dummy_zarr"

    def ingest(self, ctx):
        return self.run(ctx)

    def build_dataset(self, group, items, ctx):
        _ = (group, items, ctx)
        return xr.Dataset()


def _runtime_ctx(tmp_path: Path, telemetry: MockTelemetry | None) -> RuntimeIngestContext:
    return RuntimeIngestContext.from_ingest_context(
        IngestContext(
            source=str(tmp_path),
            target=str(tmp_path / "output.zarr"),
            output_format="zarr",
            telemetry=cast(Any, telemetry),
        ),
        run_id="run-1",
        temp_root=tmp_path,
        materializer=lambda source: Path(source),
    )


def _plugin_ctx(tmp_path: Path, telemetry: MockTelemetry | None) -> PluginContext:
    return PluginContext(_runtime_ctx(tmp_path, telemetry))


def _batch() -> PipelineBatch:
    return PipelineBatch(
        batch_id="batch-1",
        data_path=Path("."),
        items=["file-1"],
        size_bytes=1,
        files_count=1,
    )


@pytest.mark.unit
def test_batch_span_emitted(tmp_path):
    telemetry = MockTelemetry()
    batch = _batch()
    ctx = _plugin_ctx(tmp_path, telemetry)
    host = MagicMock(spec=PipelineHost)
    host._process_batch.return_value = PipelineResult(
        batch=batch, outputs=OutputPaths(primary="out"), success=True
    )

    _process_batch_timed(host, batch, ctx)

    assert "firecube.batch" in telemetry.names
    assert telemetry.spans[0][1] == {"firecube.batch_id": "batch-1"}


@pytest.mark.unit
def test_phase_spans_emitted(tmp_path, monkeypatch):
    from firecube.core.config import StorageConfig

    telemetry = MockTelemetry()
    ctx = _plugin_ctx(tmp_path, telemetry)
    batch = _batch()
    chunk_manager = SimpleNamespace(
        storage_config=StorageConfig(storage_type="local", storage_driver="fsspec"),
        acquire_claim=lambda **kwargs: nullcontext(),
    )
    ingestor = _DummyZarrIngestor(name="dummy_zarr", chunk_manager=chunk_manager)
    ingestor.engine_config = EngineConfig(write_mode="staged")
    ingestor.template_config = None

    monkeypatch.setattr(generic_module, "fs_kwargs_for_uri", lambda *args, **kwargs: {})

    def fake_append_time_groups(**kwargs):
        kwargs["dataset_for_batch"]("default", ["file-1"])
        return {"coverage": ["default"]}

    monkeypatch.setattr(
        "firecube.ingestor.runtime.zarr.append.append_time_groups", fake_append_time_groups
    )

    result = ingestor._process_batch(batch, ctx)

    assert result.success is True
    assert "firecube.batch.prepare" in telemetry.names
    assert "firecube.batch.zarr_write" in telemetry.names


@pytest.mark.unit
def test_spans_safe_with_none_telemetry(tmp_path, monkeypatch):
    from firecube.core.config import StorageConfig

    batch = _batch()
    ctx = _plugin_ctx(tmp_path, None)
    host = MagicMock(spec=PipelineHost)
    host._process_batch.return_value = PipelineResult(
        batch=batch, outputs=OutputPaths(primary="out"), success=True
    )

    result = _process_batch_timed(host, batch, ctx)

    chunk_manager = SimpleNamespace(
        storage_config=StorageConfig(storage_type="local", storage_driver="fsspec"),
        acquire_claim=lambda **kwargs: nullcontext(),
    )
    ingestor = _DummyZarrIngestor(name="dummy_zarr", chunk_manager=chunk_manager)
    ingestor.engine_config = EngineConfig(write_mode="staged")
    ingestor.template_config = None
    monkeypatch.setattr(generic_module, "fs_kwargs_for_uri", lambda *args, **kwargs: {})
    monkeypatch.setattr(
        "firecube.ingestor.runtime.zarr.append.append_time_groups",
        lambda **kwargs: {"coverage": []},
    )

    batch_result = ingestor._process_batch(batch, ctx)

    assert result.success is True
    assert batch_result.success is True


@pytest.mark.unit
def test_finalize_span_emitted(tmp_path):
    telemetry = MockTelemetry()
    executor = PipelineExecutor()
    executor._log = MagicMock()
    ctx = _runtime_ctx(tmp_path, telemetry)
    batch = _batch()
    result = PipelineResult(
        batch=batch, outputs=OutputPaths(primary="s3://bucket/product"), success=True
    )
    state = PipelineRunState(
        product="product",
        pipeline_workers=1,
        batch_size=1,
        batches=(batch,),
        ingestion_start_time=0.0,
        batch_creation_duration=0.0,
        processing_start_time=0.0,
        results=(result,),
    )
    host = MagicMock(spec=PipelineHost)
    host._aggregate_metrics.return_value = {}
    host.name = "dummy"
    host._chunk_manager = None

    final_result = executor.finalize(ctx, state, host)

    assert final_result.output_path == "s3://bucket/product"
    assert "firecube.finalize" in telemetry.names
