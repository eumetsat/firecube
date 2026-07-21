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

from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest

from firecube.ingestor.contracts.interfaces import PipelineHost
from firecube.ingestor.runtime.engine import PipelineExecutor, PipelineFailedBatchesError
from firecube.ingestor.types.context import (
    IngestContext,
    OutputPaths,
    PipelineBatch,
    PipelineResult,
    PipelineRunState,
    RuntimeIngestContext,
)


def _runtime_ctx(tmp_path: Path) -> RuntimeIngestContext:
    return RuntimeIngestContext.from_ingest_context(
        IngestContext(source=str(tmp_path), target="fallback.zarr", output_format="zarr"),
        run_id="run-001",
        temp_root=tmp_path,
        materializer=lambda p: Path(p),
    )


def _batch(batch_id: str) -> PipelineBatch:
    return PipelineBatch(
        batch_id=batch_id,
        data_path=Path("."),
        items=[],
        size_bytes=0,
        files_count=0,
    )


def _state(*results: PipelineResult) -> PipelineRunState:
    return PipelineRunState(
        product="dummy",
        pipeline_workers=1,
        batch_size=1,
        batches=tuple(result.batch for result in results),
        ingestion_start_time=0.0,
        batch_creation_duration=0.0,
        processing_start_time=0.0,
        results=results,
    )


def _host() -> Any:
    host = MagicMock(spec=PipelineHost)
    host._aggregate_metrics.return_value = {}
    host.name = "dummy"
    host._chunk_manager = None
    return host


def test_finalize_raises_when_any_batch_failed(tmp_path: Path) -> None:
    success_batch = _batch("b1")
    failed_batch = _batch("b2")
    state = _state(
        PipelineResult(
            batch=success_batch, outputs=OutputPaths(primary="s3://bucket/product"), success=True
        ),
        PipelineResult(
            batch=failed_batch, outputs=OutputPaths(primary=Path("")), success=False, error="boom"
        ),
    )

    with pytest.raises(PipelineFailedBatchesError, match="boom"):
        PipelineExecutor().finalize(_runtime_ctx(tmp_path), state, _host())


def test_finalize_passes_when_all_batches_succeed(tmp_path: Path) -> None:
    first_batch = _batch("b1")
    second_batch = _batch("b2")
    state = _state(
        PipelineResult(
            batch=first_batch, outputs=OutputPaths(primary="s3://bucket/product"), success=True
        ),
        PipelineResult(
            batch=second_batch, outputs=OutputPaths(primary="local-product.zarr"), success=True
        ),
    )

    result = PipelineExecutor().finalize(_runtime_ctx(tmp_path), state, _host())

    assert result.output_path == "s3://bucket/product"
