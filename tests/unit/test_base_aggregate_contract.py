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

import pytest

from firecube.ingestor.runtime.base import BaseIngestor
from firecube.ingestor.types.context import (
    IngestContext,
    OutputPaths,
    PipelineBatch,
    PipelineResult,
    PipelineRunState,
    RuntimeIngestContext,
)


class _MissingAggregateIngestor(BaseIngestor):
    PRODUCT_NAME = "missing_aggregate_test"

    def _process_batch(self, batch, ctx):
        return PipelineResult(
            batch=batch, outputs=OutputPaths(primary=str(ctx.target or "")), success=True
        )


class _HelperAggregateIngestor(BaseIngestor):
    PRODUCT_NAME = "helper_aggregate_test"

    def _process_batch(self, batch, ctx):
        return PipelineResult(
            batch=batch, outputs=OutputPaths(primary=str(ctx.target or "")), success=True
        )

    def _aggregate_metrics(self, ctx, state):
        return self.default_aggregate_metrics(ctx, state)


@pytest.mark.unit
def test_base_ingestor_provides_default_aggregate_metrics():
    ingestor = _MissingAggregateIngestor(name="missing_aggregate")
    ctx = RuntimeIngestContext.from_ingest_context(
        IngestContext(source=str(Path(".")), target="out.zarr", output_format="zarr"),
        run_id="test-run",
        temp_root=Path("."),
        materializer=lambda path: Path(path),
    )
    state = PipelineRunState(
        product="dummy",
        pipeline_workers=1,
        batch_size=1,
        batches=(),
        ingestion_start_time=0.0,
        batch_creation_duration=0.0,
        processing_start_time=0.0,
        results=(),
    )

    assert ingestor._aggregate_metrics(ctx, state) == {"zarr": {"coverage": []}}


@pytest.mark.unit
def test_default_aggregate_metrics_helper_merges_successful_results():
    ingestor = _HelperAggregateIngestor(name="helper_aggregate")  # pyright: ignore[reportAbstractUsage]
    ctx = IngestContext(source=str(Path(".")), target="out.zarr", output_format="zarr")
    batch = PipelineBatch(batch_id="b1", data_path=Path("."))
    failed = PipelineResult(
        batch=batch,
        outputs=OutputPaths(primary=Path(".")),
        success=False,
        metrics={"rows": 99},
        error="failed",
    )
    state = PipelineRunState(
        product="dummy",
        pipeline_workers=1,
        batch_size=1,
        batches=(batch,),
        ingestion_start_time=0.0,
        batch_creation_duration=0.0,
        processing_start_time=0.0,
        results=(
            PipelineResult(
                batch=batch,
                outputs=OutputPaths(primary=Path(".")),
                success=True,
                metrics={
                    "rows": 10,
                    "tags": ["a"],
                    "zarr": {"coverage": [{"group": "g1"}]},
                    "meta": "first",
                },
            ),
            PipelineResult(
                batch=batch,
                outputs=OutputPaths(primary=Path(".")),
                success=True,
                metrics={
                    "rows": 5,
                    "tags": ["b"],
                    "zarr": {"coverage": [{"group": "g2"}]},
                    "meta": "ignored_second",
                },
            ),
            failed,
        ),
    )

    merged = ingestor._aggregate_metrics(ctx, state)  # pyright: ignore[reportArgumentType]

    assert merged["rows"] == 15
    assert merged["tags"] == ["a", "b"]
    assert merged["meta"] == "first"
    assert merged["zarr"]["coverage"] == [{"group": "g1"}, {"group": "g2"}]
