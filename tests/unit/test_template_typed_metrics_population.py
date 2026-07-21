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

"""T11: template-produced PipelineResult metrics/outputs stay typed."""

from __future__ import annotations

from pathlib import Path

import pytest

from firecube.ingestor.api import (
    OutputPaths,
    PipelineBatch,
    PipelineMetrics,
    PipelineResult,
    ResultMetrics,
    StorageMetrics,
)


def _batch() -> PipelineBatch:
    return PipelineBatch(batch_id="batch-1", data_path=Path("/tmp/in"))


@pytest.mark.unit
def test_empty_result_metrics_does_not_crash_template_consumers() -> None:
    result = PipelineResult(batch=_batch(), outputs=OutputPaths(primary=""), success=True)

    assert isinstance(result.metrics, ResultMetrics)
    assert isinstance(result.outputs, OutputPaths)
    assert result.metrics.write_mode is None
    assert result.metrics.storage is None
    assert result.metrics.pipeline is None
    assert result.metrics.storage_handled is False
    assert result.outputs.primary == ""


@pytest.mark.unit
def test_template_typed_storage_metrics_population_round_trips() -> None:
    storage = StorageMetrics(path="/tmp/x", bytes=42, files=3, duration_s=0.5)
    pipeline = PipelineMetrics(duration_pipeline_s=1.0, rows_processed=10, rows_ingested=10)
    result = PipelineResult(
        batch=_batch(),
        outputs=OutputPaths(primary=Path("/tmp/x"), zarr=Path("/tmp/x.zarr")),
        metrics=ResultMetrics(
            write_mode="staged",
            storage=storage,
            pipeline=pipeline,
            storage_handled=True,
        ),
        success=True,
    )

    assert result.metrics.storage is storage
    assert result.metrics.pipeline is pipeline
    assert result.metrics.write_mode == "staged"
    assert result.outputs.primary == Path("/tmp/x")
    assert result.outputs.zarr == Path("/tmp/x.zarr")
