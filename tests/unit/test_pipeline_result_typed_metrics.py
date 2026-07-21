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

from firecube.ingestor.api import (
    OutputPaths,
    PipelineBatch,
    PipelineMetrics,
    PipelineResult,
    ResultMetrics,
    StorageMetrics,
)


def _batch() -> PipelineBatch:
    return PipelineBatch(batch_id="batch-1", data_path=Path("/tmp/input"))


def test_result_metrics_defaults() -> None:
    metrics = ResultMetrics()

    assert metrics.write_mode is None
    assert metrics.storage is None
    assert metrics.pipeline is None
    assert metrics.storage_handled is False


def test_storage_metrics_defaults() -> None:
    metrics = StorageMetrics()

    assert metrics.path is None
    assert metrics.bytes == 0
    assert metrics.files == 0
    assert metrics.duration_s == 0.0


def test_pipeline_metrics_defaults() -> None:
    metrics = PipelineMetrics()

    assert metrics.duration_pipeline_s == 0.0
    assert metrics.rows_processed is None
    assert metrics.rows_ingested is None
    assert metrics.coverage == []


def test_output_paths_defaults() -> None:
    outputs = OutputPaths()

    assert outputs.primary is None
    assert outputs.zarr is None


def test_empty_pipeline_result_constructs() -> None:
    result = PipelineResult(batch=_batch(), outputs=OutputPaths())

    assert result.batch.batch_id == "batch-1"
    assert result.outputs.primary is None
    assert result.output_format == "zarr"
    assert result.metrics.write_mode is None


def test_pipeline_result_with_typed_metrics() -> None:
    result = PipelineResult(
        batch=_batch(),
        outputs=OutputPaths(primary=Path("/tmp/out"), zarr=Path("/tmp/out.zarr")),
        metrics=ResultMetrics(
            write_mode="direct",
            storage=StorageMetrics(path="/tmp/out", bytes=12, files=2, duration_s=1.5),
            pipeline=PipelineMetrics(duration_pipeline_s=3.25, rows_processed=9, rows_ingested=8),
            storage_handled=True,
        ),
    )

    assert result.outputs.primary == Path("/tmp/out")
    assert result.outputs.zarr == Path("/tmp/out.zarr")
    assert result.metrics.write_mode == "direct"
    assert result.metrics.storage is not None
    assert result.metrics.storage.bytes == 12
    assert result.metrics.pipeline is not None
    assert result.metrics.pipeline.rows_ingested == 8
