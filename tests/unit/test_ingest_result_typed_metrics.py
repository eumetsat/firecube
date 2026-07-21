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
    IngestResult,
    OutputPaths,
    PipelineMetrics,
    ResultMetrics,
    StorageMetrics,
)


def test_empty_ingest_result_constructs() -> None:
    result = IngestResult(outputs=OutputPaths(), output_format="zarr")

    assert result.output_format == "zarr"
    assert result.outputs.primary is None
    assert result.metrics.write_mode is None


def test_ingest_result_all_outputs() -> None:
    result = IngestResult(
        outputs=OutputPaths(primary=Path("/tmp/product"), zarr=Path("/tmp/product.zarr")),
        output_format="zarr",
    )

    outputs = result.all_outputs()

    assert isinstance(outputs, OutputPaths)
    assert outputs.primary == Path("/tmp/product")
    assert outputs.get("zarr") == Path("/tmp/product.zarr")


def test_ingest_result_with_metrics() -> None:
    result = IngestResult(
        outputs=OutputPaths(primary=Path("/tmp/product"), zarr=Path("/tmp/product.zarr")),
        output_format="zarr",
        metrics=ResultMetrics(
            write_mode="append",
            storage=StorageMetrics(path="/tmp/product.zarr", bytes=99, files=3, duration_s=2.5),
            pipeline=PipelineMetrics(duration_pipeline_s=4.0, rows_processed=10, rows_ingested=9),
            storage_handled=True,
        ),
        registered=True,
        spans_recorded=True,
    )

    assert result.metrics.write_mode == "append"
    assert result.metrics.storage is not None
    assert result.metrics.storage.files == 3
    assert result.metrics.pipeline is not None
    assert result.metrics.pipeline.rows_processed == 10
    assert result.registered is True
    assert result.spans_recorded is True
