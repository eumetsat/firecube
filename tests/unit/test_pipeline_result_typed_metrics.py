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

from firecube.sdk import (
    OutputPaths,
    PipelineBatch,
    PipelineMetrics,
    PipelineResult,
    ResultMetrics,
    StorageMetrics,
)


def _batch() -> PipelineBatch:
    return PipelineBatch(batch_id="batch-1", data_path=Path("/tmp/input"))


_DEFAULT_ROWS = (
    (
        "result_metrics_defaults",
        ResultMetrics(),
        {"write_mode": None, "storage": None, "pipeline": None, "storage_handled": False},
    ),
    (
        "storage_metrics_defaults",
        StorageMetrics(),
        {"path": None, "bytes": 0, "files": 0, "duration_s": 0.0},
    ),
    (
        "pipeline_metrics_defaults",
        PipelineMetrics(),
        {
            "duration_pipeline_s": 0.0,
            "rows_processed": None,
            "rows_ingested": None,
            "coverage": [],
        },
    ),
    ("output_paths_defaults", OutputPaths(), {"primary": None, "zarr": None}),
)


def test_typed_result_defaults(subtests) -> None:
    for row_id, obj, expected in _DEFAULT_ROWS:
        with subtests.test(msg=f"test_{row_id}"):
            for name, want in expected.items():
                got = getattr(obj, name)
                assert type(got) is type(want) and got == want, (name, got)


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
