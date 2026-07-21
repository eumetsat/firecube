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

"""Typed-output contract for plugin result constructors.

The public contract is ``PipelineResult(outputs=OutputPaths(primary=...))``;
the legacy ``output_path=`` constructor kwarg was removed so the code matches
the documented contract. The read-only ``output_path`` property remains as a
compatibility view for readers.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from firecube.ingestor.types.context import (
    IngestResult,
    OutputPaths,
    PipelineBatch,
    PipelineResult,
)

pytestmark = pytest.mark.unit


def _batch() -> PipelineBatch:
    return PipelineBatch(batch_id="b1", data_path=Path("/tmp/b1"))


class TestLegacyConstructorRejected:
    def test_pipeline_result_rejects_output_path_kwarg(self) -> None:
        with pytest.raises(TypeError, match="output_path"):
            PipelineResult(batch=_batch(), output_path="/data/p.zarr")  # type: ignore[call-arg]

    def test_ingest_result_rejects_output_path_kwarg(self) -> None:
        with pytest.raises(TypeError, match="output_path"):
            IngestResult(output_format="zarr", output_path="/data/p.zarr")  # type: ignore[call-arg]


class TestTypedConstruction:
    def test_pipeline_result_output_path_property_reads_primary(self) -> None:
        result = PipelineResult(batch=_batch(), outputs=OutputPaths(primary="/data/p.zarr"))
        assert result.output_path == "/data/p.zarr"
        assert result.outputs.primary == "/data/p.zarr"

    def test_zarr_output_mirrored_from_primary(self) -> None:
        result = PipelineResult(batch=_batch(), outputs=OutputPaths(primary="/data/p.zarr"))
        assert result.outputs.zarr == "/data/p.zarr"

    def test_non_zarr_format_does_not_mirror(self) -> None:
        result = PipelineResult(
            batch=_batch(),
            output_format="parquet",
            outputs=OutputPaths(primary="/data/p.parquet"),
        )
        assert result.outputs.zarr is None

    def test_ingest_result_output_path_property_reads_primary(self) -> None:
        result = IngestResult(output_format="zarr", outputs=OutputPaths(primary="/data/p.zarr"))
        assert result.output_path == "/data/p.zarr"
        assert result.outputs.zarr == "/data/p.zarr"
