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

from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import Any

import pytest

from firecube.core.controlplane import ChunkManager
from firecube.ingestor.api import BaseIngestor, OutputPaths
from firecube.ingestor.runtime.engine import PipelineFailedBatchesError
from firecube.ingestor.types.context import (
    IngestContext,
    PipelineBatch,
    PipelineResult,
    PluginContext,
    RuntimeIngestContext,
    StorageContext,
)
from tests.helpers.storage import make_test_binding, make_test_session


class _MixedBatchIngestor(BaseIngestor):
    PRODUCT_NAME = "mixed_batch_wal"
    name = "mixed_batch_wal"

    def discover_source_files(self, ctx: PluginContext) -> Iterable[str]:
        source = Path(ctx.source)
        return [str(source / "ok.nc"), str(source / "boom.nc")]

    def _process_batch(self, batch: PipelineBatch, ctx: PluginContext) -> PipelineResult:
        if any("boom.nc" in str(item) for item in batch.items):
            raise RuntimeError("simulated failed batch")
        return PipelineResult(
            batch=batch,
            outputs=OutputPaths(primary=str(ctx.target or batch.data_path)),
            success=True,
        )

    def _aggregate_metrics(
        self,
        ctx: RuntimeIngestContext,
        state,
    ) -> Mapping[str, Any]:
        _ = (ctx, state)
        return {}


@pytest.mark.integration
def test_mixed_batches_yield_failed_status(tmp_path: Path) -> None:
    product = "mixed-batch-product.zarr"
    run_id = "mixed-batch-run"
    source_dir = tmp_path / "source"
    source_dir.mkdir()
    (source_dir / "ok.nc").touch()
    (source_dir / "boom.nc").touch()

    chunk_manager = ChunkManager(
        binding=make_test_binding(tmp_path, product=product),
        workspace=tmp_path / "cm-work",
    )
    ingestor = _MixedBatchIngestor(chunk_manager=chunk_manager)  # type: ignore[abstract]
    session = make_test_session(tmp_path, product=product)
    ctx = IngestContext(
        source=str(source_dir),
        target=session.product.product_uri.to_str(),
        output_format="zarr",
        options={
            "pipeline_parallel": False,
            "pipeline_batch_size": 1,
            "write_mode": "direct",
            "no_progress": True,
        },
        storage=StorageContext(output=session),
        run_id=run_id,
    )

    with pytest.raises(PipelineFailedBatchesError, match="simulated failed batch"):
        ingestor.run(ctx)

    runs = chunk_manager.list_runs(product=product, status="failed")
    assert [run.run_id for run in runs] == [run_id]
    assert runs[0].status == "failed"
