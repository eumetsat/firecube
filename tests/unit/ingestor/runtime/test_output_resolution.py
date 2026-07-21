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
from tests.helpers.storage import make_test_binding, make_test_context

from firecube.core.controlplane import ChunkManager
from firecube.ingestor.api import IngestResult, OutputPaths
from firecube.ingestor.errors import ConfigurationError
from firecube.ingestor.runtime.base import BaseIngestor
from firecube.ingestor.types.context import IngestContext, PluginContext, RuntimeIngestContext


class _DummyIngestor(BaseIngestor):
    PRODUCT_NAME = "dummy"
    name = "dummy"

    def ingest(self, ctx: IngestContext) -> IngestResult:  # pragma: no cover - not used
        return IngestResult(
            outputs=OutputPaths(primary=str(ctx.target or "")), output_format="zarr"
        )

    def _process_batch(self, batch, ctx):  # pragma: no cover - not used
        raise NotImplementedError

    def _aggregate_metrics(self, ctx, state):  # pragma: no cover - not used
        return {}


def _runtime_ctx(*, temp_root: Path) -> RuntimeIngestContext:
    ctx = make_test_context(
        temp_root,
        source=str(temp_root),
        product="dummy.zarr",
        protocol="s3",
        authority="bucket",
    )
    return RuntimeIngestContext.from_ingest_context(
        ctx,
        run_id="run-001",
        temp_root=temp_root,
        materializer=lambda p: Path(p),
    )


def test_direct_output_resolution_fails_if_chunk_manager_binding_is_stale(tmp_path):
    ingestor = _DummyIngestor()
    runtime_ctx = _runtime_ctx(temp_root=tmp_path)

    ingestor._chunk_manager = ChunkManager(binding=make_test_binding(tmp_path), workspace=tmp_path)

    with pytest.raises(ConfigurationError, match="bound to a different output root"):
        ingestor.resolve_output_uri(PluginContext(runtime_ctx), write_mode="direct")
