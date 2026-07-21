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
from typing import Any, cast

from firecube.ingestor.extensions.duck import DuckDbMixin
from firecube.ingestor.runtime.base import BaseIngestor
from firecube.ingestor.types.context import (
    IngestContext,
    OutputPaths,
    PipelineBatch,
    PipelineResult,
    PluginContext,
    RuntimeIngestContext,
)


class _MinimalBatchIngestor(BaseIngestor):
    PRODUCT_NAME = "minimal_batch"
    name = "minimal_batch"

    def _process_batch(self, batch: PipelineBatch, ctx: PluginContext) -> PipelineResult:
        return PipelineResult(batch=batch, outputs=OutputPaths(primary=""), success=True)

    def _aggregate_metrics(self, ctx, state):
        _ = (ctx, state)
        return {}


def test_generic_batch_hooks_are_noop_when_parent_hook_missing():
    ingestor = _MinimalBatchIngestor()
    runtime_ctx = RuntimeIngestContext.from_ingest_context(
        IngestContext(source=".", target="out.zarr", output_format="zarr"),
        run_id="test-run",
        temp_root=Path("."),
        materializer=lambda p: Path(p),
    )
    ctx = PluginContext(runtime_ctx)

    # No parent hook implementation exists above BaseIngestor.
    # The cooperative getattr(super(), ...) chain is intentionally a no-op.
    ingestor.batch_setup(ctx)
    ingestor.batch_teardown(ctx)


class _ParentHooks:
    def __init__(self) -> None:
        self.parent_setup_called = False
        self.parent_teardown_called = False

    def batch_setup(self, ctx) -> None:
        _ = ctx
        self.parent_setup_called = True

    def batch_teardown(self, ctx) -> None:
        _ = ctx
        self.parent_teardown_called = True


class _DuckWithParent(DuckDbMixin, _ParentHooks):
    def __init__(self) -> None:
        super().__init__()
        self.setup_called = False
        self.teardown_called = False

    def setup_duckdb(self, workspace=None, options=None, in_memory=True) -> None:
        _ = (workspace, options, in_memory)
        self.setup_called = True
        self.con = cast(Any, object())  # satisfy DuckDbMixin.prepare_duckdb_schema(self.con, ...)

    def teardown_duckdb(self) -> None:
        self.teardown_called = True

    def prepare_duckdb_schema(self, con, ctx) -> None:
        _ = (con, ctx)


def test_duckdb_mixin_cooperatively_calls_parent_hooks():
    ingestor = _DuckWithParent()
    runtime_ctx = RuntimeIngestContext.from_ingest_context(
        IngestContext(source=".", target="out.zarr", output_format="zarr", in_memory=True),
        run_id="test-run",
        temp_root=Path("."),
        materializer=lambda p: Path(p),
    )
    ctx = PluginContext(runtime_ctx)

    ingestor.batch_setup(ctx)
    ingestor.batch_teardown(ctx)

    assert ingestor.setup_called is True
    assert ingestor.teardown_called is True
    assert ingestor.parent_setup_called is True
    assert ingestor.parent_teardown_called is True
