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

import pytest

from firecube.ingestor.runtime.base import BaseIngestor
from firecube.ingestor.types.context import (
    IngestContext,
    OutputPaths,
    PipelineResult,
    StorageContext,
)
from tests.helpers.storage import make_test_session


class _NoopIngestor(BaseIngestor):
    PRODUCT_NAME = "noop_ctx_contract"

    def _process_batch(self, batch, ctx):
        return PipelineResult(
            batch=batch, outputs=OutputPaths(primary=str(ctx.target or "")), success=True
        )

    def _aggregate_metrics(self, ctx, state):
        return {}


@pytest.mark.unit
def test_run_does_not_mutate_caller_context(tmp_path):
    ingestor = _NoopIngestor(name="noop_ctx_contract")  # pyright: ignore[reportAbstractUsage]
    source = tmp_path / "src"
    source.mkdir(parents=True, exist_ok=True)
    target = tmp_path / "out.zarr"

    storage = StorageContext(output=make_test_session(tmp_path, product="out.zarr"))

    ctx = IngestContext(
        source=str(source),
        target=str(target),
        output_format="zarr",
        options={"run_id": "caller-run-id"},
        storage=storage,
    )
    options_before = dict(ctx.options)

    ingestor.run(ctx)

    assert ctx.options == options_before
    assert ctx.run_id is None
