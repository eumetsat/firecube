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

from contextlib import nullcontext
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock

import pytest

from firecube.ingestor.config.engine import EngineConfig
from firecube.ingestor.runtime.zarr import batch_runner as batch_runner_module
from firecube.ingestor.templates.generic import GenericZarrIngestor
from firecube.ingestor.types.context import (
    IngestContext,
    IngestResult,
    PipelineBatch,
    PluginContext,
    RuntimeIngestContext,
)


class _DummyZarrIngestor(GenericZarrIngestor):
    PRODUCT_NAME = "dummy_zarr"

    def build_dataset(self, group, items, ctx):  # pragma: no cover - not needed here
        _ = (group, items, ctx)
        return None

    def ingest(self, ctx: IngestContext) -> IngestResult:
        _ = ctx
        raise NotImplementedError


class _FakeAppendStrategy:
    def __init__(self, **kwargs: Any) -> None:
        self.kwargs = kwargs

    def write_groups(self, *, group_to_timestamps, dataset_for_batch, batch_size, claim_for_group):
        _ = (group_to_timestamps, dataset_for_batch, batch_size)
        claim_for_group("F024")
        return {"coverage": ["F024"]}


def _run_process_batch(monkeypatch: pytest.MonkeyPatch):
    chunk_manager = SimpleNamespace(
        storage_config=SimpleNamespace(),
        acquire_claim=MagicMock(return_value=nullcontext()),
    )
    ingestor = _DummyZarrIngestor(name="dummy_zarr", chunk_manager=chunk_manager)
    ingestor.engine_config = EngineConfig(write_mode="direct")
    ingestor.template_config = None

    batch = PipelineBatch(
        batch_id="batch-1",
        data_path=Path("."),
        items=["source.nc"],
        metadata={},
    )
    runtime_ctx = RuntimeIngestContext.from_ingest_context(
        IngestContext(source=".", target="out.zarr", output_format="zarr"),
        run_id="run-1",
        temp_root=None,
        materializer=lambda source: Path(source),
    )
    ctx = PluginContext(runtime_ctx)

    monkeypatch.setattr(ingestor, "batch_setup", lambda ctx: None)
    monkeypatch.setattr(ingestor, "prepare_batch_data", lambda batch, ctx: {})
    monkeypatch.setattr(ingestor, "resolve_output_uri", lambda ctx, write_mode: "out.zarr")
    monkeypatch.setattr(ingestor, "get_batch_groups", lambda batch, ctx: ["F024"])
    monkeypatch.setattr(ingestor, "get_zarr_config", lambda ctx: {})
    monkeypatch.setattr(batch_runner_module, "AppendStrategy", _FakeAppendStrategy)

    result = ingestor._process_batch(batch, ctx)
    return result, chunk_manager.acquire_claim


@pytest.mark.unit
def test_generic_zarr_ingestor_uses_zarr_append_category(monkeypatch):
    _, acquire_claim = _run_process_batch(monkeypatch)

    assert acquire_claim.call_count == 1
    call = acquire_claim.call_args
    assert call.kwargs["domain"].category == "zarr_append"
    assert call.kwargs["domain"].name == "F024"


@pytest.mark.unit
def test_generic_zarr_claim_owner_id_pattern(monkeypatch):
    _, acquire_claim = _run_process_batch(monkeypatch)

    assert acquire_claim.call_count == 1
    assert acquire_claim.call_args.kwargs["owner_id"] == "run-1:F024"
