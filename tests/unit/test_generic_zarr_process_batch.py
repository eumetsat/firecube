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
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from firecube.ingestor.config.engine import EngineConfig
from firecube.ingestor.runtime.zarr import batch_runner
from firecube.ingestor.templates import generic as generic_module
from firecube.ingestor.templates.generic import GenericZarrIngestor
from firecube.ingestor.types.context import (
    IngestContext,
    PipelineBatch,
    PluginContext,
    RuntimeIngestContext,
)


class _DummyZarrIngestor(GenericZarrIngestor):
    PRODUCT_NAME = "dummy_zarr"

    def build_dataset(self, group, items, ctx):  # pragma: no cover - strategy is mocked
        _ = (group, items, ctx)
        return None


class _FakeStrategy:
    def __init__(self, metrics=None, exc: Exception | None = None):
        self._metrics = metrics or {"coverage": ["F024"]}
        self._exc = exc

    def write_groups(self, *, group_to_timestamps, dataset_for_batch, batch_size, claim_for_group):
        _ = (group_to_timestamps, dataset_for_batch, batch_size, claim_for_group)
        if self._exc is not None:
            raise self._exc
        return self._metrics


def _make_subject(monkeypatch: pytest.MonkeyPatch, *, write_mode: str = "direct"):
    ingestor = _DummyZarrIngestor(name="dummy_zarr")
    ingestor.engine_config = EngineConfig(write_mode=write_mode)
    ingestor.template_config = None

    batch = PipelineBatch(
        batch_id="batch-1",
        data_path=Path("."),
        items=["source.nc"],
        metadata={},
    )
    runtime_ctx = RuntimeIngestContext.from_ingest_context(
        IngestContext(
            source=".", target="out.zarr", output_format="zarr", options={"resume_existing": True}
        ),
        run_id="run-1",
        temp_root=None,
        materializer=lambda source: Path(source),
    )
    ctx = PluginContext(runtime_ctx)

    monkeypatch.setattr(ingestor, "batch_setup", lambda ctx: None)
    monkeypatch.setattr(ingestor, "prepare_batch_data", lambda batch, ctx: {"prepared": True})
    monkeypatch.setattr(ingestor, "get_batch_groups", lambda batch, ctx: ["F024"])
    monkeypatch.setattr(ingestor, "get_zarr_config", lambda ctx: {})
    monkeypatch.setattr(ingestor, "batch_teardown", lambda ctx: None)
    monkeypatch.setattr(
        ingestor,
        "resolve_output_uri",
        lambda ctx, write_mode: "staged.zarr" if write_mode == "staged" else "final.zarr",
    )
    return ingestor, batch, ctx


def _route_legacy_append_strategy(
    monkeypatch: pytest.MonkeyPatch, strategy_mock: MagicMock
) -> None:
    def _legacy_factory(**kwargs):
        _ = kwargs
        return strategy_mock(
            store_uri="final.zarr",
            final_target_uri=None,
            zarr_config={},
            resume_existing=True,
            force_reingest=False,
            chunk_manager=SimpleNamespace(storage_config=SimpleNamespace()),
            session=None,
            logger=MagicMock(),
        )

    monkeypatch.setattr(generic_module, "AppendStrategy", _legacy_factory, raising=False)


@pytest.mark.unit
def test_process_batch_calls_batch_runner_strategy(monkeypatch: pytest.MonkeyPatch) -> None:
    ingestor, batch, ctx = _make_subject(monkeypatch)
    strategy_mock = MagicMock(return_value=_FakeStrategy())

    monkeypatch.setattr(batch_runner, "build_append_strategy", strategy_mock)
    _route_legacy_append_strategy(monkeypatch, strategy_mock)

    result = ingestor._process_batch(batch, ctx)

    assert result.success is True
    assert result.outputs.primary == "final.zarr"
    assert result.metrics["count"] == 1
    assert result.metrics["coverage"] == ["F024"]
    assert result.metrics["zarr"] == {"coverage": ["F024"]}


@pytest.mark.unit
def test_process_batch_preserves_exception_path_cleanup(monkeypatch: pytest.MonkeyPatch) -> None:
    ingestor, batch, ctx = _make_subject(monkeypatch)
    cleanup = MagicMock()
    strategy_mock = MagicMock(return_value=_FakeStrategy(exc=RuntimeError("boom")))

    monkeypatch.setattr(ingestor, "cleanup_batch_data", cleanup)
    monkeypatch.setattr(batch_runner, "build_append_strategy", strategy_mock)
    _route_legacy_append_strategy(monkeypatch, strategy_mock)

    result = ingestor._process_batch(batch, ctx)

    assert result.success is False
    assert "boom" in str(result.error)
    cleanup.assert_called_once_with(batch, ctx)


@pytest.mark.unit
def test_process_batch_direct_mode_leaves_staged_metadata_unseeded(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ingestor, batch, ctx = _make_subject(monkeypatch, write_mode="direct")
    strategy_mock = MagicMock(return_value=_FakeStrategy())

    monkeypatch.setattr(batch_runner, "build_append_strategy", strategy_mock)
    _route_legacy_append_strategy(monkeypatch, strategy_mock)

    result = ingestor._process_batch(batch, ctx)

    assert result.success is True
    assert result.outputs.primary == "final.zarr"
    assert result.metrics["storage_handled"] is True


@pytest.mark.unit
def test_process_batch_result_metrics_shape(monkeypatch: pytest.MonkeyPatch) -> None:
    ingestor, batch, ctx = _make_subject(monkeypatch, write_mode="direct")
    strategy_mock = MagicMock(return_value=_FakeStrategy(metrics={"coverage": ["F024"], "rows": 1}))

    monkeypatch.setattr(batch_runner, "build_append_strategy", strategy_mock)
    _route_legacy_append_strategy(monkeypatch, strategy_mock)

    result = ingestor._process_batch(batch, ctx)

    assert result.success is True
    assert result.metrics["count"] == 1
    assert result.metrics["storage_handled"] is True
    assert result.metrics["coverage"] == ["F024"]
    assert result.metrics["zarr"] == {"coverage": ["F024"], "rows": 1}
