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
from typing import Any, cast
from unittest.mock import MagicMock, call

import numpy as np
import pytest

from firecube.core.controlplane import ChunkManager
from firecube.core.product import resolve_dataset_target
from firecube.core.storage.completion import StorageCompleter
from firecube.ingestor.api import IngestResult, OutputPaths
from firecube.ingestor.config.engine import EngineConfig
from firecube.ingestor.runtime.base import BaseIngestor
from firecube.ingestor.runtime.zarr import batch_runner
from firecube.ingestor.templates import generic as generic_module
from firecube.ingestor.templates.direct_zarr import (
    DirectZarrIngestor,
    WriteIntent,
    ZarrArraySpec,
    ZarrGroupSpec,
)
from firecube.ingestor.types.context import (
    IngestContext,
    PipelineBatch,
    PluginContext,
    RuntimeIngestContext,
)
from tests.helpers.storage import make_test_binding, make_test_context


class _OutputResolutionIngestor(BaseIngestor):
    PRODUCT_NAME = "dummy"
    name = "dummy"

    def ingest(self, ctx: IngestContext) -> IngestResult:  # pragma: no cover - not used
        return IngestResult(
            outputs=OutputPaths(primary=str(ctx.target or "")), output_format="zarr"
        )

    def _process_batch(self, batch: Any, ctx: Any) -> Any:  # pragma: no cover - not used
        raise NotImplementedError

    def _aggregate_metrics(self, ctx: Any, state: Any) -> dict[str, Any]:  # pragma: no cover
        return {}


class _DirectMetricsIngestor(DirectZarrIngestor):
    PRODUCT_NAME = "direct_metrics"
    name = "direct_metrics"

    def zarr_schema(self, ctx: PluginContext) -> list[ZarrGroupSpec]:
        return [
            ZarrGroupSpec(
                group="data",
                arrays=[ZarrArraySpec(name="timestamp", shape=(0,), dtype="datetime64[s]")],
            )
        ]

    def build_write_intents(self, batch: PipelineBatch, ctx: PluginContext) -> list[WriteIntent]:
        return [
            WriteIntent(
                group="data",
                array="timestamp",
                ts_index=0,
                data=None,
                kind="timestamp",
                timestamp_val=np.datetime64("2026-01-01T00:00:00", "s"),
            )
        ]


def _plugin_ctx(tmp_path: Path) -> PluginContext:
    runtime_ctx = RuntimeIngestContext.from_ingest_context(
        IngestContext(source=".", target="out.zarr", output_format="zarr"),
        run_id="run-1",
        temp_root=tmp_path,
        materializer=lambda source: Path(source),
    )
    return PluginContext(runtime_ctx)


def test_staged_resolves_relative_dataset_targets_to_workspace(tmp_path: Path) -> None:
    assert resolve_dataset_target("relative.zarr", write_mode="staged", temp_root=tmp_path) == str(
        tmp_path / "relative.zarr"
    )
    assert (
        resolve_dataset_target(
            "relative.zarr",
            write_mode="direct",
            direct_base_uri="s3://bucket/products",
        )
        == "s3://bucket/products/relative.zarr"
    )


def test_base_output_resolution_staged_uses_workspace_product_name(tmp_path: Path) -> None:
    ingestor = _OutputResolutionIngestor()
    ctx = _plugin_ctx(tmp_path)

    assert ingestor.resolve_output_uri(ctx, write_mode="staged") == str(tmp_path / "dummy")


def test_base_output_resolution_direct_uses_bound_product_uri(tmp_path: Path) -> None:
    ingestor = _OutputResolutionIngestor()
    runtime_ctx = make_test_context(
        tmp_path,
        source=str(tmp_path),
        product="dummy.zarr",
        protocol="s3",
        authority="bucket",
    )
    ctx = RuntimeIngestContext.from_ingest_context(
        runtime_ctx,
        run_id="run-1",
        temp_root=tmp_path,
        materializer=lambda source: Path(source),
    )
    ingestor._chunk_manager = ChunkManager(
        binding=make_test_binding(
            tmp_path,
            product="dummy.zarr",
            protocol="s3",
            authority="bucket",
        ),
        workspace=tmp_path,
    )

    assert ingestor.resolve_output_uri(PluginContext(ctx), write_mode="direct") == (
        "s3://bucket/dummy.zarr"
    )


def test_generic_staged_mode_resolves_final_target_for_metadata_seeding() -> None:
    ingestor = SimpleNamespace(
        resolve_output_uri=MagicMock(side_effect=["workspace.zarr", "final.zarr"]),
        _log=MagicMock(),
    )
    ctx = MagicMock()

    store_uri, final_target_uri = generic_module._resolve_zarr_batch_targets(
        ingestor, ctx, "staged"
    )

    assert (store_uri, final_target_uri) == ("workspace.zarr", "final.zarr")
    assert ingestor.resolve_output_uri.call_args_list == [
        call(ctx, write_mode="staged"),
        call(ctx, write_mode="direct"),
    ]


@pytest.mark.parametrize(("write_mode", "storage_handled"), [("staged", False), ("direct", True)])
def test_batch_runner_storage_handled_metric_matches_write_mode(
    write_mode: str, storage_handled: bool
) -> None:
    metrics = batch_runner.assemble_batch_metrics(
        prep_metrics={},
        zarr_metrics={"coverage": ["data"]},
        file_count=1,
        write_mode=write_mode,
    )

    assert metrics["storage_handled"] is storage_handled


@pytest.mark.parametrize(("write_mode", "storage_handled"), [("staged", False), ("direct", True)])
def test_direct_zarr_storage_handled_metric_matches_write_mode(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    write_mode: str,
    storage_handled: bool,
) -> None:
    ingestor = _DirectMetricsIngestor()
    ingestor.engine_config = EngineConfig(write_mode=write_mode)
    cast(Any, ingestor)._chunk_manager = SimpleNamespace(
        storage_config=object(), acquire_claim=MagicMock(return_value=object())
    )
    strategy = SimpleNamespace(write_groups=MagicMock(return_value={"coverage": ["data"]}))
    monkeypatch.setattr(
        "firecube.ingestor.api.IndexedRegionStrategy", MagicMock(return_value=strategy)
    )
    monkeypatch.setattr(
        ingestor, "resolve_output_uri", MagicMock(return_value=str(tmp_path / "out.zarr"))
    )
    batch = PipelineBatch(batch_id="batch-1", data_path=tmp_path, items=["source.nc"])
    ctx = _plugin_ctx(tmp_path)

    result = ingestor._process_batch(batch, ctx)

    assert result.success is True
    assert result.metrics["storage_handled"] is storage_handled


@pytest.mark.parametrize(("write_mode", "route"), [("direct", "direct"), ("staged", "staged")])
def test_remote_storage_completion_routes_by_write_mode(
    monkeypatch: pytest.MonkeyPatch, write_mode: str, route: str
) -> None:
    completer = StorageCompleter()
    observed: list[str] = []
    monkeypatch.setattr(
        completer,
        "complete_s3_direct",
        lambda result, storage_config, final_target_uri: (
            observed.append("direct") or "direct-result"
        ),
    )
    monkeypatch.setattr(
        completer,
        "complete_s3_staged",
        lambda result, ctx, final_target_uri: observed.append("staged") or "staged-result",
    )
    product_uri = SimpleNamespace(to_str=lambda: "s3://bucket/product.zarr")
    ctx = SimpleNamespace(
        storage=SimpleNamespace(
            output=SimpleNamespace(product=SimpleNamespace(product_uri=product_uri))
        )
    )

    result = completer.complete_s3_storage(object(), ctx, MagicMock(), write_mode)

    assert result == f"{route}-result"
    assert observed == [route]
