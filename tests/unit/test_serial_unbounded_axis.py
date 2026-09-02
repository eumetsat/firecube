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

import re
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import pytest

from firecube.core.index_spec import IndexSpec, RegularTimeAxis
from firecube.ingestor.api import (
    ConfigurationError,
    EngineConfig,
    PipelineBatch,
    PluginContext,
    RuntimeIngestContext,
)
from firecube.ingestor.templates.direct_zarr import DirectZarrIngestor, WriteIntent
from tests.helpers.storage import make_test_context

pytestmark = pytest.mark.unit

UNBOUNDED_AXIS_MESSAGE = (
    "group 'data': axis has no fixed extent — set RegularTimeAxis(end_date=...) or "
    "slot_count=... to enable parallel ingestion"
)


class _SerialUnboundedAxisIngestor(DirectZarrIngestor):
    PRODUCT_NAME = "serial_unbounded_axis"

    def index_spec(self, ctx: PluginContext) -> IndexSpec:
        return IndexSpec(
            name="serial_unbounded_axis_v1",
            groups={
                "data": RegularTimeAxis(
                    coordinate="timestamp",
                    epoch="2026-01-01T00:00:00Z",
                    cadence_s=600,
                )
            },
        )

    def zarr_schema(self, ctx: PluginContext) -> list[Any]:
        return []

    def build_write_intents(self, batch: PipelineBatch, ctx: PluginContext) -> list[WriteIntent]:
        return [WriteIntent(group="data", array="data", ts_index=0, data=object())]


def _make_ctx(tmp_path: Path):
    return make_test_context(
        tmp_path,
        source=str(tmp_path / "source"),
        product="serial_unbounded_axis.zarr",
        options={"write_mode": "direct"},
    )


def test_base_run_serial_unbounded_axis_accepts(tmp_path: Path) -> None:
    """Serial mode (no slot_start/slot_end) accepts unbounded axes and completes the run."""
    ingestor = _SerialUnboundedAxisIngestor(name="serial_unbounded_axis")
    ingestor._configurator = SimpleNamespace(  # type: ignore[assignment]
        configure=lambda runtime_ctx: (
            EngineConfig(write_mode="direct"),  # NO slot_start/slot_end → serial
            None,
            None,
        )
    )
    ctx = _make_ctx(tmp_path)

    result = ingestor.ingest(ctx)

    assert result.registered is True
    assert result.write_mode_applied == "direct"
    assert str(result.outputs.primary).endswith("serial_unbounded_axis.zarr")


def test_base_run_parallel_unbounded_axis_raises_configuration_error(
    tmp_path: Path,
) -> None:
    ingestor = _SerialUnboundedAxisIngestor(name="serial_unbounded_axis")
    ingestor._configurator = SimpleNamespace(  # type: ignore[assignment]
        configure=lambda runtime_ctx: (
            EngineConfig(write_mode="direct", slot_start=0, slot_end=1),
            None,
            None,
        )
    )
    ctx = _make_ctx(tmp_path)

    with pytest.raises(ConfigurationError, match=re.escape(UNBOUNDED_AXIS_MESSAGE)):
        ingestor.ingest(ctx)


def test_direct_zarr_process_batch_defense_in_depth_raises_configuration_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    ingestor = _SerialUnboundedAxisIngestor(name="serial_unbounded_axis")
    ingestor.engine_config = EngineConfig(write_mode="direct", slot_start=0, slot_end=1)
    ingestor.template_config = None
    batch = PipelineBatch(batch_id="batch-1", data_path=tmp_path, items=["item"], metadata={})
    runtime_ctx = make_test_context(
        tmp_path,
        source=str(tmp_path / "source"),
        product="serial_unbounded_axis.zarr",
        options={"write_mode": "direct"},
    )
    ctx = PluginContext(cast(RuntimeIngestContext, runtime_ctx))

    monkeypatch.setattr(ingestor, "batch_setup", lambda ctx: None)
    monkeypatch.setattr(ingestor, "prepare_batch_data", lambda batch, ctx: {})
    monkeypatch.setattr(ingestor, "resolve_output_uri", lambda ctx, write_mode: "out.zarr")
    monkeypatch.setattr(ingestor, "cleanup_batch_data", lambda batch, ctx: None)
    monkeypatch.setattr(ingestor, "batch_teardown", lambda ctx: None)

    with pytest.raises(ConfigurationError, match=re.escape(UNBOUNDED_AXIS_MESSAGE)):
        ingestor._process_batch(batch, ctx)
