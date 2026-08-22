"""Tests for chunk-alignment warnings in parallel capability validation."""

from __future__ import annotations

import datetime as dt
from types import SimpleNamespace
from typing import Any, ClassVar

import numpy as np
import pytest

from firecube.core.index_spec import IndexSpec, ItemInfo, RegularTimeAxis
from firecube.ingestor.api import (
    DirectZarrIngestor,
    PipelineBatch,
    PipelineResult,
    PipelineRunState,
    PluginContext,
    RuntimeIngestContext,
)
from firecube.ingestor.runtime.parallel_gate import validate_parallel_capability
from firecube.ingestor.templates.direct_zarr import ZarrArraySpec, ZarrGroupSpec

pytestmark = pytest.mark.unit


class _ChunkWarningIngestor(DirectZarrIngestor):
    PRODUCT_NAME: ClassVar[str] = "parallel_gate_chunk_alignment"

    def __init__(self, *, chunk_size: int, expected: int) -> None:
        super().__init__(name="parallel_gate_chunk_alignment")
        self._chunk_size = chunk_size
        self._expected = expected

    def index_spec(self, ctx: PluginContext) -> IndexSpec:
        _ = ctx
        return IndexSpec(
            name="parallel_gate_chunk_alignment_v1",
            groups={
                "data": RegularTimeAxis(
                    coordinate="timestamp",
                    epoch="2026-01-01T00:00:00Z",
                    cadence_s=1,
                    mode="exact",
                    slot_count=self._expected,
                )
            },
        )

    def inspect_item(self, item: Any, ctx: PluginContext) -> ItemInfo | None:
        _ = ctx
        return ItemInfo(
            coordinate=dt.datetime(2026, 1, 1, tzinfo=dt.UTC) + dt.timedelta(seconds=int(item))
        )

    def zarr_schema(self, ctx: PluginContext) -> list[ZarrGroupSpec]:
        _ = ctx
        return [
            ZarrGroupSpec(
                group="data",
                arrays=[
                    ZarrArraySpec(
                        name="values",
                        shape=(self._expected, 4),
                        dtype=np.float32,
                        chunks=(self._chunk_size, 4),
                    )
                ],
            )
        ]

    def ingest(self, ctx: Any):  # pragma: no cover - abstract hook not used here
        _ = ctx
        raise NotImplementedError

    def build_write_intents(self, batch: PipelineBatch, ctx: PluginContext) -> list[Any]:
        _ = (batch, ctx)
        return []


def _ctx() -> Any:
    return SimpleNamespace(_ctx=object())

    def _process_batch(self, batch: PipelineBatch, ctx: PluginContext) -> PipelineResult:
        _ = (batch, ctx)
        raise NotImplementedError

    def _aggregate_metrics(
        self,
        ctx: RuntimeIngestContext,
        state: PipelineRunState,
    ) -> dict[str, Any]:
        _ = (ctx, state)
        return {}


def test_chunk_alignment_warning_emitted(caplog: pytest.LogCaptureFixture) -> None:
    ingestor = _ChunkWarningIngestor(chunk_size=6, expected=13)

    with caplog.at_level("WARNING"):
        result = validate_parallel_capability(ingestor, 0, 13, ctx=_ctx())

    assert result is not None
    assert result.resolved.size("data") == 13
    assert any(
        "expected time count 13 is not a multiple of time-alignment size 6" in record.message
        for record in caplog.records
    )
