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

from collections.abc import Iterator
from pathlib import Path
from typing import Any, ClassVar

import numpy as np
import pytest
from click.testing import CliRunner

from firecube.cli.main import cli
from firecube.core.index_spec import (
    AUTO,
    IndexSpec,
    IrregularTimeAxis,
    ItemInfo,
    RegularTimeAxis,
)
from firecube.ingestor.api import (
    DirectZarrIngestor,
    PipelineBatch,
    PluginContext,
    WriteIntent,
    ZarrArraySpec,
    ZarrGroupSpec,
)
from firecube.ingestor.registry import loader as _loader
from firecube.ingestor.registry.loader import register_ingestor

pytestmark = pytest.mark.integration

_EPOCH = "2026-01-01T00:00:00Z"
_ITEM_COUNT = 3
_TIMESTAMPS = tuple(
    np.datetime64("2026-01-01T00:00:00", "ns") + np.timedelta64(i * 600, "s")
    for i in range(_ITEM_COUNT)
)


@register_ingestor("preallocate_auto_rebuild")
class _PreallocateAutoRebuildIngestor(DirectZarrIngestor):
    PRODUCT_NAME: ClassVar[str] = "preallocate_auto_rebuild"
    index_spec_calls: ClassVar[int] = 0

    def discover_source_files(self, ctx: PluginContext) -> list[int]:
        _ = ctx
        return list(range(_ITEM_COUNT))

    def index_spec(self, ctx: PluginContext) -> IndexSpec:
        _ = ctx
        type(self).index_spec_calls += 1
        return IndexSpec(
            name="preallocate_auto_rebuild_v1",
            groups={
                "auto": IrregularTimeAxis(coordinate="timestamp", values=AUTO),
                "bounded": RegularTimeAxis(
                    coordinate="timestamp",
                    epoch=_EPOCH,
                    cadence_s=600,
                    slot_count=_ITEM_COUNT,
                ),
                "unbounded": RegularTimeAxis(
                    coordinate="timestamp",
                    epoch=_EPOCH,
                    cadence_s=600,
                ),
            },
        )

    def inspect_item(self, item: int, ctx: PluginContext) -> ItemInfo | None:
        _ = ctx
        return ItemInfo(coordinate=_TIMESTAMPS[item])

    def zarr_schema(self, ctx: PluginContext) -> list[ZarrGroupSpec]:
        _ = ctx
        return [
            ZarrGroupSpec(
                group=group,
                arrays=[
                    ZarrArraySpec(
                        name="values",
                        shape=(_ITEM_COUNT,),
                        dtype="float32",
                        chunks=(1,),
                        fill_value=0.0,
                        expected_time_count=_ITEM_COUNT,
                        time_indexed=True,
                    )
                ],
            )
            for group in ("auto", "bounded")
        ]

    def build_write_intents(self, batch: PipelineBatch, ctx: PluginContext) -> list[WriteIntent]:
        _ = (batch, ctx)
        return []


@pytest.fixture(autouse=True)
def _restore_registry() -> Iterator[None]:
    original_loaded = _loader._LOADED
    original_registry = dict(_loader.AVAILABLE_INGESTORS)
    _PreallocateAutoRebuildIngestor.index_spec_calls = 0
    yield
    _loader.AVAILABLE_INGESTORS.clear()
    _loader.AVAILABLE_INGESTORS.update(original_registry)
    _loader._LOADED = original_loaded


def test_preallocate_auto_rebuild_uses_bound_spec(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    target_dir = tmp_path / "out.zarr"

    class _FakeTelemetry:
        def emit(self, *args: Any, **kwargs: Any) -> None:
            _ = (args, kwargs)

        def flush(self) -> None:
            return None

    monkeypatch.setattr(
        "firecube.cli.zarr._preallocate.observability.create_ingestion_telemetry",
        lambda **_kwargs: _FakeTelemetry(),
    )

    result = CliRunner().invoke(
        cli,
        [
            "zarr",
            "preallocate",
            "preallocate_auto_rebuild",
            "--target",
            f"file://{target_dir}",
            "--product-name",
            "preallocate_auto_rebuild",
            "--storage-type",
            "local",
            "--storage-driver",
            "fsspec",
            "--write-mode",
            "direct",
        ],
    )

    assert result.exit_code == 0, result.output
    assert "ExtentUnknownError" not in result.output
    assert _PreallocateAutoRebuildIngestor.index_spec_calls == 1
