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

from collections.abc import Iterable, Iterator, Sequence
from pathlib import Path
from typing import Any, ClassVar

import numpy as np
import pytest
from click.testing import CliRunner

from firecube.cli.main import cli
from firecube.core.api import SlotAxis, SlotIndexModel
from firecube.ingestor.api import (
    DirectZarrIngestor,
    PipelineBatch,
    PluginContext,
    WriteIntent,
    ZarrArraySpec,
    ZarrGroupSpec,
    register_ingestor,
)
from firecube.ingestor.registry import loader as _loader

pytestmark = pytest.mark.integration

_PLUGIN = "phase33_phantom_group_plugin"
_PRODUCT = "phase33_phantom_group_product"


class _PhantomGroupPlugin(DirectZarrIngestor):
    PRODUCT_NAME: ClassVar[str] = _PRODUCT
    SUPPORTS_SLOT_RANGE_PARALLELISM: ClassVar[bool] = True

    def discover_source_files(self, ctx: PluginContext) -> Iterable[Any]:
        _ = ctx
        return [("real_group", 0)]

    def timestamp_to_ts_index(self, group: str, timestamp_val: Any) -> int:
        _ = group
        return int(timestamp_val)

    def global_expected_time_count(self, ctx: PluginContext) -> dict[str, int]:
        _ = ctx
        return {"real_group": 100, "phantom": 50}

    def slot_index_model(self, ctx: PluginContext) -> SlotIndexModel:
        _ = ctx
        return SlotIndexModel(
            name="phase33_phantom_group_v1",
            epoch="2026-01-01T00:00:00Z",
            groups={"real_group": SlotAxis(cadence_s=1, mode="exact")},
        )

    def filter_items_to_slot_range(
        self,
        items: Sequence[Any],
        slot_start: int,
        slot_end: int,
        ctx: PluginContext,
    ) -> Sequence[Any]:
        _ = ctx
        return [item for item in items if slot_start <= int(item[1]) < slot_end]

    def zarr_schema(self, ctx: PluginContext) -> list[ZarrGroupSpec]:
        _ = ctx
        return [
            ZarrGroupSpec(
                group="real_group",
                arrays=[
                    ZarrArraySpec(
                        name="values",
                        chunks=(50, 4),
                        shape=(100, 4),
                        dtype=np.float32,
                    )
                ],
            )
        ]

    def build_write_intents(self, batch: PipelineBatch, ctx: PluginContext) -> list[WriteIntent]:
        _ = ctx
        return [
            WriteIntent(
                group=str(group),
                array="values",
                ts_index=int(ts_idx),
                data=np.zeros((4,), dtype=np.float32),
                kind="1d",
            )
            for group, ts_idx in batch.items
        ]


@pytest.fixture(autouse=True)
def reset_plugin_registry() -> Iterator[None]:
    original_loaded = _loader._LOADED
    original_registry = dict(_loader.AVAILABLE_INGESTORS)
    _loader._LOADED = False
    _loader.AVAILABLE_INGESTORS.clear()
    register_ingestor(_PLUGIN)(_PhantomGroupPlugin)
    try:
        yield
    finally:
        _loader._LOADED = original_loaded
        _loader.AVAILABLE_INGESTORS.clear()
        _loader.AVAILABLE_INGESTORS.update(original_registry)


def _args(target_path: Path) -> list[str]:
    return [
        "ingest",
        _PLUGIN,
        "--target",
        target_path.as_uri(),
        "--product-name",
        _PRODUCT,
        "--storage-type",
        "local",
        "--storage-driver",
        "fsspec",
        "--write-mode",
        "direct",
        "--slot-start",
        "0",
        "--slot-end",
        "50",
        "--option",
        "no_progress=true",
        "--option",
        "pipeline_parallel=true",
        "--option",
        "pipeline_batch_size=10",
    ]


def test_phantom_group_caught_at_gate_e2e(tmp_path: Path) -> None:
    result = CliRunner().invoke(cli, _args(tmp_path / "out.zarr"))

    assert result.exit_code != 0
    combined = f"{result.output}\n{result.exception}"
    assert "phantom" in combined


def test_no_audit_record_for_phantom_e2e(tmp_path: Path) -> None:
    target_path = tmp_path / "out.zarr"
    result = CliRunner().invoke(cli, _args(target_path))

    assert result.exit_code != 0
    records = [path.read_text(encoding="utf-8") for path in target_path.rglob("*.jsonl")]
    assert not any("schema_verification" in record and "phantom" in record for record in records)
