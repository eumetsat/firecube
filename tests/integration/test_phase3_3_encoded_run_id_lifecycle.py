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
from types import SimpleNamespace
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

_PLUGIN = "phase33_unsafe_group_plugin"
_PRODUCT = "phase33_unsafe_group_product"
_GROUP = "grp/with/slash"


class UnsafeGroupIngestor(DirectZarrIngestor):
    PRODUCT_NAME: ClassVar[str] = _PRODUCT
    SUPPORTS_SLOT_RANGE_PARALLELISM: ClassVar[bool] = True

    def discover_source_files(self, ctx: PluginContext) -> Iterable[Any]:
        return [(_GROUP, i) for i in range(100)]

    def timestamp_to_ts_index(self, group: str, timestamp_val: Any) -> int:
        return int(timestamp_val)

    def global_expected_time_count(self, ctx: PluginContext) -> dict[str, int]:
        return {_GROUP: 100}

    def slot_index_model(self, ctx: PluginContext) -> SlotIndexModel:
        return SlotIndexModel(
            name="phase33_unsafe_group_v1",
            epoch="2026-01-01T00:00:00Z",
            groups={_GROUP: SlotAxis(cadence_s=1, mode="exact")},
        )

    def filter_items_to_slot_range(
        self,
        items: Sequence[Any],
        slot_start: int,
        slot_end: int,
        ctx: PluginContext,
    ) -> Sequence[Any]:
        return [it for it in items if slot_start <= int(it[1]) < slot_end]

    def zarr_schema(self, ctx: PluginContext) -> list[ZarrGroupSpec]:
        return [
            ZarrGroupSpec(
                group=_GROUP,
                arrays=[
                    ZarrArraySpec(
                        name="primary",
                        chunks=(50, 4),
                        shape=(100, 4),
                        dtype=np.float32,
                    )
                ],
            )
        ]

    def build_write_intents(self, batch: PipelineBatch, ctx: PluginContext) -> list[WriteIntent]:
        return [
            WriteIntent(
                group=str(group),
                array="primary",
                ts_index=int(ts_idx),
                data=np.full((4,), float(ts_idx), dtype="float32"),
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
    register_ingestor(_PLUGIN)(UnsafeGroupIngestor)
    yield
    _loader._LOADED = original_loaded
    _loader.AVAILABLE_INGESTORS.clear()
    _loader.AVAILABLE_INGESTORS.update(original_registry)


def _base_args(target_path: Path) -> list[str]:
    return [
        "ingest",
        _PLUGIN,
        "--target",
        f"file://{target_path}",
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
        "100",
        "--slot-group",
        "grp/with/slash",
        "--option",
        "no_progress=true",
        "--option",
        "pipeline_parallel=true",
        "--option",
        "pipeline_batch_size=100",
    ]


def _run_dirs(target_path: Path) -> list[Path]:
    return sorted((target_path / ".firecube" / "runs").iterdir())


def test_full_ingest_with_unsafe_slot_group(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    target_path = tmp_path / "out.zarr"
    monkeypatch.setenv("HOSTNAME", "host")
    monkeypatch.setattr("uuid.uuid4", lambda: SimpleNamespace(hex="phase33"))

    result = CliRunner().invoke(cli, _base_args(target_path))

    assert result.exit_code == 0, result.output
    assert result.exception is None
    assert "traceback" not in result.output.lower()
    dirs = _run_dirs(target_path)
    assert any(
        path.name
        == "phase33_unsafe_group_plugin-host-phase33__group=grp%2Fwith%2Fslash__slot=0-100"
        for path in dirs
    )


def test_resume_not_broken_by_encoding(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    target_path = tmp_path / "out.zarr"
    monkeypatch.setenv("HOSTNAME", "host")
    monkeypatch.setattr("uuid.uuid4", lambda: SimpleNamespace(hex="phase33resume"))
    args = _base_args(target_path)
    first = CliRunner().invoke(cli, args)
    second = CliRunner().invoke(cli, args)

    assert first.exit_code == 0, first.output
    assert second.exit_code != 0
    assert second.exception is not None
    assert "Completed spans overlap" in str(second.exception)
    dirs = _run_dirs(target_path)
    assert [path.name for path in dirs].count(
        "phase33_unsafe_group_plugin-host-phase33resume__group=grp%2Fwith%2Fslash__slot=0-100"
    ) == 1
