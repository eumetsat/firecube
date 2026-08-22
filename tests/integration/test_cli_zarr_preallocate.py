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

import importlib
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
from click.testing import CliRunner

from firecube.cli.main import cli
from firecube.core.controlplane import ChunkManager
from firecube.core.controlplane.types import INDEX_ENSURED_OUTCOME_CREATED, IndexEnsuredEvent
from firecube.core.observability.metrics import METRIC_INDEX_ENSURED
from firecube.ingestor.registry import loader as _loader

pytestmark = pytest.mark.integration

PRODUCT_NAME = "direct_zarr_capable_test_product"
PLUGIN_NAME = "direct_zarr_capable_test_plugin"


class _FakeTelemetry:
    def __init__(self) -> None:
        self.emitted: list[tuple[str, float, str, dict[str, Any] | None]] = []

    def emit(
        self,
        name: str,
        value: float,
        *,
        kind: str = "gauge",
        meta: dict[str, Any] | None = None,
    ) -> None:
        self.emitted.append((name, value, kind, dict(meta) if meta else None))

    def flush(self) -> None:
        return None


@pytest.fixture(autouse=True)
def reset_plugin_registry() -> Iterator[None]:
    original_loaded = _loader._LOADED
    original_registry = dict(_loader.AVAILABLE_INGESTORS)
    _loader._LOADED = False
    _loader.AVAILABLE_INGESTORS.clear()
    importlib.reload(importlib.import_module(PLUGIN_NAME))
    yield
    _loader._LOADED = original_loaded
    _loader.AVAILABLE_INGESTORS.clear()
    _loader.AVAILABLE_INGESTORS.update(original_registry)


def _cli_args(target_dir: Path) -> list[str]:
    return [
        "zarr",
        "preallocate",
        PLUGIN_NAME,
        "--target",
        f"file://{target_dir}",
        "--product-name",
        PRODUCT_NAME,
        "--storage-type",
        "local",
        "--storage-driver",
        "fsspec",
        "--write-mode",
        "direct",
    ]


def test_preallocate_emits_index_ensured_wal_and_telemetry(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target_dir = tmp_path / "out.zarr"
    telemetry = _FakeTelemetry()
    wal_events: list[IndexEnsuredEvent] = []
    original_record = ChunkManager.record_index_ensured_event

    def record_spy(self: ChunkManager, event: IndexEnsuredEvent) -> None:
        wal_events.append(event)
        original_record(self, event)

    monkeypatch.setattr(ChunkManager, "record_index_ensured_event", record_spy)
    monkeypatch.setattr(
        "firecube.cli.zarr.observability.create_ingestion_telemetry",
        lambda **_kwargs: telemetry,
    )

    result = CliRunner().invoke(cli, _cli_args(target_dir))

    assert result.exit_code == 0, result.output
    assert len(wal_events) == 1
    event = wal_events[0]
    assert event.product == PRODUCT_NAME
    assert event.run_id == "preallocate"
    assert event.outcome == INDEX_ENSURED_OUTCOME_CREATED
    assert event.axis_kinds == ("regular_time",)
    assert event.groups == ("data",)

    assert len(telemetry.emitted) == 1
    name, value, kind, meta = telemetry.emitted[0]
    assert name == METRIC_INDEX_ENSURED
    assert value == 1.0
    assert kind == "counter"
    assert meta is not None
    assert meta["plugin"] == PLUGIN_NAME
    assert meta["product"] == PRODUCT_NAME
    assert meta["outcome"] == INDEX_ENSURED_OUTCOME_CREATED
    assert meta["identity_hash"] == event.identity_hash
    assert meta["axis_kinds"] == "regular_time"
    assert meta["groups"] == "data"
