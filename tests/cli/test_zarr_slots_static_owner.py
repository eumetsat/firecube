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

import datetime as dt
import json
from collections.abc import Iterator
from pathlib import Path
from typing import Any, ClassVar

import numpy as np
import pytest
from click.testing import CliRunner

from firecube.cli.main import cli
from firecube.core.index_spec import IndexSpec, ItemInfo, RegularTimeAxis
from firecube.ingestor.api import (
    DirectZarrIngestor,
    PipelineBatch,
    PluginContext,
    WriteIntent,
    ZarrArraySpec,
    ZarrGroupSpec,
    register_ingestor,
)
from firecube.ingestor.registry import loader

pytestmark = pytest.mark.unit


class _StaticOwnerSlotsPlugin(DirectZarrIngestor):
    PRODUCT_NAME: ClassVar[str] = "static_owner_slots_product"

    def index_spec(self, ctx: PluginContext) -> IndexSpec:
        return IndexSpec(
            name="static_owner_slots_v1",
            groups={
                "data": RegularTimeAxis(
                    coordinate="timestamp",
                    epoch="2026-01-01T00:00:00Z",
                    cadence_s=1,
                    mode="exact",
                    slot_count=250,
                )
            },
        )

    def inspect_item(self, item: Any, ctx: PluginContext) -> ItemInfo | None:
        return ItemInfo(
            coordinate=dt.datetime(2026, 1, 1, tzinfo=dt.UTC) + dt.timedelta(seconds=int(item))
        )

    def zarr_schema(self, ctx: PluginContext) -> list[ZarrGroupSpec]:
        return [
            ZarrGroupSpec(
                group="data",
                arrays=[
                    ZarrArraySpec(
                        name="data",
                        chunks=(100, 4),
                        shape=(250, 4),
                        dtype=np.float32,
                    )
                ],
            )
        ]

    def build_write_intents(self, batch: PipelineBatch, ctx: PluginContext) -> list[WriteIntent]:
        return []


class _SingleEntryStaticOwnerSlotsPlugin(_StaticOwnerSlotsPlugin):
    PRODUCT_NAME: ClassVar[str] = "single_static_owner_slots_product"

    def index_spec(self, ctx: PluginContext) -> IndexSpec:
        return IndexSpec(
            name="single_static_owner_slots_v1",
            groups={
                "data": RegularTimeAxis(
                    coordinate="timestamp",
                    epoch="2026-01-01T00:00:00Z",
                    cadence_s=1,
                    mode="exact",
                    slot_count=80,
                )
            },
        )

    def zarr_schema(self, ctx: PluginContext) -> list[ZarrGroupSpec]:
        return [
            ZarrGroupSpec(
                group="data",
                arrays=[
                    ZarrArraySpec(
                        name="data",
                        chunks=(100, 4),
                        shape=(80, 4),
                        dtype=np.float32,
                    )
                ],
            )
        ]


@pytest.fixture(autouse=True)
def _register_plugins() -> Iterator[None]:
    saved_ingestors = loader.AVAILABLE_INGESTORS.copy()
    saved_loaded = loader._LOADED
    register_ingestor("static_owner_slots")(_StaticOwnerSlotsPlugin)
    register_ingestor("single_static_owner_slots")(_SingleEntryStaticOwnerSlotsPlugin)
    try:
        yield
    finally:
        loader.AVAILABLE_INGESTORS.clear()
        loader.AVAILABLE_INGESTORS.update(saved_ingestors)
        loader._LOADED = saved_loaded


def _args(tmp_path: Path, plugin: str) -> list[str]:
    config = tmp_path / "config.toml"
    config.write_text("", encoding="utf-8")
    return [
        "--config-file",
        str(config),
        "zarr",
        "slots",
        plugin,
        "--target",
        (tmp_path / f"{plugin}.zarr").as_uri(),
        "--product-name",
        plugin,
        "--storage-type",
        "local",
        "--storage-driver",
        "fsspec",
        "--write-mode",
        "direct",
        "--no-resume",
    ]


def _slots_payload(tmp_path: Path, plugin: str) -> dict[str, Any]:
    result = CliRunner().invoke(cli, _args(tmp_path, plugin))
    assert result.exit_code == 0, result.output
    return json.loads(result.output)


def test_slots_json_includes_static_owner_field(tmp_path: Path) -> None:
    payload = _slots_payload(tmp_path, "static_owner_slots")

    group = next(entry for entry in payload["groups"] if entry["name"] == "data")

    assert len([entry for entry in payload["ranges"] if entry["group"] == "data"]) >= 2
    assert group["static_owner"] == {"slot_start": 0, "slot_end": 100}


def test_static_owner_deterministic_across_reruns(tmp_path: Path) -> None:
    first = _slots_payload(tmp_path, "static_owner_slots")
    second = _slots_payload(tmp_path, "static_owner_slots")

    first_group = next(entry for entry in first["groups"] if entry["name"] == "data")
    second_group = next(entry for entry in second["groups"] if entry["name"] == "data")
    assert first_group["static_owner"] == second_group["static_owner"]


def test_single_entry_plan_is_its_own_owner(tmp_path: Path) -> None:
    payload = _slots_payload(tmp_path, "single_static_owner_slots")

    group = next(entry for entry in payload["groups"] if entry["name"] == "data")

    assert group["static_owner"] == {"slot_start": 0, "slot_end": 80}
