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
from collections.abc import Iterable, Iterator
from pathlib import Path
from typing import Any, ClassVar

import numpy as np
import pytest
from click.testing import CliRunner

from firecube.cli.main import cli
from firecube.core.api import IndexSpec, ItemInfo, RegularTimeAxis
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

_PLUGIN_PHANTOM = "phase34_phantom_group_plugin"
_PLUGIN_VALID = "phase34_valid_multi_group_plugin"
_PRODUCT_PHANTOM = "phase34_phantom_group_product"
_PRODUCT_VALID = "phase34_valid_multi_group_product"


class _PhantomGroupPlugin(DirectZarrIngestor):
    PRODUCT_NAME: ClassVar[str] = _PRODUCT_PHANTOM

    def discover_source_files(self, ctx: PluginContext) -> Iterable[Any]:
        _ = ctx
        return [("real", 0)]

    def index_spec(self, ctx: PluginContext) -> IndexSpec:
        _ = ctx
        return IndexSpec(
            name="phase34_phantom_group_v1",
            groups={
                "real": RegularTimeAxis(
                    coordinate="timestamp",
                    epoch="2026-01-01T00:00:00Z",
                    cadence_s=1,
                    mode="exact",
                    slot_count=100,
                ),
                "phantom": RegularTimeAxis(
                    coordinate="timestamp",
                    epoch="2026-01-01T00:00:00Z",
                    cadence_s=1,
                    mode="exact",
                    slot_count=100,
                ),
            },
        )

    def inspect_item(self, item: Any, ctx: PluginContext) -> ItemInfo | None:
        _ = ctx
        return ItemInfo(
            coordinate=dt.datetime(2026, 1, 1, tzinfo=dt.UTC) + dt.timedelta(seconds=int(item[1]))
        )

    def zarr_schema(self, ctx: PluginContext) -> list[ZarrGroupSpec]:
        _ = ctx
        return [
            ZarrGroupSpec(
                group="real",
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


class _ValidMultiGroupPlugin(DirectZarrIngestor):
    PRODUCT_NAME: ClassVar[str] = _PRODUCT_VALID

    def discover_source_files(self, ctx: PluginContext) -> Iterable[Any]:
        _ = ctx
        return [("a", 0), ("b", 0)]

    def index_spec(self, ctx: PluginContext) -> IndexSpec:
        _ = ctx
        return IndexSpec(
            name="phase34_valid_multi_group_v1",
            groups={
                "a": RegularTimeAxis(
                    coordinate="timestamp",
                    epoch="2026-01-01T00:00:00Z",
                    cadence_s=1,
                    mode="exact",
                    slot_count=100,
                ),
                "b": RegularTimeAxis(
                    coordinate="timestamp",
                    epoch="2026-01-01T00:00:00Z",
                    cadence_s=1,
                    mode="exact",
                    slot_count=200,
                ),
            },
        )

    def inspect_item(self, item: Any, ctx: PluginContext) -> ItemInfo | None:
        _ = ctx
        return ItemInfo(
            coordinate=dt.datetime(2026, 1, 1, tzinfo=dt.UTC) + dt.timedelta(seconds=int(item[1]))
        )

    def zarr_schema(self, ctx: PluginContext) -> list[ZarrGroupSpec]:
        _ = ctx
        return [
            ZarrGroupSpec(
                group="a",
                arrays=[
                    ZarrArraySpec(
                        name="values",
                        chunks=(50, 4),
                        shape=(100, 4),
                        dtype=np.float32,
                    )
                ],
            ),
            ZarrGroupSpec(
                group="b",
                arrays=[
                    ZarrArraySpec(
                        name="values",
                        chunks=(100, 4),
                        shape=(200, 4),
                        dtype=np.float32,
                    )
                ],
            ),
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
    register_ingestor(_PLUGIN_PHANTOM)(_PhantomGroupPlugin)
    register_ingestor(_PLUGIN_VALID)(_ValidMultiGroupPlugin)
    try:
        yield
    finally:
        _loader._LOADED = original_loaded
        _loader.AVAILABLE_INGESTORS.clear()
        _loader.AVAILABLE_INGESTORS.update(original_registry)


def _plan_args(tmp_path: Path, plugin: str, product: str) -> list[str]:
    config = tmp_path / "config.toml"
    config.write_text("", encoding="utf-8")
    return [
        "--config-file",
        str(config),
        "zarr",
        "slots",
        plugin,
        "--target",
        (tmp_path / f"{product}.zarr").as_uri(),
        "--product-name",
        product,
        "--storage-type",
        "local",
        "--storage-driver",
        "fsspec",
        "--write-mode",
        "direct",
    ]


def _preallocate_args(tmp_path: Path, plugin: str, product: str) -> list[str]:
    config = tmp_path / "config.toml"
    config.write_text("", encoding="utf-8")
    return [
        "--config-file",
        str(config),
        "zarr",
        "preallocate",
        plugin,
        "--target",
        (tmp_path / f"{product}.zarr").as_uri(),
        "--product-name",
        product,
        "--storage-type",
        "local",
        "--storage-driver",
        "fsspec",
        "--write-mode",
        "direct",
    ]


def test_plan_rejects_phantom_global_expected(tmp_path: Path) -> None:
    result = CliRunner().invoke(cli, _plan_args(tmp_path, _PLUGIN_PHANTOM, _PRODUCT_PHANTOM))

    assert result.exit_code != 0, result.output
    assert "phantom" in result.output
    assert "slot_start" not in result.output


def test_plan_accepts_valid_multi_group(tmp_path: Path) -> None:
    result = CliRunner().invoke(cli, _plan_args(tmp_path, _PLUGIN_VALID, _PRODUCT_VALID))

    assert result.exit_code == 0, result.output
    assert "a" in result.output
    assert "b" in result.output


def test_preallocate_rejects_phantom_global_expected(tmp_path: Path) -> None:
    target_path = tmp_path / f"{_PRODUCT_PHANTOM}.zarr"
    result = CliRunner().invoke(cli, _preallocate_args(tmp_path, _PLUGIN_PHANTOM, _PRODUCT_PHANTOM))

    assert result.exit_code != 0, result.output
    assert "phantom" in result.output
    assert not (target_path / "real").exists()
    assert not (target_path / "phantom").exists()
    run_record = json.loads(
        (target_path / ".firecube" / "runs" / "preallocate" / "run.json").read_text(
            encoding="utf-8"
        )
    )
    assert run_record["status"] == "failed"


def test_preallocate_accepts_valid_multi_group(tmp_path: Path) -> None:
    target_path = tmp_path / f"{_PRODUCT_VALID}.zarr"
    result = CliRunner().invoke(cli, _preallocate_args(tmp_path, _PLUGIN_VALID, _PRODUCT_VALID))

    assert result.exit_code == 0, result.output
    assert target_path.exists()
    assert (target_path / "zarr.json").exists()
