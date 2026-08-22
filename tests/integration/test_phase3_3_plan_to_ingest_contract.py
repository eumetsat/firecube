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

"""Phase 3.3 plan-to-ingest contract tests for terminal partial chunks."""

from __future__ import annotations

import datetime as dt
import json
from collections.abc import Iterator
from pathlib import Path
from typing import Any, ClassVar, cast

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
from firecube.ingestor.types.planned_range import validate_chunk_alignment

pytestmark = pytest.mark.integration

_TERM_PLUGIN = "phase33_plan_terminal_partial"
_ALIGNED_PLUGIN = "phase33_plan_aligned"
_MULTI_PLUGIN = "phase33_plan_multi_group"


class _TerminalPartialPlanPlugin(DirectZarrIngestor):
    PRODUCT_NAME: ClassVar[str] = "phase33_plan_terminal_product"

    def index_spec(self, ctx: PluginContext) -> IndexSpec:
        return IndexSpec(
            name="phase33_plan_terminal_v1",
            groups={
                "data": RegularTimeAxis(
                    coordinate="timestamp",
                    epoch="2026-01-01T00:00:00Z",
                    cadence_s=1,
                    mode="exact",
                    slot_count=950,
                )
            },
        )

    def inspect_item(self, item: Any, ctx: PluginContext) -> ItemInfo | None:
        _ = ctx
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
                        chunks=(100, 10),
                        shape=(950, 10),
                        dtype=np.float32,
                    )
                ],
            )
        ]

    def build_write_intents(self, batch: PipelineBatch, ctx: PluginContext) -> list[WriteIntent]:
        return []


class _AlignedPlanPlugin(_TerminalPartialPlanPlugin):
    PRODUCT_NAME: ClassVar[str] = "phase33_plan_aligned_product"

    def index_spec(self, ctx: PluginContext) -> IndexSpec:
        return IndexSpec(
            name="phase33_plan_aligned_v1",
            groups={
                "data": RegularTimeAxis(
                    coordinate="timestamp",
                    epoch="2026-01-01T00:00:00Z",
                    cadence_s=1,
                    mode="exact",
                    slot_count=1000,
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
                        chunks=(100, 10),
                        shape=(1000, 10),
                        dtype=np.float32,
                    )
                ],
            )
        ]


class _MultiGroupPlanPlugin(DirectZarrIngestor):
    PRODUCT_NAME: ClassVar[str] = "phase33_plan_multi_product"

    def index_spec(self, ctx: PluginContext) -> IndexSpec:
        return IndexSpec(
            name="phase33_plan_multi_v1",
            groups={
                "group_a": RegularTimeAxis(
                    coordinate="timestamp",
                    epoch="2026-01-01T00:00:00Z",
                    cadence_s=1,
                    mode="exact",
                    slot_count=950,
                ),
                "group_b": RegularTimeAxis(
                    coordinate="timestamp",
                    epoch="2026-01-01T00:00:00Z",
                    cadence_s=1,
                    mode="exact",
                    slot_count=1000,
                ),
            },
        )

    def inspect_item(self, item: Any, ctx: PluginContext) -> ItemInfo | None:
        _ = ctx
        return ItemInfo(
            coordinate=dt.datetime(2026, 1, 1, tzinfo=dt.UTC) + dt.timedelta(seconds=int(item))
        )

    def zarr_schema(self, ctx: PluginContext) -> list[ZarrGroupSpec]:
        return [
            ZarrGroupSpec(
                group="group_a",
                arrays=[
                    ZarrArraySpec(
                        name="a",
                        chunks=(100, 10),
                        shape=(950, 10),
                        dtype=np.float32,
                    )
                ],
            ),
            ZarrGroupSpec(
                group="group_b",
                arrays=[
                    ZarrArraySpec(
                        name="b",
                        chunks=(100, 10),
                        shape=(1000, 10),
                        dtype=np.float32,
                    )
                ],
            ),
        ]

    def build_write_intents(self, batch: PipelineBatch, ctx: PluginContext) -> list[WriteIntent]:
        return []


@pytest.fixture(autouse=True)
def reset_plugin_registry() -> Iterator[None]:
    original_loaded = loader._LOADED
    original_registry = dict(loader.AVAILABLE_INGESTORS)
    loader._LOADED = False
    loader.AVAILABLE_INGESTORS.clear()
    register_ingestor(_TERM_PLUGIN)(_TerminalPartialPlanPlugin)
    register_ingestor(_ALIGNED_PLUGIN)(_AlignedPlanPlugin)
    register_ingestor(_MULTI_PLUGIN)(_MultiGroupPlanPlugin)
    try:
        yield
    finally:
        loader._LOADED = original_loaded
        loader.AVAILABLE_INGESTORS.clear()
        loader.AVAILABLE_INGESTORS.update(original_registry)


def _plan_args(tmp_path: Path, plugin: str, product_name: str, *, name: str) -> list[str]:
    config = tmp_path / "config.toml"
    config.write_text("", encoding="utf-8")
    return [
        "--config-file",
        str(config),
        "zarr",
        "slots",
        plugin,
        "--target",
        (tmp_path / name).as_uri(),
        "--product-name",
        product_name,
        "--storage-type",
        "local",
        "--storage-driver",
        "fsspec",
        "--write-mode",
        "direct",
        "--slot-size",
        "100",
    ]


def _run_plan(tmp_path: Path, plugin: str, product_name: str, *, name: str) -> dict[str, object]:
    result = CliRunner().invoke(cli, _plan_args(tmp_path, plugin, product_name, name=name))
    assert result.exit_code == 0, result.output
    decoder = json.JSONDecoder()
    output = result.output.strip()
    pos, last = 0, None
    while pos < len(output):
        try:
            obj, end = decoder.raw_decode(output, pos)
            last = obj
            pos = end
            while pos < len(output) and output[pos] in " \t\n\r":
                pos += 1
        except json.JSONDecodeError:
            pos += 1
    assert last is not None, f"No JSON found in output: {result.output!r}"
    return last


def _chunk_shapes_per_group(plugin: DirectZarrIngestor) -> dict[str, list[tuple[int, ...]]]:
    shapes: dict[str, list[tuple[int, ...]]] = {}
    for group_spec in plugin.zarr_schema(cast(Any, None)):
        group_shapes = [
            arr_spec.chunks for arr_spec in group_spec.arrays if arr_spec.chunks is not None
        ]
        if group_shapes:
            shapes[group_spec.group] = group_shapes
    return shapes


def _validate_plan_contract(payload: dict[str, object], plugin: DirectZarrIngestor) -> None:
    chunk_shapes = _chunk_shapes_per_group(plugin)
    global_expected = {
        group["name"]: group["total_slots"]
        for group in payload["groups"]  # type: ignore[index]
    }
    for entry in payload["ranges"]:  # type: ignore[index]
        group_name = entry["group"]  # type: ignore[index]
        validate_chunk_alignment(
            entry["slot_start"],  # type: ignore[index]
            entry["slot_end"],  # type: ignore[index]
            {group_name: chunk_shapes[group_name]},
            global_expected=global_expected,
        )


def test_plan_remainder_range_accepted_by_ingest(tmp_path: Path) -> None:
    payload = _run_plan(
        tmp_path,
        _TERM_PLUGIN,
        "phase33_plan_terminal_product",
        name="terminal.zarr",
    )
    assert payload["groups"][0]["total_slots"] == 950  # type: ignore[index]
    assert payload["groups"][0]["slot_size"] == 100  # type: ignore[index]
    assert any(entry["slot_end"] == 950 for entry in payload["ranges"])  # type: ignore[index]

    _validate_plan_contract(payload, cast(Any, _TerminalPartialPlanPlugin)())


def test_plan_no_remainder_all_aligned(tmp_path: Path) -> None:
    payload = _run_plan(
        tmp_path,
        _ALIGNED_PLUGIN,
        "phase33_plan_aligned_product",
        name="aligned.zarr",
    )
    assert payload["groups"][0]["total_slots"] == 1000  # type: ignore[index]
    assert payload["groups"][0]["slot_size"] == 100  # type: ignore[index]
    assert payload["ranges"][-1]["slot_end"] == 1000  # type: ignore[index]

    _validate_plan_contract(payload, cast(Any, _AlignedPlanPlugin)())


def test_plan_multi_group_heterogeneous_remainder(tmp_path: Path) -> None:
    payload = _run_plan(
        tmp_path,
        _MULTI_PLUGIN,
        "phase33_plan_multi_product",
        name="multi.zarr",
    )
    groups = {group["name"]: group for group in payload["groups"]}  # type: ignore[index]
    assert groups["group_a"]["total_slots"] == 950
    assert groups["group_b"]["total_slots"] == 1000

    _validate_plan_contract(payload, cast(Any, _MultiGroupPlanPlugin)())
