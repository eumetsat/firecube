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

"""Error-wrapping coverage for ``firecube zarr preallocate``."""

from __future__ import annotations

import datetime as dt
from collections.abc import Iterator
from pathlib import Path
from typing import Any, ClassVar

import pytest
from click.testing import CliRunner

from firecube.cli.main import cli
from firecube.core.api import IndexSpec, ItemInfo, RegularTimeAxis
from firecube.core.errors import SchemaDriftError
from firecube.ingestor.api import DirectZarrIngestor, PluginContext, ZarrGroupSpec
from firecube.ingestor.registry import loader as _loader

pytestmark = pytest.mark.integration

_PLUGIN = "preallocate_error_wrapping_test_plugin"
_GROUP = "data"
_COORD = "time"
_SLOT_COUNT = 12
_EPOCH = "2024-01-01T00:00:00Z"


class _State:
    slot_indices: ClassVar[list[int]] = list(range(_SLOT_COUNT))
    offset_s: ClassVar[int] = 2

    @classmethod
    def reset(cls) -> None:
        cls.slot_indices = list(range(_SLOT_COUNT))
        cls.offset_s = 2


class _Plugin(DirectZarrIngestor):
    PRODUCT_NAME: ClassVar[str] = _PLUGIN
    time_dim_name: ClassVar[str] = _COORD

    def index_spec(self, ctx: PluginContext) -> IndexSpec:
        return IndexSpec(
            name=f"{_PLUGIN}_v1",
            groups={
                _GROUP: RegularTimeAxis(
                    coordinate=_COORD,
                    epoch=_EPOCH,
                    cadence_s=600,
                    mode="floor",
                    slot_count=_SLOT_COUNT,
                )
            },
        )

    def inspect_item(self, item: Any, ctx: PluginContext) -> ItemInfo | None:
        if not isinstance(item, int):
            return None
        coord = dt.datetime(2024, 1, 1, tzinfo=dt.UTC) + dt.timedelta(
            seconds=item * 600 + _State.offset_s
        )
        return ItemInfo(coordinate=coord)

    def discover_source_files(self, ctx: PluginContext) -> list[Any]:
        return list(_State.slot_indices)

    def zarr_schema(self, ctx: PluginContext) -> list[ZarrGroupSpec]:
        return [
            ZarrGroupSpec(
                group=_GROUP,
                arrays=[],
            )
        ]

    def build_write_intents(self, batch, ctx: PluginContext):
        return []


@pytest.fixture(autouse=True)
def _register_plugin() -> Iterator[None]:
    original_loaded = _loader._LOADED
    original_registry = dict(_loader.AVAILABLE_INGESTORS)
    _loader._LOADED = True
    _loader.AVAILABLE_INGESTORS.clear()
    _Plugin.name = _PLUGIN  # pyright: ignore[reportAttributeAccessIssue]
    _loader.AVAILABLE_INGESTORS[_PLUGIN] = _Plugin
    _State.reset()
    try:
        yield
    finally:
        _State.reset()
        _loader._LOADED = original_loaded
        _loader.AVAILABLE_INGESTORS.clear()
        _loader.AVAILABLE_INGESTORS.update(original_registry)


def _args(target: Path, source: Path) -> list[str]:
    return [
        "zarr",
        "preallocate",
        _PLUGIN,
        "--target",
        f"file://{target}",
        "--product-name",
        _PLUGIN,
        "--storage-type",
        "local",
        "--storage-driver",
        "fsspec",
        "--write-mode",
        "direct",
        "--input-data",
        str(source),
    ]


def test_schema_drift_is_user_facing(tmp_path: Path) -> None:
    target = tmp_path / "cube.zarr"
    source = tmp_path / "source"
    source.mkdir()
    runner = CliRunner()

    first = runner.invoke(cli, _args(target, source))
    assert first.exit_code == 0, first.output

    _State.offset_s = 137
    second = runner.invoke(cli, _args(target, source))

    assert second.exit_code == 1, second.output
    # The drift must be wrapped at the CLI boundary: Click renders the message
    # and no raw exception escapes to the operator. An unwrapped SchemaDriftError
    # here means the boundary regressed to printing tracebacks.
    assert not isinstance(second.exception, SchemaDriftError), second.output
    combined = second.output + second.stderr
    assert "Traceback" not in combined, combined
    assert "slot 0" in combined, combined
    assert "drift" in combined, combined
