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

"""A serial run may declare a RegularTimeAxis without a horizon.

``RegularTimeAxis`` documents that both ``slot_count`` and ``end_date`` may be
``None`` for serial-mode plugins, but the run-startup extent validation used
to raise ``UnboundedAxisError`` for every DirectZarr ingestor with a binding,
which made the documented declaration unusable and forced plugins to gate
``index_spec()`` behind a configured horizon. Serial runs must accept the
unbounded declaration; parallel runs must keep failing loudly.
"""

from __future__ import annotations

import datetime as dt
import importlib
from collections.abc import Iterator
from pathlib import Path
from typing import Any, ClassVar

import pytest
from click.testing import CliRunner

from firecube.cli.main import cli
from firecube.core.api import IndexSpec, ItemInfo, TimeAxis
from firecube.ingestor.api import (
    DirectZarrIngestor,
    PipelineBatch,
    PluginContext,
    WriteIntent,
    ZarrArraySpec,
    ZarrGroupSpec,
)
from firecube.ingestor.registry import loader as _loader

pytestmark = pytest.mark.integration

_PLUGIN = "serial_unbounded_axis"
_COORD = "time"
_GROUP = "data"


class _SerialUnboundedIngestor(DirectZarrIngestor):
    PRODUCT_NAME: ClassVar[str] = _PLUGIN
    time_dim_name: ClassVar[str] = _COORD

    def index_spec(self, ctx: PluginContext) -> IndexSpec:
        return IndexSpec(
            name=f"{_PLUGIN}_v1",
            groups={
                _GROUP: TimeAxis.observed(
                    coordinate=_COORD,
                    epoch="2024-01-01T00:00:00Z",
                    cadence_s=600,
                ),
            },
        )

    def inspect_item(self, item: Any, ctx: PluginContext) -> ItemInfo | None:
        if not isinstance(item, (str, dt.datetime)):
            return None
        return ItemInfo(coordinate=item)

    def zarr_schema(self, ctx: PluginContext) -> list[ZarrGroupSpec]:
        return [
            ZarrGroupSpec(
                group=_GROUP,
                arrays=[
                    ZarrArraySpec(
                        name="values",
                        shape=(1, 5),
                        dtype="float32",
                        chunks=(1, 5),
                        fill_value=0.0,
                        time_indexed=True,
                        dimension_names=(_COORD, "x"),
                    ),
                ],
            )
        ]

    def build_write_intents(self, batch: PipelineBatch, ctx: PluginContext) -> list[WriteIntent]:
        return []


@pytest.fixture(autouse=True)
def register_plugin() -> Iterator[None]:
    original_loaded = _loader._LOADED
    original_registry = dict(_loader.AVAILABLE_INGESTORS)
    _loader._LOADED = True
    _loader.AVAILABLE_INGESTORS.clear()
    _SerialUnboundedIngestor.name = _PLUGIN  # pyright: ignore[reportAttributeAccessIssue]
    _loader.AVAILABLE_INGESTORS[_PLUGIN] = _SerialUnboundedIngestor
    yield
    _loader._LOADED = original_loaded
    _loader.AVAILABLE_INGESTORS.clear()
    _loader.AVAILABLE_INGESTORS.update(original_registry)
    importlib.invalidate_caches()


def _ingest_args(tmp_path: Path, *extra: str) -> list[str]:
    input_dir = tmp_path / "input"
    input_dir.mkdir(exist_ok=True)
    (input_dir / "item-20240101T000002.dat").write_text("payload")
    return [
        "ingest",
        _PLUGIN,
        "--input-data",
        str(input_dir),
        "--target",
        f"file://{tmp_path / 'cube.zarr'}",
        "--product-name",
        _PLUGIN,
        "--storage-type",
        "local",
        "--storage-driver",
        "fsspec",
        "--write-mode",
        "direct",
        "--option",
        "no_progress=true",
        *extra,
    ]


def test_serial_run_accepts_unbounded_axis(tmp_path: Path) -> None:
    result = CliRunner().invoke(cli, _ingest_args(tmp_path))
    assert result.exit_code == 0, result.output
    assert "UnboundedAxisError" not in result.output


def test_parallel_run_still_rejects_unbounded_axis(tmp_path: Path) -> None:
    result = CliRunner().invoke(cli, _ingest_args(tmp_path, "--slot-start", "0", "--slot-end", "2"))
    assert result.exit_code != 0
    message = str(result.exception) if result.exception is not None else result.output
    assert "extent" in message.lower(), message
