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

"""Regression: empty-discovery must not leave a marked coord shell.

Preallocating an observed-values coord array with ``--input-data`` pointed
at a source that yields zero items would, without a guard, still stamp
``firecube_coord_managed`` on the NaT-shell coord and return exit 0.
Every subsequent ingest then hits the NaT-under-marker check in
``RegionZarrWriter.write_timestamp`` and refuses to write — the store is
effectively bricked without any dedicated reset flag.

This test locks the guard: empty discovery raises ``ClickException`` before
the observed coord shell is created or stamped, so the store stays recoverable
via a corrected ``--input-data``/``--slot-start``/``--slot-end`` invocation.
"""

from __future__ import annotations

import datetime as dt
from collections.abc import Iterator
from pathlib import Path
from typing import Any, ClassVar, cast

import numpy as np
import pytest
import zarr
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
)
from firecube.ingestor.registry import loader as _loader

pytestmark = pytest.mark.integration


_STUB_PLUGIN_NAME = "preallocate_empty_discovery_test_plugin"
_EPOCH_ISO = "2024-01-01T00:00:00Z"
_CADENCE_S = 600
_SLOT_COUNT = 12
_COORD_NAME = "time"
_GROUP = "data"
_VALUES_ARRAY = "values"
_VALUES_X_DIM = 4


class EmptyDiscoveryIngestor(DirectZarrIngestor):
    """Stub plugin whose ``discover_source_files`` always returns ``[]``.

    Declares an observed-values ``RegularTimeAxis`` so ``preallocate`` with
    ``--input-data`` takes the engine-managed materialization path. With
    zero discovered items the guard MUST fire before ``firecube_coord_managed``
    is stamped.
    """

    PRODUCT_NAME: ClassVar[str] = _STUB_PLUGIN_NAME
    time_dim_name: ClassVar[str] = _COORD_NAME

    def index_spec(self, ctx: PluginContext) -> IndexSpec:
        return IndexSpec(
            name=f"{_STUB_PLUGIN_NAME}_v1",
            groups={
                _GROUP: RegularTimeAxis(
                    coordinate=_COORD_NAME,
                    epoch=_EPOCH_ISO,
                    cadence_s=_CADENCE_S,
                    mode="floor",
                    slot_count=_SLOT_COUNT,
                ),
            },
        )

    def inspect_item(self, item: Any, ctx: PluginContext) -> ItemInfo | None:
        if not isinstance(item, int):
            return None
        coord = dt.datetime(2024, 1, 1, tzinfo=dt.UTC) + dt.timedelta(
            seconds=int(item) * _CADENCE_S
        )
        return ItemInfo(coordinate=coord)

    def discover_source_files(self, ctx: PluginContext) -> list[Any]:
        return []

    def zarr_schema(self, ctx: PluginContext) -> list[ZarrGroupSpec]:
        return [
            ZarrGroupSpec(
                group=_GROUP,
                arrays=[
                    ZarrArraySpec(
                        name=_COORD_NAME,
                        shape=(_SLOT_COUNT,),
                        dtype="datetime64[ns]",
                        chunks=None,
                        fill_value=np.datetime64("NaT", "ns"),
                        expected_time_count=_SLOT_COUNT,
                        time_indexed=True,
                        dimension_names=(_COORD_NAME,),
                    ),
                    ZarrArraySpec(
                        name=_VALUES_ARRAY,
                        shape=(_SLOT_COUNT, _VALUES_X_DIM),
                        dtype="float32",
                        chunks=(1, _VALUES_X_DIM),
                        fill_value=0.0,
                        expected_time_count=_SLOT_COUNT,
                        time_indexed=True,
                        dimension_names=(_COORD_NAME, "x"),
                    ),
                ],
            )
        ]

    def build_write_intents(self, batch: PipelineBatch, ctx: PluginContext) -> list[WriteIntent]:
        return []


@pytest.fixture(autouse=True)
def _register_stub_plugin() -> Iterator[None]:
    original_loaded = _loader._LOADED
    original_registry = dict(_loader.AVAILABLE_INGESTORS)

    _loader._LOADED = True
    _loader.AVAILABLE_INGESTORS.clear()
    EmptyDiscoveryIngestor.name = _STUB_PLUGIN_NAME  # pyright: ignore[reportAttributeAccessIssue]
    _loader.AVAILABLE_INGESTORS[_STUB_PLUGIN_NAME] = EmptyDiscoveryIngestor
    try:
        yield
    finally:
        _loader._LOADED = original_loaded
        _loader.AVAILABLE_INGESTORS.clear()
        _loader.AVAILABLE_INGESTORS.update(original_registry)


def _preallocate_args(target: Path, source: Path) -> list[str]:
    return [
        "zarr",
        "preallocate",
        _STUB_PLUGIN_NAME,
        "--target",
        f"file://{target}",
        "--product-name",
        _STUB_PLUGIN_NAME,
        "--storage-type",
        "local",
        "--storage-driver",
        "fsspec",
        "--write-mode",
        "direct",
        "--input-data",
        str(source),
        "--option",
        "no_progress=true",
    ]


def test_empty_discovery_refuses_marker_stamp(tmp_path: Path) -> None:
    """Zero items discovered → exit non-zero, no observed coord shell."""
    target = tmp_path / "empty.zarr"
    source = tmp_path / "src"
    source.mkdir()
    runner = CliRunner()

    result = runner.invoke(cli, _preallocate_args(target, source))

    assert result.exit_code != 0, (
        "empty discovery must fail loudly, not silently create a marked coord shell:\n"
        + result.output
    )
    combined = result.output + (str(result.exception) if result.exception else "")
    assert "no items discovered" in combined, (
        f"error message must name the empty-discovery failure: output={result.output!r} "
        f"exception={result.exception!r}"
    )

    root = zarr.open_group(store=str(target), mode="r", zarr_format=3)
    group = cast(Any, root[_GROUP])
    assert _COORD_NAME not in set(group.array_keys())
