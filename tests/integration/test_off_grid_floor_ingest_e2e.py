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

"""End-to-end contract for off-grid floor-mode ingest.

The FCI-shaped scenario: a ``RegularTimeAxis(mode="floor", ...)`` group where
``inspect_item`` reports real observation times that sit at a constant offset
past each cadence boundary (``epoch + i * cadence + 2s``, mirroring FCI's
``+2s`` real-archive offset). The engine materializes the coordinate array
from ``inspect_item`` during ``preallocate`` under the
``firecube_coord_managed`` marker, and the ingest step verifies the stored
coord instead of re-writing it. These tests pin that contract: the stored
coord must equal ``inspect_item.coordinate`` for every ingested slot, must
not collapse to the nominal grid boundary, and must preserve ``NaT`` holes
for uncovered slots.

The stub plugin registered in-file avoids modifying the shared
``regular_axis_test_plugin`` fixture and exercises the real CLI wiring end to
end (no mocks on ``write_timestamp``).
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


_STUB_PLUGIN_NAME = "off_grid_test_plugin"
_EPOCH_ISO = "2024-01-01T00:00:00Z"
_EPOCH_NS = np.datetime64("2024-01-01T00:00:00", "ns")
_CADENCE_S = 600
_SLOT_COUNT = 12
_INGESTED_ITEM_COUNT = 6
_COORD_NAME = "time"
_GROUP = "data"
_OFF_GRID_OFFSET_S = 2
_VALUES_ARRAY = "values"
_VALUES_X_DIM = 4


def _off_grid_time_for_slot(slot: int) -> np.datetime64:
    """FCI-shaped off-grid coordinate: ``epoch + slot*cadence + 2s``."""
    return _EPOCH_NS + np.timedelta64(slot * _CADENCE_S + _OFF_GRID_OFFSET_S, "s").astype(
        "timedelta64[ns]"
    )


def _grid_time_for_slot(slot: int) -> np.datetime64:
    """Nominal grid boundary: ``epoch + slot*cadence``."""
    return _EPOCH_NS + np.timedelta64(slot * _CADENCE_S, "s").astype("timedelta64[ns]")


class OffGridFloorIngestor(DirectZarrIngestor):
    """Inline stub plugin exercising the FCI-shaped off-grid ingest path.

    Declares ``RegularTimeAxis(mode="floor", ...)`` and reports off-grid
    observation times via ``inspect_item``. Emits real slot-data intents
    (``kind="1d"``) but delegates coordinate materialization to the engine —
    the engine writes the coord values from ``inspect_item`` at
    ``preallocate`` time, so plugins do not have to emit
    ``kind="timestamp"`` intents just to keep the coord array in sync.
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
        slot = int(item)
        # Off-grid: shift 2 seconds past the cadence boundary to mirror FCI.
        seconds_since_epoch = slot * _CADENCE_S + _OFF_GRID_OFFSET_S
        coord = dt.datetime(2024, 1, 1, tzinfo=dt.UTC) + dt.timedelta(seconds=seconds_since_epoch)
        return ItemInfo(coordinate=coord)

    def discover_source_files(self, ctx: PluginContext) -> list[Any]:
        # The stub synthesizes items directly instead of reading ctx.source;
        # the input directory contents are irrelevant here. Ingesting the
        # first six slots leaves slots [6..12) uncovered so the NaT-hole
        # contract can be asserted.
        return list(range(_INGESTED_ITEM_COUNT))

    def zarr_schema(self, ctx: PluginContext) -> list[ZarrGroupSpec]:
        return [
            ZarrGroupSpec(
                group=_GROUP,
                arrays=[
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
        resolved = self.resolved_index(ctx)
        intents: list[WriteIntent] = []
        for item in batch.items:
            info = self.inspect_item(item, ctx)
            if info is None:
                continue
            ts_index = resolved.position(_GROUP, info.coordinate)
            intents.append(
                WriteIntent.slot(
                    group=_GROUP,
                    array=_VALUES_ARRAY,
                    index=ts_index,
                    data=np.full(
                        (_VALUES_X_DIM,),
                        float(cast(int, item)),
                        dtype="float32",
                    ),
                )
            )
        return intents


@pytest.fixture(autouse=True)
def _register_off_grid_plugin() -> Iterator[None]:
    """Register ``OffGridFloorIngestor`` under a stable name for each test.

    Mirrors the plugin-registry lifecycle used by other RegularTimeAxis
    integration tests: snapshot state, install the stub, restore on teardown.
    """
    original_loaded = _loader._LOADED
    original_registry = dict(_loader.AVAILABLE_INGESTORS)

    _loader._LOADED = True  # short-circuit entry-point rescan
    _loader.AVAILABLE_INGESTORS.clear()
    cast(Any, OffGridFloorIngestor).name = _STUB_PLUGIN_NAME
    _loader.AVAILABLE_INGESTORS[_STUB_PLUGIN_NAME] = OffGridFloorIngestor
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


def _ingest_args(target: Path, source: Path) -> list[str]:
    return [
        "ingest",
        _STUB_PLUGIN_NAME,
        "--input-data",
        str(source),
        "--target",
        f"file://{target}",
        "--product-name",
        _STUB_PLUGIN_NAME,
        "--storage-type",
        "local",
        "--storage-driver",
        "fsspec",
        "--output-format",
        "zarr",
        "--write-mode",
        "direct",
        "--option",
        "no_progress=true",
    ]


def _open_coord(target: Path) -> Any:
    root = zarr.open_group(store=str(target), mode="r", zarr_format=3)
    return cast(Any, root[f"{_GROUP}/{_COORD_NAME}"])


def _make_source_dir(tmp_path: Path) -> Path:
    """Create the ``--input-data`` directory required by the preallocate CLI.

    The stub synthesizes its own items, so the directory contents are
    irrelevant. The directory itself must still exist because the CLI
    validates the path before dispatch.
    """
    source_dir = tmp_path / "src"
    source_dir.mkdir()
    return source_dir


def test_ingest_fails_with_schema_drift(tmp_path: Path) -> None:
    """End-to-end floor-mode ingest materializes off-grid coords.

    The FCI-shaped contract: after ``preallocate`` + ``ingest`` the stored
    coord values must equal ``inspect_item.coordinate`` for every ingested
    slot. ``preallocate`` materializes engine-managed coord values from
    ``inspect_item.coordinate`` under the ``firecube_coord_managed`` marker;
    the ingest step is a verify-only no-op for the coord array. A regression
    here surfaces either as a :class:`SchemaDriftError` from the ingest step
    (a sealed grid coord rejecting the off-grid time) or as a coord array
    left all-``NaT``.
    """
    target = tmp_path / "off_grid.zarr"
    source = _make_source_dir(tmp_path)
    runner = CliRunner()

    preallocate_result = runner.invoke(cli, _preallocate_args(target, source))
    assert preallocate_result.exit_code == 0, (
        "preallocate must succeed for a floor-mode axis:\n"
        f"exit={preallocate_result.exit_code}\noutput={preallocate_result.output}"
    )

    ingest_result = runner.invoke(cli, _ingest_args(target, source))
    assert ingest_result.exit_code == 0, (
        "ingest must succeed: floor coords are engine-managed at preallocate "
        "time and verified (not re-written) during ingest; a SchemaDriftError "
        "here means the sealed-grid rejection path has regressed:\n"
        f"exit={ingest_result.exit_code}\noutput={ingest_result.output}"
    )

    coord = _open_coord(target)
    stored = np.asarray(coord[:])
    expected = np.asarray(
        [_off_grid_time_for_slot(slot) for slot in range(_INGESTED_ITEM_COUNT)],
        dtype="datetime64[ns]",
    )
    ingested_slice = stored[:_INGESTED_ITEM_COUNT]
    assert np.array_equal(ingested_slice, expected), (
        "stored coord values must equal inspect_item.coordinate for every "
        "ingested slot:\n"
        f"expected={expected!r}\nstored={ingested_slice!r}"
    )
    # Guard against silent drift: the plugin reports off-grid times, so the
    # stored value must not match the nominal grid boundary either.
    grid = np.asarray(
        [_grid_time_for_slot(slot) for slot in range(_INGESTED_ITEM_COUNT)],
        dtype="datetime64[ns]",
    )
    assert not np.array_equal(ingested_slice, grid), (
        "stored coord must not equal the nominal grid — that would mean "
        "the sealed-grid regressed path is back; regime under test is "
        "floor+observed."
    )


def test_uncovered_slots_remain_nat(tmp_path: Path) -> None:
    """Uncovered windows preserve ``NaT`` holes after floor-mode ingest.

    The engine materializes coord values for the ingested window only; slots
    outside that window (``[_INGESTED_ITEM_COUNT.._SLOT_COUNT)``) must
    remain ``NaT`` so downstream readers can distinguish "not ingested yet"
    from "coord value is real observation time".
    """
    target = tmp_path / "off_grid_partial.zarr"
    source = _make_source_dir(tmp_path)
    runner = CliRunner()

    preallocate_result = runner.invoke(cli, _preallocate_args(target, source))
    assert preallocate_result.exit_code == 0, preallocate_result.output

    ingest_result = runner.invoke(cli, _ingest_args(target, source))
    assert ingest_result.exit_code == 0, ingest_result.output

    coord = _open_coord(target)
    stored = np.asarray(coord[:])
    uncovered = stored[_INGESTED_ITEM_COUNT:]
    assert bool(np.all(np.isnat(uncovered))), (
        "uncovered slots must remain NaT; found "
        f"{uncovered!r} at indexes [{_INGESTED_ITEM_COUNT}..{_SLOT_COUNT})"
    )
