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

"""End-to-end coverage for region writes into arrays of rank > 4.

An ensemble-forecast product is a natural rank-5/6 cube::

    temperature         (timestamp, realization, forecast_period, pressure, y, x)  6-D
    screen_temperature  (timestamp, realization, forecast_period, y, x)            5-D

The inline ingestor writes each member of each slot with its own
``IndexedWrite.region`` (``y_slice`` = the member's position along
``realization``, the first axis after time) and a lazy payload, plus a lazy
``IndexedWrite.slot`` for a per-slot coordinate. Arrays are sharded one member
of one slot per shard. The last slot runs fewer members, so one member position
must stay at the fill value. Every value encodes its slot, member, period and
level, so a misplaced block is detected by value.
"""

from __future__ import annotations

from collections.abc import Iterable, Iterator
from functools import partial
from pathlib import Path
from typing import Any, ClassVar, cast

import numpy as np
import pytest
import zarr
from click.testing import CliRunner

from firecube.cli.main import cli
from firecube.core.api import IndexedWrite, IndexSpec, ItemInfo, RegularTimeAxis
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

_EPOCH = "2026-01-01T00:00:00Z"
_CADENCE_S = 6 * 3600
_SLOTS = 3
_MEMBERS = 3
_PERIODS = 2
_LEVELS = 2
_Y, _X = 4, 5
_BASE = np.datetime64("2026-01-01T00:00:00", "ns")
# Members run in each slot; the last slot runs fewer than the array holds.
_MEMBERS_RUN = {0: (0, 1, 2), 1: (0, 1, 2), 2: (0, 1)}


def _slot_time(slot: int) -> np.datetime64:
    return _BASE + np.timedelta64(slot * _CADENCE_S, "s")


def _member_block(slot: int, member: int, with_levels: bool) -> np.ndarray:
    """``(1, period, [level,] y, x)``; every value encodes slot, member, period, level."""
    period = np.arange(_PERIODS).reshape(-1, 1)
    level = np.arange(_LEVELS).reshape(1, -1)
    values = 1000.0 * slot + 100.0 * member + 10.0 * period + (level if with_levels else 0.0)
    if not with_levels:
        values = values[:, :1]
    block = np.broadcast_to(values[..., None, None], (*values.shape, _Y, _X))
    if not with_levels:
        block = block[:, 0]
    return np.ascontiguousarray(block[None], dtype=np.float32)


def _valid_times(slot: int) -> np.ndarray:
    hours = np.arange(_PERIODS) * 6
    return np.array([_slot_time(slot) + np.timedelta64(int(h), "h") for h in hours])


def _expected(with_levels: bool) -> np.ndarray:
    level_shape = (_LEVELS,) if with_levels else ()
    out = np.full((_SLOTS, _MEMBERS, _PERIODS, *level_shape, _Y, _X), np.nan, np.float32)
    for slot, members in _MEMBERS_RUN.items():
        for member in members:
            out[slot, member] = _member_block(slot, member, with_levels)[0]
    return out


def _data_spec(name: str, with_levels: bool) -> ZarrArraySpec:
    level_dims = ("pressure",) if with_levels else ()
    level_shape = (_LEVELS,) if with_levels else ()
    return ZarrArraySpec(
        name=name,
        shape=(_SLOTS, _MEMBERS, _PERIODS, *level_shape, _Y, _X),
        dtype="float32",
        chunks=(1, 1, 1, *(1,) * len(level_shape), _Y, _X),
        shards=(1, 1, _PERIODS, *level_shape, _Y, _X),
        fill_value=np.float32(np.nan),
        dimension_names=("timestamp", "realization", "forecast_period", *level_dims, "y", "x"),
    )


class _EnsembleIngestor(DirectZarrIngestor):
    """Writes one member per region write into 6-D and 5-D arrays."""

    PRODUCT_NAME: ClassVar[str] = "nd_ensemble"
    channel_index: ClassVar[int | None] = None

    def discover_source_files(self, ctx: PluginContext) -> Iterable[Any]:
        return list(range(_SLOTS))

    def index_spec(self, ctx: PluginContext) -> IndexSpec:
        axis = RegularTimeAxis(
            coordinate="timestamp",
            epoch=_EPOCH,
            cadence_s=_CADENCE_S,
            mode="exact",
            slot_count=_SLOTS,
        )
        return IndexSpec(name="nd_ensemble_v1", groups={"ensemble": axis})

    def inspect_item(self, item: Any, ctx: PluginContext) -> ItemInfo | None:
        return ItemInfo(coordinate=_slot_time(item)) if isinstance(item, int) else None

    def zarr_schema(self, ctx: PluginContext) -> list[ZarrGroupSpec]:
        return [
            ZarrGroupSpec(
                group="ensemble",
                coord_names=frozenset({"timestamp", "valid_time"}),
                arrays=[
                    ZarrArraySpec(
                        name="timestamp",
                        shape=(_SLOTS,),
                        dtype="datetime64[ns]",
                        chunks=(1,),
                        fill_value=np.datetime64("NaT", "ns"),
                        dimension_names=("timestamp",),
                    ),
                    ZarrArraySpec(
                        name="valid_time",
                        shape=(_SLOTS, _PERIODS),
                        dtype="datetime64[ns]",
                        chunks=(1, _PERIODS),
                        fill_value=np.datetime64("NaT", "ns"),
                        dimension_names=("timestamp", "forecast_period"),
                    ),
                    _data_spec("temperature", with_levels=True),
                    _data_spec("screen_temperature", with_levels=False),
                ],
            )
        ]

    def build_write_intents(
        self, batch: PipelineBatch, ctx: PluginContext
    ) -> list[WriteIntent | IndexedWrite]:
        out: list[WriteIntent | IndexedWrite] = []
        for slot in batch.items:
            coordinate = _slot_time(slot)
            out.append(
                IndexedWrite.slot(
                    group="ensemble",
                    array="valid_time",
                    coordinate=coordinate,
                    data=partial(_valid_times, slot),
                )
            )
            for member in _MEMBERS_RUN[slot]:
                for array, with_levels in (("temperature", True), ("screen_temperature", False)):
                    out.append(
                        IndexedWrite.region(
                            group="ensemble",
                            array=array,
                            coordinate=coordinate,
                            data=partial(_member_block, slot, member, with_levels),
                            y_slice=slice(member, member + 1),
                            channel_index=self.channel_index,
                        )
                    )
        return out


class _ChannelIndexOnRank6Ingestor(_EnsembleIngestor):
    """Asks for a rank-4-only channel selection on 6-D and 5-D arrays."""

    PRODUCT_NAME: ClassVar[str] = "nd_ensemble_channel"
    channel_index: ClassVar[int | None] = 0


_INLINE_INGESTORS: dict[str, type[DirectZarrIngestor]] = {
    "nd_ensemble": _EnsembleIngestor,
    "nd_ensemble_channel": _ChannelIndexOnRank6Ingestor,
}


@pytest.fixture(autouse=True)
def _register_inline_ingestors() -> Iterator[None]:
    """Register the inline ingestors for CLI lookups; restore the registry afterwards."""
    original_loaded = _loader._LOADED
    original_registry = dict(_loader.AVAILABLE_INGESTORS)
    for name, cls in _INLINE_INGESTORS.items():
        _loader.AVAILABLE_INGESTORS[name] = cls
        cls.name = name  # register_ingestor would normally set this
    _loader._LOADED = True
    yield
    _loader._LOADED = original_loaded
    _loader.AVAILABLE_INGESTORS.clear()
    _loader.AVAILABLE_INGESTORS.update(original_registry)


def _run(head: list[str], plugin: str, target: Path, *extra: str) -> Any:
    args = [*head, plugin, "--target", f"file://{target}", "--product-name", plugin]
    args += ["--storage-type", "local", "--storage-driver", "fsspec", "--write-mode", "direct"]
    args += ["--option", "no_progress=true", *extra]
    return CliRunner().invoke(cli, args)


def _array(target: Path, path: str) -> zarr.Array:
    root = zarr.open_group(str(target), mode="r", zarr_format=3)
    return cast(zarr.Array, root[path])


def _assert_store(target: Path) -> None:
    temperature = _array(target, "ensemble/temperature")
    screen = _array(target, "ensemble/screen_temperature")
    assert temperature.shards == (1, 1, _PERIODS, _LEVELS, _Y, _X)
    assert screen.shards == (1, 1, _PERIODS, _Y, _X)
    np.testing.assert_array_equal(temperature[...], _expected(with_levels=True))
    np.testing.assert_array_equal(screen[...], _expected(with_levels=False))
    expected_times = np.stack([_valid_times(slot) for slot in range(_SLOTS)])
    np.testing.assert_array_equal(_array(target, "ensemble/valid_time")[...], expected_times)


def test_nd_ensemble_ingest_writes_each_member_at_its_position(tmp_path: Path) -> None:
    """A serial ingest places every member block; the member not run stays at fill."""
    target = tmp_path / "serial.zarr"
    result = _run(["ingest"], "nd_ensemble", target)
    assert result.exit_code == 0, result.output
    _assert_store(target)


def test_nd_ensemble_slot_range_workers_write_disjoint_slots(tmp_path: Path) -> None:
    """Preallocate, then one slot-range worker per slot, as parallel workers run."""
    target = tmp_path / "workers.zarr"
    assert _run(["zarr", "preallocate"], "nd_ensemble", target).exit_code == 0
    for slot in range(_SLOTS):
        result = _run(
            ["ingest"],
            "nd_ensemble",
            target,
            "--slot-start",
            str(slot),
            "--slot-end",
            str(slot + 1),
            "--suppress-static-emission-for-non-owner",
            "--static-owner-slot-start",
            "0",
        )
        assert result.exit_code == 0, f"slot {slot}: {result.output}"
    _assert_store(target)

    repeat = _run(["ingest"], "nd_ensemble", target, "--slot-start", "1", "--slot-end", "2")
    assert repeat.exit_code != 0
    assert "overlap" in repeat.output


def test_nd_region_write_refuses_channel_index_on_rank_above_4(tmp_path: Path) -> None:
    """``channel_index`` is a rank-4 selection; on rank 6 the ingest fails loudly."""
    target = tmp_path / "channel.zarr"
    result = _run(["ingest"], "nd_ensemble_channel", target)
    assert result.exit_code != 0
    assert "channel_index is only supported for rank-4 arrays" in result.output
    assert np.isnan(np.asarray(_array(target, "ensemble/temperature")[...])).all()
