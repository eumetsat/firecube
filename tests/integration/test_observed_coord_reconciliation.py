# Copyright 2025-2026 EUMETSAT
#
# Licensed under the Apache License, Version 2.0 (the "License");

from __future__ import annotations

import datetime as dt
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Literal, cast

import numpy as np
import pytest
import zarr

from firecube.core.api import ItemInfo
from firecube.core.errors import SchemaDriftError
from firecube.core.zarr._sealing_markers import ATTR_COORD_MANAGED, ATTR_PREALLOCATED
from firecube.core.zarr.coord_materialization import NOTICE_LEVEL, materialize_regular_coord_array
from firecube.core.zarr.region_writer import RegionZarrWriter

pytestmark = pytest.mark.integration

_GROUP = "data"
_COORD = "time"
_SLOT_COUNT = 12
_EPOCH = dt.datetime(2024, 1, 1, tzinfo=dt.UTC)
_CADENCE_S = 600
_OBSERVED_OFFSET_S = 7


def _axis() -> SimpleNamespace:
    return SimpleNamespace(
        coordinate=_COORD,
        epoch="2024-01-01T00:00:00Z",
        cadence_s=_CADENCE_S,
        mode="floor",
        slot_count=_SLOT_COUNT,
    )


def _observed(slot: int) -> dt.datetime:
    return _EPOCH + dt.timedelta(seconds=slot * _CADENCE_S + _OBSERVED_OFFSET_S)


def _observed_np(slot: int) -> np.datetime64:
    return np.datetime64(_observed(slot).replace(tzinfo=None), "ns")


class _ResolvedIndex:
    def position(self, group: str, coordinate: Any) -> int:
        assert group == _GROUP
        delta = coordinate - _EPOCH
        return int(delta.total_seconds() // _CADENCE_S)

    def size(self, group: str) -> int:
        assert group == _GROUP
        return _SLOT_COUNT


class _Ingestor:
    def __init__(self, slots: tuple[int, ...]) -> None:
        self._slots = slots

    def discover_source_files(self, ctx: Any) -> list[int]:
        return list(self._slots)

    def filter_item(self, item: int, ctx: Any) -> bool:
        return True

    def inspect_item(self, item: int, ctx: Any) -> ItemInfo:
        return ItemInfo(coordinate=_observed(item))


def _writer(target: Path) -> RegionZarrWriter:
    return RegionZarrWriter(str(target), time_coord_name=_COORD)


def _coord(target: Path, mode: Literal["r", "r+", "a", "w", "w-"] = "r") -> Any:
    root = zarr.open_group(store=str(target), mode=mode, zarr_format=3)
    return cast(Any, root[f"{_GROUP}/{_COORD}"])


def _nat() -> np.datetime64:
    return cast(np.datetime64, np.array(np.datetime64("NaT", "ns"), dtype="datetime64[ns]")[()])


def _materialize(
    target: Path,
    *,
    has_input_data: bool,
    input_slots: tuple[int, ...] = (0, 1, 2),
    slot_start: int = 0,
    slot_end: int | None = None,
) -> None:
    writer = _writer(target)
    materialize_regular_coord_array(
        writer=writer,
        root=writer._open_root(),
        group_name=_GROUP,
        axis=_axis(),
        spec=None,
        resolved_index=_ResolvedIndex(),
        ingestor=_Ingestor(input_slots),
        plugin_ctx=object(),
        slot_start=slot_start,
        slot_end=slot_end,
        has_input_data=has_input_data,
        input_data="observed-input",
    )


def test_crash_resume_reconciles_nat_slots(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    target = tmp_path / "crash-resume.zarr"
    _materialize(target, has_input_data=False)
    coord = _coord(target, mode="a")
    coord[0] = _observed_np(0)
    coord[1] = _nat()
    coord[2] = _observed_np(2)

    caplog.set_level(NOTICE_LEVEL, logger="firecube.core.zarr.coord_materialization")
    _materialize(target, has_input_data=True, input_slots=(0, 1, 2), slot_end=3)

    values = np.asarray(_coord(target)[:3]).astype("datetime64[ns]")
    expected = np.asarray([_observed_np(0), _observed_np(1), _observed_np(2)])
    np.testing.assert_array_equal(values, expected)
    assert any(record.levelname == "NOTICE" for record in caplog.records)
    assert "NaT observed coord slot" in caplog.text


def test_window_extension_reconciles_overlap_and_new_slots(tmp_path: Path) -> None:
    target = tmp_path / "window-extension.zarr"
    _materialize(target, has_input_data=True, input_slots=(0, 1, 2), slot_start=0, slot_end=3)

    _materialize(target, has_input_data=True, input_slots=(2, 3, 4), slot_start=2, slot_end=5)

    coord = _coord(target)

    assert coord.attrs[ATTR_COORD_MANAGED] is True
    assert ATTR_PREALLOCATED not in dict(coord.attrs)
    values = np.asarray(coord[:]).astype("datetime64[ns]")
    for slot in range(5):
        assert values[slot] == _observed_np(slot)
    assert bool(np.all(np.isnat(values[5:])))


def test_divergent_reconciliation_refuses_slot_with_both_values(tmp_path: Path) -> None:
    target = tmp_path / "divergent.zarr"
    _materialize(target, has_input_data=True, input_slots=(0, 1, 2), slot_start=0, slot_end=3)
    coord = _coord(target, mode="a")
    stored = _observed_np(1) + np.timedelta64(1, "s")
    coord[1] = stored

    with pytest.raises(SchemaDriftError) as exc_info:
        _materialize(target, has_input_data=True, input_slots=(0, 1, 2), slot_start=0, slot_end=3)

    message = str(exc_info.value)
    assert "slot 1" in message
    assert "stored value" in message
    assert "incoming value" in message
    assert str(stored) in message
    assert str(_observed_np(1)) in message
    assert np.asarray(_coord(target)[1]).astype("datetime64[ns]") == stored
