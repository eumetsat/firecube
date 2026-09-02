# Copyright 2025-2026 EUMETSAT
#
# Licensed under the Apache License, Version 2.0 (the "License");

from __future__ import annotations

import datetime as dt
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import numpy as np
import pytest
import zarr

import firecube.core.zarr.coord_materialization as coord_mat
from firecube.core.api import ItemInfo
from firecube.core.errors import SchemaDriftError
from firecube.core.zarr._sealing_markers import ATTR_COORD_MANAGED, ATTR_PREALLOCATED
from firecube.core.zarr.coord_materialization import materialize_regular_coord_array
from firecube.core.zarr.region_writer import RegionZarrWriter

pytestmark = pytest.mark.integration

_GROUP = "data"
_COORD = "time"
_SLOTS = 12
_EPOCH = dt.datetime(2024, 1, 1, tzinfo=dt.UTC)
_CADENCE_S = 600


def _axis(*, mode: str = "floor") -> SimpleNamespace:
    return SimpleNamespace(
        coordinate=_COORD,
        epoch="2024-01-01T00:00:00Z",
        cadence_s=_CADENCE_S,
        mode=mode,
        slot_count=_SLOTS,
    )


def _observed(slot: int) -> dt.datetime:
    return _EPOCH + dt.timedelta(seconds=slot * _CADENCE_S + 7)


def _observed_np(slot: int) -> np.datetime64:
    return np.datetime64(_observed(slot).replace(tzinfo=None), "ns")


class _ResolvedIndex:
    def position(self, group: str, coordinate: Any) -> int:
        assert group == _GROUP
        delta = coordinate - _EPOCH
        return int(delta.total_seconds() // _CADENCE_S)

    def size(self, group: str) -> int:
        assert group == _GROUP
        return _SLOTS


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


def _coord(target: Path) -> Any:
    root = zarr.open_group(store=str(target), mode="r", zarr_format=3)
    return cast(Any, root[f"{_GROUP}/{_COORD}"])


def _materialize(
    target: Path,
    *,
    has_input_data: bool,
    input_slots: tuple[int, ...] = (0, 1, 2),
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
        has_input_data=has_input_data,
        input_data="observed-input",
    )


def test_fresh_observed_with_input_stamps_marker_before_fill(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    target = tmp_path / "cube.zarr"
    original = coord_mat.write_observed_regular_coord_values

    def assert_marker_then_fill(**kwargs: Any) -> tuple[int, int]:
        arr = kwargs["arr"]
        assert arr.attrs[ATTR_COORD_MANAGED] is True
        assert ATTR_PREALLOCATED not in dict(arr.attrs)
        return original(**kwargs)

    monkeypatch.setattr(
        coord_mat,
        "write_observed_regular_coord_values",
        assert_marker_then_fill,
    )

    _materialize(target, has_input_data=True, input_slots=(0, 1, 2))

    coord = _coord(target)
    assert coord.attrs[ATTR_COORD_MANAGED] is True
    assert ATTR_PREALLOCATED not in dict(coord.attrs)
    values = np.asarray(coord[:3]).astype("datetime64[ns]")
    expected = np.asarray([_observed_np(i) for i in range(3)], dtype="datetime64[ns]")
    assert np.array_equal(values, expected)


def test_fresh_observed_without_input_creates_marked_nat_shell(tmp_path: Path) -> None:
    target = tmp_path / "cube.zarr"

    _materialize(target, has_input_data=False)

    coord = _coord(target)
    assert coord.attrs[ATTR_COORD_MANAGED] is True
    assert ATTR_PREALLOCATED not in dict(coord.attrs)
    assert bool(np.all(np.isnat(np.asarray(coord[:]))))


def test_exact_prefill_keeps_only_preallocated_marker(tmp_path: Path) -> None:
    target = tmp_path / "cube.zarr"
    writer = _writer(target)

    materialize_regular_coord_array(
        writer=writer,
        root=writer._open_root(),
        group_name=_GROUP,
        axis=_axis(mode="exact"),
        spec=None,
    )

    coord = _coord(target)
    assert coord.attrs[ATTR_PREALLOCATED] is True
    assert ATTR_COORD_MANAGED not in dict(coord.attrs)
    assert not np.any(np.isnat(np.asarray(coord[:])))


def test_existing_observed_unmarked_refuses_without_retroactive_stamp(tmp_path: Path) -> None:
    target = tmp_path / "cube.zarr"
    root = zarr.open_group(store=str(target), mode="w", zarr_format=3)
    group = root.create_group(_GROUP)
    group.create_array(
        _COORD,
        shape=(_SLOTS,),
        dtype="datetime64[ns]",
        fill_value=np.datetime64("NaT", "ns"),
        chunks=(_SLOTS,),
        dimension_names=[_COORD],
    )
    writer = _writer(target)

    with pytest.raises(SchemaDriftError, match=r"legacy.*firecube chunks"):
        materialize_regular_coord_array(
            writer=writer,
            root=writer._open_root(),
            group_name=_GROUP,
            axis=_axis(),
            spec=None,
            resolved_index=_ResolvedIndex(),
            ingestor=_Ingestor((0, 1)),
            plugin_ctx=object(),
            has_input_data=True,
            input_data="observed-input",
        )

    assert ATTR_COORD_MANAGED not in dict(_coord(target).attrs)


def test_existing_observed_marked_preserves_current_materialization_path(tmp_path: Path) -> None:
    target = tmp_path / "cube.zarr"
    _materialize(target, has_input_data=False)

    _materialize(target, has_input_data=True, input_slots=(0, 1))

    coord = _coord(target)
    assert coord.attrs[ATTR_COORD_MANAGED] is True
    assert ATTR_PREALLOCATED not in dict(coord.attrs)
    expected = np.asarray([_observed_np(0), _observed_np(1)], dtype="datetime64[ns]")
    assert np.array_equal(np.asarray(coord[:2]).astype("datetime64[ns]"), expected)


def test_marker_write_failure_raises_cleanup_guidance() -> None:
    class _FailingAttrs(dict[str, Any]):
        def __setitem__(self, key: str, value: Any) -> None:
            if key == ATTR_COORD_MANAGED:
                raise OSError("attrs store unavailable")
            super().__setitem__(key, value)

    class _Arr:
        attrs = _FailingAttrs()

    class _Writer:
        def ensure_group(self, *args: Any, **kwargs: Any) -> _Arr:
            return _Arr()

    with pytest.raises(SchemaDriftError, match="firecube chunks"):
        materialize_regular_coord_array(
            writer=_Writer(),
            root={},
            group_name=_GROUP,
            axis=_axis(),
            spec=None,
            has_input_data=False,
        )
