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

"""Fixture-owned tests for the irregular-axis test plugin.

These tests exercise each ingestor's ``inspect_item`` and ``index_spec`` hooks
on the canonical synthetic dataset so that failures in the fixture itself are
caught before the shared A7-A12 integration tests run against it.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any, cast

import numpy as np
import pytest
from irregular_axis_test_plugin import (
    IrregularAxisConcreteIngestor,
    IrregularAxisDuplicateIngestor,
    IrregularAxisEmptyIngestor,
    IrregularAxisMissingCoordIngestor,
    IrregularAxisReverseOrderIngestor,
    IrregularAxisSafeIngestor,
    _canonical_timestamps,  # type: ignore[attr-defined]
)

from firecube.core.api import (
    AUTO,
    IndexSpec,
    IrregularTimeAxis,
    ItemInfo,
)


def _fake_ctx() -> Any:
    return SimpleNamespace(options={}, source=None, storage=None)


def _bare[T](cls: type[T]) -> T:
    """Return an instance of ``cls`` bypassing ``__init__``.

    ``BaseIngestor.__init__`` requires a runtime context this fixture-level
    suite does not construct; the six ingestors' hook methods depend only on
    class-level constants, so a bare instance is sufficient.
    """
    return cast(T, object.__new__(cls))


@pytest.mark.parametrize(
    ("cls", "expected_name"),
    [
        (IrregularAxisSafeIngestor, "irregular_axis_safe_v1"),
        (IrregularAxisReverseOrderIngestor, "irregular_axis_reverse_order_v1"),
        (IrregularAxisDuplicateIngestor, "irregular_axis_duplicate_v1"),
        (IrregularAxisEmptyIngestor, "irregular_axis_empty_v1"),
        (IrregularAxisMissingCoordIngestor, "irregular_axis_missing_coord_v1"),
    ],
)
def test_auto_ingestors_declare_irregular_time_axis_with_auto(
    cls: type, expected_name: str
) -> None:
    spec = _bare(cls).index_spec(_fake_ctx())
    assert isinstance(spec, IndexSpec)
    assert spec.name == expected_name
    axis = spec.groups["data"]
    assert isinstance(axis, IrregularTimeAxis)
    assert axis.coordinate == "timestamp"
    assert axis.values is AUTO


def test_concrete_ingestor_declares_five_timestamps_not_auto() -> None:
    spec = _bare(IrregularAxisConcreteIngestor).index_spec(_fake_ctx())
    axis = spec.groups["data"]
    assert isinstance(axis, IrregularTimeAxis)
    assert axis.coordinate == "timestamp"
    assert axis.values is not AUTO
    values = axis.values
    assert values is not AUTO
    assert tuple(cast(tuple[Any, ...], values)) == _canonical_timestamps()


def test_safe_ingestor_discovers_five_monotonic_timestamps() -> None:
    ingestor = _bare(IrregularAxisSafeIngestor)
    ctx = _fake_ctx()
    items = list(ingestor.discover_source_files(ctx))
    assert items == [0, 1, 2, 3, 4]
    assert len(items) == 5
    coords = [ingestor.inspect_item(item, ctx) for item in items]
    assert all(isinstance(info, ItemInfo) for info in coords)
    values: list[Any] = []
    for info in coords:
        assert info is not None
        values.append(info.coordinate)
    expected = list(_canonical_timestamps())
    assert values == expected
    assert values == sorted(values)


def test_reverse_order_ingestor_yields_items_in_reversed_order() -> None:
    ingestor = _bare(IrregularAxisReverseOrderIngestor)
    ctx = _fake_ctx()
    items = list(ingestor.discover_source_files(ctx))
    assert items == [4, 3, 2, 1, 0]
    values: list[Any] = []
    for item in items:
        info = ingestor.inspect_item(item, ctx)
        assert info is not None
        values.append(info.coordinate)
    expected = list(reversed(_canonical_timestamps()))
    assert values == expected


def test_duplicate_ingestor_maps_two_items_to_same_coordinate() -> None:
    ingestor = _bare(IrregularAxisDuplicateIngestor)
    ctx = _fake_ctx()
    items = list(ingestor.discover_source_files(ctx))
    assert len(items) == 5
    assert items == [0, 0, 2, 3, 4]
    coords: list[Any] = []
    for item in items:
        info = ingestor.inspect_item(item, ctx)
        assert info is not None
        coords.append(info.coordinate)
    canonical = _canonical_timestamps()
    assert coords[0] == canonical[0]
    assert coords[1] == canonical[0]
    assert coords[2] == canonical[2]
    assert len(set(coords)) == 4


def test_empty_ingestor_discovers_zero_items() -> None:
    ingestor = _bare(IrregularAxisEmptyIngestor)
    ctx = _fake_ctx()
    assert list(ingestor.discover_source_files(ctx)) == []


def test_missing_coord_ingestor_returns_none_for_item_two() -> None:
    ingestor = _bare(IrregularAxisMissingCoordIngestor)
    ctx = _fake_ctx()
    items = list(ingestor.discover_source_files(ctx))
    assert items == [0, 1, 2, 3, 4]
    coords = [ingestor.inspect_item(item, ctx) for item in items]
    info_missing = coords[2]
    assert info_missing is not None
    assert info_missing.coordinate is None
    canonical = _canonical_timestamps()
    for idx in (0, 1, 3, 4):
        info = coords[idx]
        assert info is not None
        assert info.coordinate == canonical[idx]


def test_concrete_and_safe_agree_on_discovered_coordinates() -> None:
    safe = _bare(IrregularAxisSafeIngestor)
    concrete = _bare(IrregularAxisConcreteIngestor)
    ctx = _fake_ctx()
    safe_coords: list[Any] = []
    safe_items = list(safe.discover_source_files(ctx))
    concrete_items = list(concrete.discover_source_files(ctx))
    assert safe_items == [0, 1, 2, 3, 4]
    assert concrete_items == [0, 1, 2, 3, 4]
    for it in safe_items:
        info = safe.inspect_item(it, ctx)
        assert info is not None
        safe_coords.append(info.coordinate)
    concrete_coords: list[Any] = []
    for it in concrete_items:
        info = concrete.inspect_item(it, ctx)
        assert info is not None
        concrete_coords.append(info.coordinate)
    assert safe_coords == concrete_coords
    assert all(v.dtype == np.dtype("datetime64[ns]") for v in safe_coords)
