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

"""Tests for planning-time AUTO irregular-axis discovery."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest

from firecube.core.errors import (
    DuplicateIrregularCoordinateError,
    MissingIrregularCoordinateError,
    NoDiscoveredItemsError,
)
from firecube.core.index_spec import AUTO, IndexSpec, IntegerAxis, IrregularTimeAxis, ItemInfo
from firecube.ingestor.runtime.index_binding import resolve_index_spec_for_ingestor


class _AutoIngestor:
    def __init__(self, spec: IndexSpec, coordinates: dict[str, Any], items: list[Any]) -> None:
        self._spec = spec
        self._coordinates = coordinates
        self._items = items
        self.inspect_calls: list[Any] = []

    def index_spec(self, ctx: Any) -> IndexSpec:
        return self._spec

    def _resolve_time_dim_name(self) -> str:
        return "time"

    def discover_source_files(self, ctx: Any) -> list[Any]:
        return list(self._items)

    def filter_item(self, item: Any, ctx: Any) -> bool:
        return True

    def inspect_item(self, item: Any, ctx: Any) -> ItemInfo | None:
        self.inspect_calls.append(item)
        coordinate = self._coordinates.get(str(item))
        if coordinate is None:
            return None
        return ItemInfo(coordinate=coordinate)


def _auto_spec(axis: IrregularTimeAxis | None = None) -> IndexSpec:
    return IndexSpec(
        name="auto_irregular_v1",
        groups={"data": axis or IrregularTimeAxis(coordinate="time", values=AUTO)},
    )


def _ctx(source: object = "source") -> SimpleNamespace:
    return SimpleNamespace(source=source)


def test_resolve_index_spec_for_ingestor_discovers_sorts_and_manifests(tmp_path) -> None:
    later = tmp_path / "later.txt"
    earlier = tmp_path / "earlier.txt"
    later.write_text("later", encoding="utf-8")
    earlier.write_text("earlier", encoding="utf-8")
    ingestor = _AutoIngestor(
        _auto_spec(),
        {
            str(later): "2024-01-01T00:10:00Z",
            str(earlier): "2024-01-01T00:00:00Z",
        },
        [later, earlier],
    )

    binding = resolve_index_spec_for_ingestor(ingestor, _ctx(tmp_path))

    assert binding is not None
    assert binding.resolved.size("data") == 2
    assert binding.resolved.coordinate("data", 0) == "2024-01-01T00:00:00Z"
    assert binding.resolved.coordinate("data", 1) == "2024-01-01T00:10:00Z"
    assert binding.resolved.items is not None
    assert [entry.source_ref for entry in binding.resolved.items] == [str(earlier), str(later)]
    assert [entry.source_ref_kind for entry in binding.resolved.items] == ["path", "path"]
    assert [entry.coordinate_value for entry in binding.resolved.items] == [
        "2024-01-01T00:00:00Z",
        "2024-01-01T00:10:00Z",
    ]


def test_resolve_index_spec_for_ingestor_is_deterministic_for_same_source_state(tmp_path) -> None:
    one = tmp_path / "one.txt"
    two = tmp_path / "two.txt"
    one.write_text("one", encoding="utf-8")
    two.write_text("two", encoding="utf-8")
    coordinates = {
        str(two): "2024-01-01T00:10:00Z",
        str(one): "2024-01-01T00:00:00Z",
    }

    first = resolve_index_spec_for_ingestor(
        _AutoIngestor(_auto_spec(), coordinates, [two, one]), _ctx(tmp_path)
    )
    second = resolve_index_spec_for_ingestor(
        _AutoIngestor(_auto_spec(), coordinates, [two, one]), _ctx(tmp_path)
    )

    assert first is not None and second is not None
    assert first.resolved.identity_hash == second.resolved.identity_hash
    first_record = first.resolved.as_resolved_index_record(
        run_id="r", recorded_at="2026-08-24T00:00:00Z"
    )
    second_record = second.resolved.as_resolved_index_record(
        run_id="r", recorded_at="2026-08-24T00:00:00Z"
    )
    assert first_record.to_json_bytes() == second_record.to_json_bytes()


def test_resolve_index_spec_for_ingestor_rejects_duplicate_coordinates(tmp_path) -> None:
    one = tmp_path / "one.txt"
    two = tmp_path / "two.txt"
    one.write_text("one", encoding="utf-8")
    two.write_text("two", encoding="utf-8")
    ingestor = _AutoIngestor(
        _auto_spec(),
        {str(one): "2024-01-01T00:00:00Z", str(two): "2024-01-01T00:00:00Z"},
        [one, two],
    )

    with pytest.raises(DuplicateIrregularCoordinateError, match="coordinates must be unique"):
        resolve_index_spec_for_ingestor(ingestor, _ctx(tmp_path))


def test_resolve_index_spec_for_ingestor_rejects_empty_discovery() -> None:
    ingestor = _AutoIngestor(_auto_spec(), {}, [])

    with pytest.raises(NoDiscoveredItemsError, match="no items found"):
        resolve_index_spec_for_ingestor(ingestor, _ctx("empty-source"))


def test_resolve_index_spec_for_ingestor_rejects_missing_coordinate(tmp_path) -> None:
    item = tmp_path / "missing.txt"
    item.write_text("missing", encoding="utf-8")
    ingestor = _AutoIngestor(_auto_spec(), {}, [item])

    with pytest.raises(MissingIrregularCoordinateError, match="no resolvable coordinate"):
        resolve_index_spec_for_ingestor(ingestor, _ctx(tmp_path))


def test_resolve_index_spec_for_ingestor_does_not_inspect_non_irregular_auto_axes() -> None:
    spec = IndexSpec(name="integer_v1", groups={"data": IntegerAxis(slot_count=2)})
    ingestor = _AutoIngestor(spec, {}, ["a", "b"])

    binding = resolve_index_spec_for_ingestor(ingestor, _ctx())

    assert binding is not None
    assert binding.resolved.items is None
    assert ingestor.inspect_calls == []
