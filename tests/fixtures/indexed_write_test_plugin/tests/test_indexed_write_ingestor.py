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

"""Fixture-owned tests for the indexed_write test plugin.

Every ingestor's ``index_spec`` / ``build_indexed_write`` hook is exercised
here on a bare instance so that fixture-level breakage is caught before the
shared B-plan integration tests consume these plugins.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any, cast

import numpy as np
import pytest
from indexed_write_test_plugin import (
    IndexedWriteDropIngestor,
    IndexedWriteErrorIngestor,
    IndexedWriteFanOutIngestor,
    IndexedWriteSingleIngestor,
    IndexedWriteWithStaticsIngestor,
    _canonical_timestamps,  # type: ignore[attr-defined]
)

from firecube.core.api import IndexedWrite, IndexSpec, RegularTimeAxis
from firecube.core.errors import IndexedWriteCompilationError
from firecube.core.index_resolve import resolve_index_spec
from firecube.ingestor.templates.direct_zarr import (
    _compile_indexed_write,  # type: ignore[attr-defined]
)


def _fake_ctx() -> Any:
    return SimpleNamespace(options={}, source=None, storage=None)


def _bare[T](cls: type[T]) -> T:
    return cast(T, object.__new__(cls))


@pytest.mark.parametrize(
    ("cls", "expected_name", "expected_slots"),
    [
        (IndexedWriteSingleIngestor, "indexed_write_single_v1", 5),
        (IndexedWriteFanOutIngestor, "indexed_write_fan_out_v1", 3),
        (IndexedWriteWithStaticsIngestor, "indexed_write_with_statics_v1", 5),
        (IndexedWriteErrorIngestor, "indexed_write_error_v1", 5),
        (IndexedWriteDropIngestor, "indexed_write_drop_v1", 5),
    ],
)
def test_index_spec_declares_expected_regular_axis(
    cls: type, expected_name: str, expected_slots: int
) -> None:
    spec = _bare(cls).index_spec(_fake_ctx())
    assert isinstance(spec, IndexSpec)
    assert spec.name == expected_name
    axis = spec.groups["data"]
    assert isinstance(axis, RegularTimeAxis)
    assert axis.coordinate == "timestamp"
    assert axis.cadence_s == 300
    assert axis.slot_count == expected_slots


def test_single_ingestor_returns_one_indexed_write_per_item() -> None:
    ingestor = _bare(IndexedWriteSingleIngestor)
    ctx = _fake_ctx()
    items = list(ingestor.discover_source_files(ctx))
    assert items == [0, 1, 2, 3, 4]
    results = [ingestor.build_indexed_write(item, ctx) for item in items]
    assert len(results) == 5
    coords = _canonical_timestamps()
    for idx, result in enumerate(results):
        assert isinstance(result, IndexedWrite)
        assert result._kind == "region"
        assert result.group == "data"
        assert result.array == "values"
        assert result.coordinate == coords[idx]
        assert result.y_slice == slice(0, 3)
        payload = result.data
        assert isinstance(payload, np.ndarray)
        assert payload.shape == (3, 4)
        assert payload.dtype == np.float32


def test_fan_out_ingestor_returns_two_indexed_writes_per_item() -> None:
    ingestor = _bare(IndexedWriteFanOutIngestor)
    ctx = _fake_ctx()
    items = list(ingestor.discover_source_files(ctx))
    assert items == [0, 1, 2]
    coords = _canonical_timestamps(3)
    for idx, item in enumerate(items):
        result = ingestor.build_indexed_write(item, ctx)
        assert not isinstance(result, IndexedWrite)
        assert result is not None
        seq = list(result)
        assert len(seq) == 2
        first, second = seq
        assert isinstance(first, IndexedWrite)
        assert isinstance(second, IndexedWrite)
        assert first.coordinate == coords[idx]
        assert second.coordinate == coords[idx]
        assert first.y_slice == slice(0, 1)
        assert second.y_slice == slice(1, 2)


def test_drop_ingestor_returns_none_for_item_two() -> None:
    ingestor = _bare(IndexedWriteDropIngestor)
    ctx = _fake_ctx()
    items = list(ingestor.discover_source_files(ctx))
    assert items == [0, 1, 2, 3, 4]
    results = [ingestor.build_indexed_write(item, ctx) for item in items]
    assert results[2] is None
    non_none = [r for r in results if r is not None]
    assert len(non_none) == 4
    for r in non_none:
        assert isinstance(r, IndexedWrite)


def test_error_ingestor_returns_indexed_write_with_unresolvable_coordinate() -> None:
    ingestor = _bare(IndexedWriteErrorIngestor)
    ctx = _fake_ctx()
    result = ingestor.build_indexed_write(0, ctx)
    assert isinstance(result, IndexedWrite)
    assert result.coordinate == "NOT_IN_INDEX"


def test_error_ingestor_compilation_raises_indexed_write_compilation_error() -> None:
    ingestor = _bare(IndexedWriteErrorIngestor)
    ctx = _fake_ctx()
    spec = ingestor.index_spec(ctx)
    resolved = resolve_index_spec(spec, time_dim_name="timestamp")
    iw = ingestor.build_indexed_write(0, ctx)
    assert isinstance(iw, IndexedWrite)
    with pytest.raises(IndexedWriteCompilationError) as excinfo:
        _compile_indexed_write(iw, resolved)
    assert excinfo.value.coordinate == "NOT_IN_INDEX"


def test_with_statics_ingestor_declares_static_lat_alongside_time_indexed_values() -> None:
    ingestor = _bare(IndexedWriteWithStaticsIngestor)
    ctx = _fake_ctx()
    schema = ingestor.zarr_schema(ctx)
    assert len(schema) == 1
    group = schema[0]
    assert group.group == "data"
    by_name = {arr.name: arr for arr in group.arrays}
    assert set(by_name) == {"values", "lat"}
    assert by_name["values"].time_indexed is True
    assert by_name["values"].shape == (5, 3, 4)
    assert by_name["lat"].time_indexed is False
    assert by_name["lat"].shape == (3,)
