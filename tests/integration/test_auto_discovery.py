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

"""Integration placeholder for the irregular-axis AUTO fixture plugin.

The A6 fixture plugin is optional until that task lands. This file becomes an
integration lane as soon as ``irregular_axis_test_plugin`` is installed.
"""

from types import SimpleNamespace
from typing import Any, cast

import pytest

from firecube.core.errors import (
    DuplicateIrregularCoordinateError,
    NoDiscoveredItemsError,
)
from firecube.ingestor.runtime.index_binding import resolve_index_spec_for_ingestor

pytestmark = pytest.mark.integration

plugin = pytest.importorskip(
    "irregular_axis_test_plugin",
    reason="irregular_axis_test_plugin fixture is installed by A6",
)


def _ctx() -> SimpleNamespace:
    return SimpleNamespace(source="fixture-source", options={}, storage=None)


def _bare(cls: type[Any]) -> Any:
    return cast(Any, object.__new__(cls))


def test_fixture_safe_ingestor_discovers_manifest_and_sorted_axis() -> None:
    binding = resolve_index_spec_for_ingestor(_bare(plugin.IrregularAxisSafeIngestor), _ctx())

    assert binding is not None
    assert binding.resolved.size("data") == 5
    assert binding.resolved.items is not None
    assert len(binding.resolved.items) == 5
    assert [entry.coordinate_value for entry in binding.resolved.items] == sorted(
        entry.coordinate_value for entry in binding.resolved.items
    )


def test_fixture_reverse_order_ingestor_resolves_to_sorted_axis() -> None:
    binding = resolve_index_spec_for_ingestor(
        _bare(plugin.IrregularAxisReverseOrderIngestor), _ctx()
    )

    assert binding is not None
    assert [binding.resolved.coordinate("data", index) for index in range(5)] == sorted(
        binding.resolved.coordinate("data", index) for index in range(5)
    )


def test_fixture_duplicate_ingestor_raises_duplicate_error() -> None:
    with pytest.raises(DuplicateIrregularCoordinateError, match="coordinates must be unique"):
        resolve_index_spec_for_ingestor(_bare(plugin.IrregularAxisDuplicateIngestor), _ctx())


def test_fixture_empty_ingestor_raises_empty_error() -> None:
    with pytest.raises(NoDiscoveredItemsError):
        resolve_index_spec_for_ingestor(_bare(plugin.IrregularAxisEmptyIngestor), _ctx())
