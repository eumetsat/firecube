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

from __future__ import annotations

from unittest.mock import MagicMock

from multi_group_capable_test_plugin import (
    MultiGroupCapableTestIngestor,  # type: ignore[reportMissingImports]
)

from firecube.ingestor.api import PluginContext


def test_fixture_loads() -> None:
    assert MultiGroupCapableTestIngestor.PRODUCT_NAME == "multi_group_capable_test_product"


def test_fixture_index_spec_uses_new_api() -> None:
    ctx = MagicMock(spec=PluginContext)
    ingestor = MultiGroupCapableTestIngestor()

    spec = ingestor.index_spec(ctx)

    assert set(spec.groups) == {"group_a", "group_b"}


def test_fixture_discover_returns_400_items() -> None:
    ctx = MagicMock(spec=PluginContext)
    ingestor = MultiGroupCapableTestIngestor()

    items = list(ingestor.discover_source_files(ctx))

    assert len(items) == 400
    assert items[0] == ("group_a", 0)
    assert items[200] == ("group_b", 0)


def test_fixture_zarr_schema_has_two_groups_with_heterogeneous_chunks() -> None:
    ctx = MagicMock(spec=PluginContext)
    ingestor = MultiGroupCapableTestIngestor()

    schema = ingestor.zarr_schema(ctx)

    assert len(schema) == 2
    assert [group.group for group in schema] == ["group_a", "group_b"]

    group_a = schema[0]
    assert len(group_a.arrays) == 4
    assert group_a.arrays[0].name == "primary"
    assert group_a.arrays[0].chunks == (100, 10)
    assert group_a.arrays[1].name == "calibration"
    assert group_a.arrays[1].chunks == (50, 4)
    assert group_a.arrays[2].name == "lat"
    assert group_a.arrays[2].time_indexed is False
    assert group_a.arrays[3].name == "lon"
    assert group_a.arrays[3].time_indexed is False

    group_b = schema[1]
    assert len(group_b.arrays) == 1
    assert group_b.arrays[0].name == "primary"
    assert group_b.arrays[0].chunks == (50, 5)
