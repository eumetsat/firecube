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

"""Protocol contracts for Zarr write strategies."""

from __future__ import annotations

from typing import Protocol

import pytest


def test_append_write_strategy_protocol_exists() -> None:
    from firecube.ingestor.runtime.zarr.contracts import AppendWriteStrategy

    assert issubclass(AppendWriteStrategy, Protocol)
    assert getattr(AppendWriteStrategy, "_is_runtime_protocol", False) is True


def test_region_write_strategy_protocol_exists() -> None:
    from firecube.ingestor.runtime.zarr.contracts import RegionWriteStrategy

    assert issubclass(RegionWriteStrategy, Protocol)
    assert getattr(RegionWriteStrategy, "_is_runtime_protocol", False) is True


def test_append_strategy_implements_append_write_strategy() -> None:
    from firecube.ingestor.runtime.zarr.contracts import AppendWriteStrategy
    from firecube.ingestor.runtime.zarr.strategies.append import AppendStrategy

    strategy = AppendStrategy(store=object())

    assert isinstance(strategy, AppendWriteStrategy)


def test_indexed_region_strategy_implements_region_write_strategy() -> None:
    from firecube.ingestor.runtime.zarr.contracts import RegionWriteStrategy
    from firecube.ingestor.runtime.zarr.strategies.indexed_region import IndexedRegionStrategy

    strategy = IndexedRegionStrategy(store_uri="memory://test.zarr")

    assert isinstance(strategy, RegionWriteStrategy)


def test_zarr_write_strategy_removed() -> None:
    statement = "from firecube.ingestor.runtime.zarr.contracts import " + "Zarr" + "WriteStrategy"

    with pytest.raises(ImportError):
        exec(statement, {})
