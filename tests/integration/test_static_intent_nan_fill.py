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

from typing import Any, cast

import numpy as np
import pytest
import zarr
from zarr.storage import LocalStore

from firecube.core.errors import SchemaDriftError
from firecube.core.zarr.region_writer import RegionZarrWriter
from firecube.ingestor.runtime.zarr.strategies.indexed_region import (
    IndexedRegionStrategy,
)
from firecube.ingestor.templates.direct_zarr import (
    WriteIntent,
    ZarrArraySpec,
    ZarrGroupSpec,
)

pytestmark = pytest.mark.integration

_GROUP = "g"
_ARRAY = "lat"
_SHAPE = (10, 10)
_FILL_VALUE = np.float32("nan")


def _make_strategy(store_uri: str, shape: tuple[int, ...] = _SHAPE) -> IndexedRegionStrategy:
    schema = [
        ZarrGroupSpec(
            group=_GROUP,
            arrays=[
                ZarrArraySpec(
                    name=_ARRAY,
                    shape=shape,
                    dtype="float32",
                    fill_value=_FILL_VALUE,
                    time_indexed=False,
                )
            ],
        )
    ]
    return IndexedRegionStrategy(store_uri=store_uri, schema=schema)


def _preallocate(tmp_path: Any, shape: tuple[int, ...] = _SHAPE) -> None:
    writer = RegionZarrWriter(f"file://{tmp_path}")
    writer.ensure_group(
        f"{_GROUP}/{_ARRAY}",
        shape=shape,
        dtype="float32",
        fill_value=_FILL_VALUE,
    )


def _static_intent(data: np.ndarray) -> WriteIntent:
    return WriteIntent(group=_GROUP, array=_ARRAY, ts_index=0, data=data, kind="static")


def _read_lat(tmp_path: Any) -> np.ndarray:
    root = zarr.open_group(store=LocalStore(str(tmp_path)), mode="r", zarr_format=3)
    return np.asarray(cast(Any, root[f"{_GROUP}/{_ARRAY}"])[:])


def test_first_static_write_with_nan_fill_succeeds(tmp_path: Any) -> None:
    _preallocate(tmp_path)
    strategy = _make_strategy(f"file://{tmp_path}")
    data = np.zeros(_SHAPE, dtype=np.float32)

    strategy.write_groups(group_to_intents={_GROUP: [_static_intent(data)]})

    np.testing.assert_array_equal(_read_lat(tmp_path), data)


def test_second_static_write_with_nan_data_is_idempotent(tmp_path: Any) -> None:
    # Use (2, 2) shape so the data shape matches the pre-allocated array.
    _2x2 = (2, 2)
    _preallocate(tmp_path, shape=_2x2)
    strategy = _make_strategy(f"file://{tmp_path}", shape=_2x2)
    data = np.array([[1.0, np.nan], [np.nan, 4.0]], dtype=np.float32)

    strategy.write_groups(group_to_intents={_GROUP: [_static_intent(data)]})
    strategy.write_groups(group_to_intents={_GROUP: [_static_intent(data)]})

    result = _read_lat(tmp_path)
    assert np.array_equal(result, data, equal_nan=True)


def test_static_write_with_divergent_nan_positions_raises(tmp_path: Any) -> None:
    # Use (2, 2) shape so the data shape matches the pre-allocated array.
    _2x2 = (2, 2)
    _preallocate(tmp_path, shape=_2x2)
    strategy = _make_strategy(f"file://{tmp_path}", shape=_2x2)
    first = np.array([[1.0, np.nan], [3.0, 4.0]], dtype=np.float32)
    second = np.array([[1.0, 2.0], [np.nan, 4.0]], dtype=np.float32)

    strategy.write_groups(group_to_intents={_GROUP: [_static_intent(first)]})

    with pytest.raises(SchemaDriftError, match="diverged from existing data on resume"):
        strategy.write_groups(group_to_intents={_GROUP: [_static_intent(second)]})
