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

from pathlib import Path
from typing import Any, cast

import numpy as np
import pytest
import zarr
from zarr.storage import LocalStore

from firecube.ingestor.runtime.zarr.strategies.indexed_region import IndexedRegionStrategy
from firecube.ingestor.templates.direct_zarr import WriteIntent, ZarrArraySpec, ZarrGroupSpec

pytestmark = pytest.mark.unit


def test_static_array_high_ts_index_no_bounds_error(tmp_path: Path) -> None:
    store_uri = f"file://{tmp_path / 'store.zarr'}"
    schema = [
        ZarrGroupSpec(
            group="grp",
            arrays=[
                ZarrArraySpec(
                    name="static_grid",
                    shape=(4, 5),
                    dtype=np.float32,
                    time_indexed=False,
                )
            ],
        )
    ]
    strategy = IndexedRegionStrategy(store_uri=store_uri, schema=schema)

    strategy.write_groups(group_to_intents={"grp": []})

    strategy.write_groups(
        group_to_intents={
            "grp": [
                WriteIntent(
                    group="grp",
                    array="timestamp",
                    ts_index=622080,
                    data=np.datetime64("2026-01-01T00:00:00"),
                    kind="timestamp",
                    timestamp_val=np.datetime64("2026-01-01T00:00:00"),
                )
            ]
        }
    )


def test_static_array_also_in_coord_names_still_works(tmp_path: Path) -> None:
    store_path = tmp_path / "store.zarr"
    store_uri = f"file://{store_path}"
    schema = [
        ZarrGroupSpec(
            group="grp",
            arrays=[
                ZarrArraySpec(
                    name="static_grid",
                    shape=(4, 5),
                    dtype=np.float32,
                    time_indexed=False,
                )
            ],
            coord_names=frozenset({"y", "x", "channel", "static_grid"}),
        )
    ]
    strategy = IndexedRegionStrategy(store_uri=store_uri, schema=schema)

    strategy.write_groups(group_to_intents={"grp": []})
    strategy.write_groups(
        group_to_intents={
            "grp": [
                WriteIntent(
                    group="grp",
                    array="timestamp",
                    ts_index=622080,
                    data=np.datetime64("2026-01-01T00:00:00"),
                    kind="timestamp",
                    timestamp_val=np.datetime64("2026-01-01T00:00:00"),
                )
            ]
        }
    )

    root = zarr.open_group(store=LocalStore(store_path), mode="r", zarr_format=3)
    assert cast(Any, root["grp/static_grid"]).shape == (4, 5)
    assert cast(Any, root["grp/timestamp"]).shape[0] == 622081
