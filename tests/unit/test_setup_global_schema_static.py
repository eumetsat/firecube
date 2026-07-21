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

"""Static (``time_indexed=False``) array handling in ``_setup_global_zarr_schema``.

Static coordinate arrays such as ``lat``/``lon`` do not have a time axis and
must be created with their declared shape verbatim. The global parallel-schema
setup must skip the time-axis preallocation logic for these specs, and a
group composed entirely of static arrays must not require an entry in
``global_expected_time_count()``.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any, cast

import numpy as np
import zarr

from firecube.core.zarr.region_writer import RegionZarrWriter, group_schema_satisfied
from firecube.ingestor.api import ZarrArraySpec, ZarrGroupSpec
from firecube.ingestor.templates.direct_zarr import _setup_global_zarr_schema


def _strategy(store_uri: str) -> SimpleNamespace:
    return SimpleNamespace(
        _store_uri=store_uri,
        _storage_config=None,
        _session=None,
        _coord_names_by_group={},
    )


def test_static_array_created_without_time_prefix(tmp_path):
    store_path = tmp_path / "static.zarr"
    schema = [
        ZarrGroupSpec(
            group="coords",
            arrays=[
                ZarrArraySpec(
                    name="lat",
                    shape=(4,),
                    dtype="float64",
                    time_indexed=False,
                ),
            ],
        )
    ]

    _setup_global_zarr_schema(
        strategy=_strategy(str(store_path)),
        schema=schema,
        global_expected={},
        product="product",
        run_id="run-1",
        chunk_manager=None,
    )

    arr = cast(Any, zarr.open_group(store=str(store_path), mode="r", zarr_format=3)["coords/lat"])
    assert arr.shape == (4,), f"Expected shape (4,), got {arr.shape}"
    assert np.dtype(arr.dtype) == np.dtype("float64")


def test_mixed_arrays_coexist(tmp_path):
    store_path = tmp_path / "mixed.zarr"
    schema = [
        ZarrGroupSpec(
            group="data",
            arrays=[
                ZarrArraySpec(
                    name="values",
                    shape=(10, 4, 5),
                    dtype=np.float32,
                    chunks=(2, 4, 5),
                    fill_value=0.0,
                ),
                ZarrArraySpec(
                    name="lat",
                    shape=(4,),
                    dtype="float64",
                    time_indexed=False,
                ),
            ],
        )
    ]

    _setup_global_zarr_schema(
        strategy=_strategy(str(store_path)),
        schema=schema,
        global_expected={"data": 10},
        product="product",
        run_id="run-1",
        chunk_manager=None,
    )

    root = zarr.open_group(store=str(store_path), mode="r", zarr_format=3)
    values_arr = cast(Any, root["data/values"])
    lat_arr = cast(Any, root["data/lat"])
    assert values_arr.shape == (10, 4, 5), (
        f"Time-indexed array shape mismatch: expected (10, 4, 5), got {values_arr.shape}"
    )
    assert lat_arr.shape == (4,), f"Static array shape mismatch: expected (4,), got {lat_arr.shape}"


def test_static_schema_satisfied_after_creation(tmp_path):
    store_path = tmp_path / "satisfied.zarr"
    schema = [
        ZarrGroupSpec(
            group="coords",
            arrays=[
                ZarrArraySpec(
                    name="lat",
                    shape=(4,),
                    dtype="float64",
                    time_indexed=False,
                ),
            ],
        )
    ]

    _setup_global_zarr_schema(
        strategy=_strategy(str(store_path)),
        schema=schema,
        global_expected={},
        product="product",
        run_id="run-1",
        chunk_manager=None,
    )

    writer = RegionZarrWriter(str(store_path))
    assert group_schema_satisfied(writer, "coords", schema[0].arrays, expected_time_count=0) is True


def test_schema_satisfied_missing_store_is_read_only(tmp_path):
    store_path = tmp_path / "missing.zarr"
    schema = [
        ZarrGroupSpec(
            group="data",
            arrays=[
                ZarrArraySpec(
                    name="values",
                    shape=(4, 3),
                    dtype=np.float32,
                    chunks=(2, 3),
                ),
            ],
        )
    ]

    writer = RegionZarrWriter(str(store_path))

    assert group_schema_satisfied(writer, "data", schema[0].arrays, expected_time_count=4) is False
    assert not store_path.exists()
