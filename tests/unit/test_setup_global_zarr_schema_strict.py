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

from types import SimpleNamespace
from typing import Any, cast

import numpy as np
import pytest
import zarr

from firecube.ingestor.api import ZarrArraySpec, ZarrGroupSpec
from firecube.ingestor.errors import SchemaSizeMismatchError
from firecube.ingestor.templates.direct_zarr import _setup_global_zarr_schema


def _strategy(store_uri: str) -> SimpleNamespace:
    return SimpleNamespace(
        _store_uri=store_uri,
        _storage_config=None,
        _session=None,
        _coord_names_by_group={},
    )


def _schema() -> list[ZarrGroupSpec]:
    return [
        ZarrGroupSpec(
            group="data",
            arrays=[
                ZarrArraySpec(name="data", shape=(4, 3, 2), dtype=np.float32, chunks=(2, 3, 2)),
            ],
        )
    ]


def test_existing_matching_array_is_accepted_without_mutation(tmp_path):
    store_path = tmp_path / "existing.zarr"
    root = zarr.open_group(store=str(store_path), mode="w", zarr_format=3)
    root.require_group("data").create_array(
        "data", shape=(4, 3, 2), dtype=np.float32, chunks=(2, 3, 2), fill_value=0.0
    )

    _setup_global_zarr_schema(
        strategy=_strategy(str(store_path)),
        schema=_schema(),
        global_expected={"data": 4},
        product="product",
        run_id="run-1",
        chunk_manager=None,
    )

    arr = cast(Any, zarr.open_group(store=str(store_path), mode="r", zarr_format=3)["data/data"])
    assert arr.shape == (4, 3, 2)
    assert arr.dtype == np.dtype("float32")
    assert arr.chunks == (2, 3, 2)


def test_new_array_is_created_with_global_expected_shape(tmp_path):
    store_path = tmp_path / "new.zarr"

    _setup_global_zarr_schema(
        strategy=_strategy(str(store_path)),
        schema=_schema(),
        global_expected={"data": 4},
        product="product",
        run_id="run-1",
        chunk_manager=None,
    )

    arr = cast(Any, zarr.open_group(store=str(store_path), mode="r", zarr_format=3)["data/data"])
    assert arr.shape == (4, 3, 2)
    assert arr.dtype == np.dtype("float32")
    assert arr.chunks == (2, 3, 2)


def test_undersized_existing_array_raises_schema_size_mismatch(tmp_path):
    store_path = tmp_path / "drift.zarr"
    root = zarr.open_group(store=str(store_path), mode="w", zarr_format=3)
    root.require_group("data").create_array(
        "data", shape=(3, 3, 2), dtype=np.float32, chunks=(2, 3, 2), fill_value=0.0
    )

    with pytest.raises(SchemaSizeMismatchError, match="existing array shape\\[0\\]=3"):
        _setup_global_zarr_schema(
            strategy=_strategy(str(store_path)),
            schema=_schema(),
            global_expected={"data": 4},
            product="product",
            run_id="run-1",
            chunk_manager=None,
        )


def test_missing_global_expected_group_is_skipped_without_store_side_effect(tmp_path):
    store_path = tmp_path / "skipped.zarr"

    _setup_global_zarr_schema(
        strategy=_strategy(str(store_path)),
        schema=[
            ZarrGroupSpec(
                group="unused", arrays=[ZarrArraySpec(name="data", shape=(4, 3), dtype=np.float32)]
            )
        ],
        global_expected={"other": 4},
        product="product",
        run_id="run-1",
        chunk_manager=None,
    )

    assert not store_path.exists()
