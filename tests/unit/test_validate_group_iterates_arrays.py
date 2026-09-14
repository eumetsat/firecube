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

import numpy as np
import pytest
import zarr

from firecube.core.zarr.validation import validate_group_with_fs
from tests.helpers.storage import make_local_session

pytestmark = pytest.mark.unit


def _seed_group_with_time_arrays(store_path: Path, names: tuple[str, ...]) -> None:
    root = zarr.open_group(store=str(store_path), mode="w", zarr_format=3)
    group = root.require_group("G")
    group.create_array(
        "timestamp",
        data=np.arange(4, dtype=np.int64),
        chunks=(2,),
        dimension_names=("timestamp",),
    )
    group.create_array(
        "firecube_timestamp_state",
        data=np.ones(4, dtype=np.uint8),
        chunks=(2,),
        dimension_names=("timestamp",),
    )
    for offset, name in enumerate(names):
        group.create_array(
            name,
            data=np.full((4, 2), offset, dtype=np.float32),
            chunks=(2, 2),
            dimension_names=("timestamp", "x"),
        )


def test_validate_group_iterates_all_time_indexed_arrays(tmp_path: Path) -> None:
    store_path = tmp_path / "product.zarr"
    _seed_group_with_time_arrays(store_path, ("alpha", "beta", "gamma"))
    session = make_local_session(str(store_path))

    report = validate_group_with_fs(session.fs(), session.product.product_uri, "G")

    assert report.is_valid is True
    assert {"G/alpha", "G/beta", "G/gamma"}.issubset(report.arrays_checked)
