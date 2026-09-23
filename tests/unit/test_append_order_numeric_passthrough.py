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

"""A NaN hole in a numeric (non-time) append coordinate is still refused.

Guards the numeric passthrough of ``AppendOrder``: supporting calendar-valued
time coordinates must not stop a float append dimension from treating NaN as a
missing value.
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pytest
import zarr

from firecube.core.filesystem.store_factory import ZarrStoreHandle
from firecube.ingestor.errors import AppendOverwriteRefused
from firecube.ingestor.runtime.zarr.append_order import AppendOrder

pytestmark = pytest.mark.unit


def _store_with_level(tmp_path: Path, values: list[float]) -> ZarrStoreHandle:
    """Write a real float64 append coordinate with no time attrs under ``data/level``."""
    store_dir = tmp_path / "cube.zarr"
    root = zarr.open_group(store=str(store_dir), mode="w", zarr_format=3)
    group = root.create_group("data")
    level = group.create_array(
        "level", shape=(len(values),), dtype="float64", chunks=(len(values),)
    )
    level[:] = np.asarray(values, dtype="float64")
    return ZarrStoreHandle(
        store=str(store_dir), storage_options=None, target_uri=f"file://{store_dir}"
    )


def test_nan_hole_in_float_append_coordinate_is_refused(tmp_path: Path) -> None:
    handle = _store_with_level(tmp_path, [1.0, 2.0, float("nan"), 4.0])

    with pytest.raises(AppendOverwriteRefused) as excinfo:
        AppendOrder()._maximum(handle, "data", "level")

    assert excinfo.value.reason == "nat_existing"


def test_sorted_float_append_coordinate_reports_its_maximum(tmp_path: Path) -> None:
    handle = _store_with_level(tmp_path, [1.0, 2.0, 3.0, 4.0])

    boundary = AppendOrder()._maximum(handle, "data", "level")

    assert boundary is not None
    assert boundary.length == 4
    assert boundary.maximum == 4.0
