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

import warnings
from types import SimpleNamespace
from typing import Any

import numpy as np
import pytest

from firecube.core.zarr.coord_materialization import (
    coord_to_datetime64,
    materialize_regular_coord_array,
)
from firecube.core.zarr.region_writer import RegionZarrWriter

pytestmark = pytest.mark.unit


def test_regular_axis_epoch_z_suffix_emits_no_numpy_userwarning(
    tmp_path: Any,
) -> None:
    store_path = tmp_path / "cube.zarr"
    writer = RegionZarrWriter(str(store_path))
    root = writer._open_root()

    axis = SimpleNamespace(
        coordinate="time",
        epoch="2024-01-01T00:00:00Z",
        cadence_s=600,
        mode="exact",
        slot_count=1,
    )

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        materialize_regular_coord_array(
            writer=writer,
            root=root,
            group_name="data",
            axis=axis,
            spec=None,
        )

    assert not any(issubclass(item.category, UserWarning) for item in caught)


def testcoord_to_datetime64_accepts_plus_zero_offset() -> None:
    assert coord_to_datetime64("2024-01-01T00:00:00+00:00") == np.datetime64(
        "2024-01-01T00:00:00", "ns"
    )
