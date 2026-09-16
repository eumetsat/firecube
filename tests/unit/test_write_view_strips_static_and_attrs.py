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

import numpy as np
import pandas as pd
import pytest
import xarray as xr

from firecube.ingestor.runtime.zarr.write import _append_write_view

pytestmark = pytest.mark.unit


def _dataset() -> xr.Dataset:
    timestamps = pd.date_range("2024-01-01", periods=2, freq="h")
    return xr.Dataset(
        {
            "dynamic": (("timestamp", "x"), np.ones((2, 3), dtype=np.float32)),
            "static": (("x",), np.arange(3, dtype=np.float32)),
        },
        coords={"timestamp": timestamps, "x": np.arange(3)},
        attrs={"title": "first"},
    )


def test_append_write_view_contains_only_time_indexed_vars_and_no_attrs() -> None:
    view = _append_write_view(_dataset(), time_dim="timestamp")

    assert set(view.data_vars) == {"dynamic"}
    assert view.attrs == {}


def test_append_write_view_is_idempotent() -> None:
    first = _append_write_view(_dataset(), time_dim="timestamp")
    second = _append_write_view(first, time_dim="timestamp")

    assert set(second.data_vars) == {"dynamic"}
    assert second.attrs == {}
    xr.testing.assert_identical(first, second)
