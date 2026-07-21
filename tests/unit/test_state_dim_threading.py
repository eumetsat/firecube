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

from typing import Any

import pytest
import zarr

from firecube.core.zarr.state import ensure_timestamp_state_array


def _dimension_names(array: Any) -> tuple[str, ...] | None:
    return tuple(array.metadata.dimension_names or ())


@pytest.mark.unit
def test_attach_timestamp_state_honors_custom_dim(tmp_path):
    """Setting dim='time' MUST produce a zarr state array with dimension_names=('time',)."""
    store_path = tmp_path / "custom-dim.zarr"

    ensure_timestamp_state_array(
        store_uri=str(store_path),
        array_path="G1/firecube_timestamp_state",
        length=3,
        chunk_len=2,
        dim="time",
    )

    root = zarr.open_group(store=str(store_path), mode="r")
    state = root["G1/firecube_timestamp_state"]
    assert _dimension_names(state) == ("time",)
    assert _dimension_names(state) != ("timestamp",)


@pytest.mark.unit
def test_attach_timestamp_state_backcompat_default_timestamp(tmp_path):
    """Default 'timestamp' still produces dimension_names=('timestamp',)."""
    store_path = tmp_path / "default-dim.zarr"

    ensure_timestamp_state_array(
        store_uri=str(store_path),
        array_path="G1/firecube_timestamp_state",
        length=3,
        chunk_len=2,
    )

    root = zarr.open_group(store=str(store_path), mode="r")
    state = root["G1/firecube_timestamp_state"]
    assert _dimension_names(state) == ("timestamp",)
