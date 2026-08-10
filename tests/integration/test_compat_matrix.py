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

"""Compatibility matrix integration tests for time_dim_name behavior.

Each test corresponds to one row of the Existing-Cube Compatibility Matrix
defined in the CF-1.8 compliance contract decisions.

| # | Existing cube dim                  | Plugin-declared dim | Expected action                                    |
|---|------------------------------------|---------------------|----------------------------------------------------|
| 1 | (new cube, no existing)            | ``timestamp``       | write new cube with ``timestamp``                  |
| 2 | (new cube, no existing)            | ``time``            | write new cube with ``time``                       |
| 3 | (new cube, no existing)            | ``foo`` (any other) | write new cube with ``foo``                        |
| 4 | ``timestamp``                      | ``timestamp``       | append (back-compat path)                          |
| 5 | ``timestamp``                      | ``time``            | FAIL with exact migration guidance                 |
| 6 | ``time``                           | ``time``            | append                                             |
| 7 | ``time``                           | ``timestamp``       | FAIL with exact migration guidance                 |
| 8 | both ``time`` AND ``timestamp``    | (any)               | FAIL with ambiguous-state diagnostic               |
"""

from __future__ import annotations

import numpy as np
import pytest
import xarray as xr

from firecube.ingestor.errors import ConfigurationError
from firecube.ingestor.runtime.zarr.existing_cube_check import verify_dim_compatibility
from tests.fixtures.cf_dataset_fixtures import (
    make_cf_compliant_dataset,
    make_legacy_timestamp_dataset,
)


@pytest.mark.integration
def test_row1_new_cube_timestamp_dim_allowed(tmp_path):
    """Row 1: no existing cube + declared='timestamp' → no error (will be written fresh)."""
    target = str(tmp_path / "row1.zarr")
    verify_dim_compatibility(target, "timestamp", group_paths=["."], storage_config=None)


@pytest.mark.integration
def test_row2_new_cube_time_dim_allowed(tmp_path):
    """Row 2: no existing cube + declared='time' → no error."""
    target = str(tmp_path / "row2.zarr")
    verify_dim_compatibility(target, "time", group_paths=["."], storage_config=None)


@pytest.mark.integration
def test_row3_new_cube_arbitrary_dim_allowed(tmp_path):
    """Row 3: no existing cube + declared='foo' (any other) → no error."""
    target = str(tmp_path / "row3.zarr")
    verify_dim_compatibility(target, "foo", group_paths=["."], storage_config=None)


@pytest.mark.integration
def test_row4_existing_timestamp_matches_timestamp(tmp_path):
    """Row 4: existing 'timestamp' cube + declared='timestamp' → append OK (back-compat)."""
    target = str(tmp_path / "row4.zarr")
    make_legacy_timestamp_dataset().to_zarr(target, mode="w", zarr_format=3, consolidated=False)
    verify_dim_compatibility(target, "timestamp", group_paths=["."], storage_config=None)


@pytest.mark.integration
def test_row5_existing_timestamp_mismatch_time_fails_with_guidance(tmp_path):
    """Row 5: existing 'timestamp' cube + declared='time' → FAIL with migration guidance."""
    target = str(tmp_path / "row5.zarr")
    make_legacy_timestamp_dataset().to_zarr(target, mode="w", zarr_format=3, consolidated=False)
    with pytest.raises(ConfigurationError) as exc:
        verify_dim_compatibility(target, "time", group_paths=["."], storage_config=None)
    msg = str(exc.value)
    assert "Refusing to append" in msg
    assert "declared time dimension" in msg
    assert "migrate the existing cube" in msg


@pytest.mark.integration
def test_row6_existing_time_matches_time(tmp_path):
    """Row 6: existing 'time' cube + declared='time' → append OK."""
    target = str(tmp_path / "row6.zarr")
    make_cf_compliant_dataset(time_dim="time").to_zarr(
        target, mode="w", zarr_format=3, consolidated=False
    )
    verify_dim_compatibility(target, "time", group_paths=["."], storage_config=None)


@pytest.mark.integration
def test_row7_existing_time_mismatch_timestamp_fails_with_guidance(tmp_path):
    """Row 7: existing 'time' cube + declared='timestamp' → FAIL with migration guidance."""
    target = str(tmp_path / "row7.zarr")
    make_cf_compliant_dataset(time_dim="time").to_zarr(
        target, mode="w", zarr_format=3, consolidated=False
    )
    with pytest.raises(ConfigurationError) as exc:
        verify_dim_compatibility(target, "timestamp", group_paths=["."], storage_config=None)
    msg = str(exc.value)
    assert "Refusing to append" in msg
    assert "declared time dimension" in msg
    assert "migrate the existing cube" in msg


@pytest.mark.integration
def test_row8_ambiguous_both_dims_fails(tmp_path):
    """Row 8: existing cube with BOTH 'time' AND 'timestamp' dims → FAIL with ambiguous diagnostic."""
    target = str(tmp_path / "row8.zarr")
    ambiguous = xr.Dataset(
        {
            "temperature": (
                ["time", "timestamp", "lat"],
                np.zeros((2, 2, 3), dtype=np.float32),
            )
        },
        coords={"time": [0, 1], "timestamp": [0, 1], "lat": [10, 20, 30]},
    )
    ambiguous.to_zarr(target, mode="w", zarr_format=3, consolidated=False)
    with pytest.raises(ConfigurationError) as exc:
        verify_dim_compatibility(target, "time", group_paths=["."], storage_config=None)
    msg = str(exc.value)
    assert any(token in msg.lower() for token in ("ambiguous", "both", "conflicting"))
