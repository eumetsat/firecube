# Copyright 2025-2026 EUMETSAT
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
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


def _seed_group(
    store_path: Path,
    *,
    timestamps: np.ndarray,
    state_values: np.ndarray | None,
) -> None:
    root = zarr.open_group(store=str(store_path), mode="w", zarr_format=3)
    group = root.require_group("G")
    group.create_array(
        "timestamp",
        data=timestamps,
        chunks=(max(1, len(timestamps)),),
        dimension_names=("timestamp",),
    )
    if state_values is not None:
        group.create_array(
            "firecube_timestamp_state",
            data=state_values,
            chunks=(max(1, len(state_values)),),
            dimension_names=("timestamp",),
        )
    group.create_array(
        "data",
        data=np.zeros((len(timestamps), 1), dtype=np.float32),
        chunks=(max(1, len(timestamps)), 1),
        dimension_names=("timestamp", "x"),
    )


def _validate(store_path: Path):
    session = make_local_session(str(store_path))
    return validate_group_with_fs(
        session.fs(), session.product.product_uri, "G", time_dim_name="timestamp"
    )


def test_validate_generic_zarr_with_state_array_still_enforced(tmp_path: Path) -> None:
    """Append store with state array and duplicate timestamps remains invalid."""
    store_path = tmp_path / "append-duplicate.zarr"
    _seed_group(
        store_path,
        timestamps=np.array([0, 1, 1], dtype=np.int64),
        state_values=np.ones(3, dtype=np.uint8),
    )

    report = _validate(store_path)

    assert report.is_valid is False
    assert any("duplicate" in issue.lower() for issue in report.validity_issues)
    assert report.info_notes == []


def test_validate_legacy_prestate_store_treated_as_informational(tmp_path: Path) -> None:
    """Legacy pre-state stores without state array are valid with an informational note."""
    store_path = tmp_path / "legacy-prestate.zarr"
    _seed_group(
        store_path,
        timestamps=np.array([10, 20, 30], dtype=np.int64),
        state_values=None,
    )

    report = _validate(store_path)

    assert report.is_valid is True
    assert report.validity_issues == []
    assert report.info_notes == [
        "state array G/firecube_timestamp_state: absent (DirectZarr or legacy pre-state store)"
    ]


def test_validate_state_length_check_when_array_present(tmp_path: Path) -> None:
    """State array present but wrong length remains invalid."""
    store_path = tmp_path / "state-length.zarr"
    _seed_group(
        store_path,
        timestamps=np.array([0, 1, 2], dtype=np.int64),
        state_values=np.ones(2, dtype=np.uint8),
    )

    report = _validate(store_path)

    assert report.is_valid is False
    assert any("length 2 != time coordinate length 3" in issue for issue in report.validity_issues)
    assert report.info_notes == []
