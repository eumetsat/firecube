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

"""``firecube_group_identity_hash`` survives ``consolidate-time-coord``.

The consolidate flow rebuilds the coord array's attrs (pops
``firecube_preallocated``/``firecube_coord_managed`` and re-adds the sealed
markers). All other reserved SHAPE attrs — including the group identity
hash — must pass through unchanged so mixed-spec verification still works
after consolidation.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, cast

import numpy as np
import pytest
import zarr
from click.testing import CliRunner

from firecube.cli.main import cli
from firecube.core.api import FIRECUBE_GROUP_IDENTITY_HASH_ATTR

pytestmark = pytest.mark.integration

_STAMPED_HASH = "a" * 64
_COORD_NAME = "time"
_GROUP_NAME = "grp"
_SLOT_COUNT = 32


def _build_unsealed_time_coord_with_stamp(target_path: Path) -> None:
    target_path.mkdir()
    root = zarr.open_group(store=str(target_path), mode="a", zarr_format=3)
    group = root.create_group(_GROUP_NAME)
    values = np.arange(_SLOT_COUNT, dtype="int64").astype("datetime64[s]").astype("datetime64[ns]")
    arr = group.create_array(
        _COORD_NAME,
        shape=(_SLOT_COUNT,),
        dtype="datetime64[ns]",
        chunks=(1,),
        fill_value=np.array(np.datetime64("NaT", "ns"), dtype="datetime64[ns]")[()],
        dimension_names=(_COORD_NAME,),
        attributes={FIRECUBE_GROUP_IDENTITY_HASH_ATTR: _STAMPED_HASH},
    )
    arr[...] = values


def _consolidate_args(target_path: Path) -> list[str]:
    return [
        "zarr",
        "consolidate-time-coord",
        "--target",
        f"file://{target_path}",
        "--product-name",
        target_path.name,
        "--storage-type",
        "local",
        "--storage-driver",
        "fsspec",
    ]


def _stamped_hash(target_path: Path) -> Any:
    root = zarr.open_group(store=str(target_path), mode="r", zarr_format=3)
    coord = cast(Any, root[f"{_GROUP_NAME}/{_COORD_NAME}"])
    return coord.attrs.get(FIRECUBE_GROUP_IDENTITY_HASH_ATTR)


def test_group_identity_hash_survives_consolidate(tmp_path: Path) -> None:
    target_path = tmp_path / "cube.zarr"
    _build_unsealed_time_coord_with_stamp(target_path)
    assert _stamped_hash(target_path) == _STAMPED_HASH

    result = CliRunner().invoke(cli, _consolidate_args(target_path))
    assert result.exit_code == 0, result.output

    assert _stamped_hash(target_path) == _STAMPED_HASH


def test_group_identity_hash_survives_consolidate_dry_run(tmp_path: Path) -> None:
    target_path = tmp_path / "cube.zarr"
    _build_unsealed_time_coord_with_stamp(target_path)

    result = CliRunner().invoke(cli, [*_consolidate_args(target_path), "--dry-run"])
    assert result.exit_code == 0, result.output

    assert _stamped_hash(target_path) == _STAMPED_HASH
