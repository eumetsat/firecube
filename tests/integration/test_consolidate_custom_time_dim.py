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

"""Regression coverage: consolidate threads ``time_dim_name``, never a literal.

Before this fix the ``firecube zarr consolidate-time-coord`` command carried
four hardcoded ``"time"`` literals through its state detection, recovery,
and rewrite helpers. Any store written by a plugin whose
``BaseIngestor.time_dim_name`` ClassVar declared a different name — the
framework default is now ``"timestamp"`` — silently no-oped: the command
scanned every group, found no ``"time"`` array, and exited zero without
touching the store.

These tests pin the fix by:

* driving a live consolidation on a cube whose time coord is named
  ``timestamp`` (mirrors the framework default) and confirming the sealing
  markers land on the ``timestamp`` array;
* driving a live consolidation on a cube with a fully custom coord name
  (``obs_time``) resolved via discovery and confirming the same sealing
  path fires;
* verifying an explicit ``--time-dim`` override that names a coord absent
  from the group refuses loudly instead of silently no-oping.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import numpy as np
import pytest
import zarr
from click.testing import CliRunner

from firecube.cli.main import cli
from firecube.core.zarr._sealing_markers import ATTR_CONSOLIDATED_AT, ATTR_PREALLOCATED

pytestmark = pytest.mark.integration

_LEGACY_SLOTS = 100
_EPOCH = np.datetime64("2024-01-01T00:00:00", "ns")
_CADENCE_NS = np.timedelta64(600_000_000_000, "ns")


def _create_legacy_cube_with_coord(cube: Path, coord_name: str, total: int) -> np.ndarray:
    """Create a legacy chunks=(1,) cube where the time coord is named ``coord_name``."""
    root = zarr.open_group(store=str(cube), mode="w", zarr_format=3)
    group = root.require_group("data")
    values = _EPOCH + np.arange(total, dtype=np.int64) * _CADENCE_NS
    group.create_array(
        coord_name,
        data=values,
        chunks=(1,),
        overwrite=True,
        dimension_names=(coord_name,),
    )
    return values


def _consolidate_args(cube: Path, *, time_dim: str | None = None) -> list[str]:
    args = [
        "zarr",
        "consolidate-time-coord",
        "--target",
        f"file://{cube}",
        "--product-name",
        cube.name,
        "--storage-type",
        "local",
        "--storage-driver",
        "fsspec",
    ]
    if time_dim is not None:
        args.extend(["--time-dim", time_dim])
    return args


def _count_chunk_files(cube: Path, coord_name: str, group: str = "data") -> int:
    chunk_root = cube / group / coord_name / "c"
    if not chunk_root.exists():
        return 0
    return sum(1 for entry in chunk_root.rglob("*") if entry.is_file())


def _open_coord_array(cube: Path, coord_name: str, group: str = "data") -> Any:
    root = zarr.open_group(store=str(cube), mode="r", zarr_format=3)
    return root[f"{group}/{coord_name}"]


def test_consolidate_seals_timestamp_named_coord(tmp_path: Path) -> None:
    """A cube written by a default-``time_dim_name`` plugin is now sealed.

    Prior behavior: the command scanned the group, found no ``"time"``
    array, printed ``no time coord, skipping``, and exited zero without
    stamping any sealing markers. This is the silent no-op regression
    for every plugin using the ``"timestamp"`` default from
    ``BaseIngestor.time_dim_name``.
    """
    cube = tmp_path / "cube.zarr"
    expected = _create_legacy_cube_with_coord(cube, "timestamp", _LEGACY_SLOTS)
    assert _count_chunk_files(cube, "timestamp") == _LEGACY_SLOTS

    result = CliRunner().invoke(cli, _consolidate_args(cube))
    assert result.exit_code == 0, result.output
    assert "consolidated /data/timestamp" in result.output, result.output

    arr = _open_coord_array(cube, "timestamp")
    assert arr.attrs.get(ATTR_PREALLOCATED) is True
    assert arr.attrs.get(ATTR_CONSOLIDATED_AT) is not None
    assert _count_chunk_files(cube, "timestamp") < _LEGACY_SLOTS
    assert np.array_equal(np.asarray(arr[:]), expected)


def test_consolidate_seals_custom_time_dim_via_discovery(tmp_path: Path) -> None:
    """Discovery finds a fully custom coord name (``obs_time``) with no override."""
    cube = tmp_path / "cube.zarr"
    expected = _create_legacy_cube_with_coord(cube, "obs_time", _LEGACY_SLOTS)

    result = CliRunner().invoke(cli, _consolidate_args(cube))
    assert result.exit_code == 0, result.output
    assert "consolidated /data/obs_time" in result.output, result.output

    arr = _open_coord_array(cube, "obs_time")
    assert arr.attrs.get(ATTR_PREALLOCATED) is True
    assert arr.attrs.get(ATTR_CONSOLIDATED_AT) is not None
    assert np.array_equal(np.asarray(arr[:]), expected)


def test_consolidate_respects_explicit_time_dim_flag(tmp_path: Path) -> None:
    """An explicit ``--time-dim`` value drives sealing on the named coord."""
    cube = tmp_path / "cube.zarr"
    expected = _create_legacy_cube_with_coord(cube, "timestamp", _LEGACY_SLOTS)

    result = CliRunner().invoke(cli, _consolidate_args(cube, time_dim="timestamp"))
    assert result.exit_code == 0, result.output

    arr = _open_coord_array(cube, "timestamp")
    assert arr.attrs.get(ATTR_PREALLOCATED) is True
    assert np.array_equal(np.asarray(arr[:]), expected)


def test_consolidate_refuses_when_explicit_time_dim_missing(tmp_path: Path) -> None:
    """Explicit ``--time-dim`` mismatch must refuse loudly, not silently no-op.

    Cube has a coord named ``time`` but the operator passes ``--time-dim
    timestamp``: the previous silent-skip masked the exact defect this task
    fixes. The error must name both the expected coord and the present arrays
    so operators can either correct the flag or the plugin declaration.
    """
    cube = tmp_path / "cube.zarr"
    _create_legacy_cube_with_coord(cube, "time", _LEGACY_SLOTS)

    result = CliRunner().invoke(cli, _consolidate_args(cube, time_dim="timestamp"))
    assert result.exit_code != 0, result.output
    combined = f"{result.output}\n{result.exception!s}"
    assert "'timestamp'" in combined, combined
    assert "Refusing to no-op" in combined, combined
    assert "'time'" in combined, combined
    assert "source: explicit" in combined, combined

    arr = _open_coord_array(cube, "time")
    assert tuple(arr.chunks) == (1,)
    assert ATTR_PREALLOCATED not in arr.attrs


def test_consolidate_dry_run_reports_custom_time_dim(tmp_path: Path) -> None:
    """Dry-run must reflect the resolved custom coord name in the plan output."""
    cube = tmp_path / "cube.zarr"
    _create_legacy_cube_with_coord(cube, "timestamp", _LEGACY_SLOTS)

    args = [*_consolidate_args(cube), "--dry-run"]
    result = CliRunner().invoke(cli, args)
    assert result.exit_code == 0, result.output
    assert "/data/timestamp" in result.output, result.output
    assert "Groups eligible for consolidation: 1" in result.output, result.output

    arr = _open_coord_array(cube, "timestamp")
    assert tuple(arr.chunks) == (1,)
    assert ATTR_PREALLOCATED not in arr.attrs
