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

"""No tool may seal, overwrite, or consolidate an engine-managed coordinate.

``firecube_preallocated`` and ``firecube_coord_managed`` are mutually
exclusive lifecycles. Every stamp site must refuse a cross-regime rerun and
must surface the terminal both-markers state instead of extending it:

* consolidate-time-coord over a coord-managed array refuses;
* an exact/grid prefill rerun over a coord-managed array refuses instead of
  overwriting observed values with the nominal grid;
* the irregular materializer refuses to seal a coord-managed array;
* an array already carrying both markers is rejected by every entry point.
"""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import numpy as np
import pytest
import zarr
from click.testing import CliRunner

from firecube.cli.main import cli
from firecube.core.api import ATTR_COORD_MANAGED, ATTR_PREALLOCATED
from firecube.core.errors import SchemaDriftError
from firecube.core.zarr.coord_materialization import materialize_regular_coord_array
from firecube.core.zarr.region_writer import RegionZarrWriter

pytestmark = pytest.mark.integration

_GROUP = "data"
_COORD = "time"
_SLOTS = 12
_EPOCH = np.datetime64("2024-01-01T00:00:00", "ns")
_OBSERVED = _EPOCH + np.arange(_SLOTS) * np.timedelta64(600, "s") + np.timedelta64(2, "s")


def _managed_store(tmp_path: Path, *, extra_attrs: dict[str, Any] | None = None) -> Path:
    """A store whose time coord holds observed values under the managed marker."""
    target = tmp_path / "cube.zarr"
    root = zarr.open_group(store=str(target), mode="w", zarr_format=3)
    group = root.create_group(_GROUP)
    arr = group.create_array(
        _COORD,
        shape=(_SLOTS,),
        chunks=(_SLOTS,),
        dtype="datetime64[ns]",
        fill_value=np.datetime64("NaT", "ns"),
    )
    arr[:] = _OBSERVED
    arr.attrs[ATTR_COORD_MANAGED] = True
    for key, value in (extra_attrs or {}).items():
        arr.attrs[key] = value
    return target


def _coord(target: Path) -> Any:
    root = zarr.open_group(store=str(target), mode="r", zarr_format=3)
    return cast(Any, root[f"{_GROUP}/{_COORD}"])


def _consolidate(target: Path) -> Any:
    return CliRunner().invoke(
        cli,
        [
            "zarr",
            "consolidate-time-coord",
            "--target",
            f"file://{target}",
            "--product-name",
            target.name,
            "--storage-type",
            "local",
            "--storage-driver",
            "fsspec",
        ],
    )


def _axis(*, mode: str) -> SimpleNamespace:
    return SimpleNamespace(
        coordinate=_COORD,
        epoch="2024-01-01T00:00:00Z",
        cadence_s=600,
        mode=mode,
        slot_count=_SLOTS,
    )


def test_consolidate_refuses_coord_managed(tmp_path: Path) -> None:
    target = _managed_store(tmp_path)

    result = _consolidate(target)

    assert result.exit_code != 0
    message = result.output + (str(result.exception) if result.exception is not None else "")
    assert ATTR_COORD_MANAGED in message
    arr = _coord(target)
    assert ATTR_PREALLOCATED not in dict(arr.attrs), "consolidate must not seal a managed array"
    assert np.array_equal(np.asarray(arr[:]), _OBSERVED), "observed values must be untouched"


def test_grid_prefill_rerun_refuses_coord_managed(tmp_path: Path) -> None:
    target = _managed_store(tmp_path)
    writer = RegionZarrWriter(str(target), time_coord_name=_COORD)
    root = writer._open_root()

    with pytest.raises(SchemaDriftError, match=ATTR_COORD_MANAGED):
        materialize_regular_coord_array(
            writer=writer,
            root=root,
            group_name=_GROUP,
            axis=_axis(mode="exact"),
            spec=None,
        )

    arr = _coord(target)
    assert ATTR_PREALLOCATED not in dict(arr.attrs)
    assert np.array_equal(np.asarray(arr[:]), _OBSERVED), (
        "the nominal grid must never overwrite engine-materialized observed values"
    )


def test_both_markers_rejected_by_every_entry_point(tmp_path: Path) -> None:
    target = _managed_store(tmp_path, extra_attrs={ATTR_PREALLOCATED: True})
    writer = RegionZarrWriter(str(target), time_coord_name=_COORD)

    consolidate = _consolidate(target)
    assert consolidate.exit_code != 0
    consolidate_message = consolidate.output + (
        str(consolidate.exception) if consolidate.exception is not None else ""
    )
    assert "mutually exclusive" in consolidate_message

    with pytest.raises(SchemaDriftError, match="mutually exclusive"):
        materialize_regular_coord_array(
            writer=writer,
            root=writer._open_root(),
            group_name=_GROUP,
            axis=_axis(mode="exact"),
            spec=None,
        )

    with pytest.raises(SchemaDriftError, match="mutually exclusive"):
        writer.write_timestamp(_GROUP, ts_index=0, timestamp_val=_OBSERVED[0])
