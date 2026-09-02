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

"""``consolidate-time-coord`` backfills a missing ``ConsolidatedTimeCoord``
WAL event when the on-array sealing marker is present but the event log is
empty.

Guards against the crash-recovery hole where a prior ``consolidate-time-coord``
stamped ``firecube_consolidated_at`` on the coord array (via
``_rewrite_time_coord_local``) but crashed before ``record_time_coord_consolidation``
wrote the WAL event. The following rerun sees ``state == "already_sealed"`` and
must self-heal the WAL event so ``ResumeGuard._check_time_coord_seal`` can block
further ingest.

Blocker B: the guard MUST require ``firecube_consolidated_at``; a
``firecube_preallocated``-only array is an ordinary preallocated-but-never
consolidated cube (fresh window awaiting ingest), and backfilling there would
spuriously seal a live cube.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, cast

import numpy as np
import pytest
import zarr
from click.testing import CliRunner

from firecube.cli.main import cli
from firecube.core.controlplane import ChunkManager
from firecube.core.product.identity import ProductIdentity
from firecube.core.storage.binding import StorageBinding
from firecube.core.storage.driver_config import StorageDriverConfig
from firecube.core.storage.uri import StorageUri
from firecube.core.zarr._sealing_markers import ATTR_CONSOLIDATED_AT, ATTR_PREALLOCATED

pytestmark = pytest.mark.integration

_EPOCH = np.datetime64("2026-01-01T00:00:00", "ns")
_STEP = np.timedelta64(60, "s").astype("timedelta64[ns]")
_SEALED_AT = "2026-05-17T09:30:00+00:00"


def _values(count: int) -> np.ndarray[Any, Any]:
    return _EPOCH + np.arange(count, dtype=np.int64) * _STEP


def _build_sealed_cube(
    cube: Path,
    *,
    stamp_consolidated_at: bool,
    count: int = 32,
    chunk_size: int = 16,
) -> None:
    """Create a cube whose time coord already carries the sealing markers.

    Simulates the crash-post-marker-stamp / pre-WAL-write scenario: the marker
    is on-array but no ``ConsolidatedTimeCoord`` event lives in ``.firecube/``.
    Setting ``stamp_consolidated_at=False`` reproduces the Blocker B trap: a
    preallocated cube that has never been consolidated.
    """
    root = zarr.open_group(store=str(cube), mode="w", zarr_format=3)
    data = root.create_group("data")
    values = _values(count)
    arr = data.create_array(
        "time",
        shape=values.shape,
        dtype=values.dtype,
        chunks=(chunk_size,),
        dimension_names=("time",),
    )
    arr[...] = values
    arr.attrs[ATTR_PREALLOCATED] = True
    if stamp_consolidated_at:
        arr.attrs[ATTR_CONSOLIDATED_AT] = _SEALED_AT


def _consolidate_args(cube: Path, product_name: str | None = None) -> list[str]:
    return [
        "zarr",
        "consolidate-time-coord",
        "--target",
        f"file://{cube}",
        "--product-name",
        product_name if product_name is not None else cube.name,
        "--storage-type",
        "local",
        "--storage-driver",
        "fsspec",
    ]


def _open_manager(cube: Path, product_name: str | None = None) -> ChunkManager:
    uri = StorageUri.from_local_path(cube)
    identity = ProductIdentity.from_uri(
        uri, "zarr", product_name=product_name if product_name is not None else cube.name
    )
    return ChunkManager(
        binding=StorageBinding(
            identity=identity,
            driver=StorageDriverConfig(driver="fsspec"),
        )
    )


def _list_seal_events(cube: Path, product_name: str | None = None) -> list[Any]:
    name = product_name if product_name is not None else cube.name
    manager = _open_manager(cube, name)
    try:
        return list(manager.list_time_coord_consolidations(product=name))
    finally:
        manager.close()


def _time_array(cube: Path) -> Any:
    root = zarr.open_group(store=str(cube), mode="r", zarr_format=3)
    return cast(Any, root["data/time"])


def test_sealed_marker_without_wal_event_is_backfilled_on_rerun(tmp_path: Path) -> None:
    """already-sealed markers + missing WAL → consolidate writes a WAL event
    whose timestamp matches the stored ``firecube_consolidated_at`` (not
    ``datetime.now()``)."""
    cube = tmp_path / "sealed_no_wal.zarr"
    _build_sealed_cube(cube, stamp_consolidated_at=True)

    assert _list_seal_events(cube) == [], "precondition: WAL must be empty"

    result = CliRunner().invoke(cli, _consolidate_args(cube))
    assert result.exit_code == 0, result.output
    assert "already sealed" in result.output.lower(), result.output
    assert "backfilled consolidated wal event" in result.output.lower(), result.output

    events = _list_seal_events(cube)
    assert len(events) == 1, f"expected one backfilled event, got {events!r}"
    event = events[0]
    assert event.timestamp_iso == _SEALED_AT, (
        f"backfill must preserve original firecube_consolidated_at "
        f"({_SEALED_AT}); got {event.timestamp_iso!r}"
    )
    assert "data" in event.groups, event.groups

    arr = _time_array(cube)
    assert arr.attrs.get(ATTR_CONSOLIDATED_AT) == _SEALED_AT, (
        "backfill must not overwrite the on-array marker"
    )


def test_backfill_is_idempotent_on_repeat_rerun(tmp_path: Path) -> None:
    """Two consecutive reruns backfill exactly once."""
    cube = tmp_path / "sealed_idempotent.zarr"
    _build_sealed_cube(cube, stamp_consolidated_at=True)

    first = CliRunner().invoke(cli, _consolidate_args(cube))
    assert first.exit_code == 0, first.output
    assert "backfilled consolidated wal event" in first.output.lower(), first.output

    events_after_first = _list_seal_events(cube)
    assert len(events_after_first) == 1

    second = CliRunner().invoke(cli, _consolidate_args(cube))
    assert second.exit_code == 0, second.output
    assert "backfilled consolidated wal event" not in second.output.lower(), (
        f"second rerun must not re-backfill; output:\n{second.output}"
    )

    events_after_second = _list_seal_events(cube)
    assert len(events_after_second) == 1, (
        f"WAL must not grow on repeat rerun; got {events_after_second!r}"
    )
    assert events_after_second[0].timestamp_iso == _SEALED_AT


def test_backfilled_seal_blocks_subsequent_ingest(tmp_path: Path) -> None:
    """After backfill, ``ResumeGuard._check_time_coord_seal`` rejects ingest."""
    cube = tmp_path / "sealed_blocks_ingest.zarr"
    _build_sealed_cube(cube, stamp_consolidated_at=True)

    seal = CliRunner().invoke(cli, _consolidate_args(cube))
    assert seal.exit_code == 0, seal.output
    assert "backfilled consolidated wal event" in seal.output.lower(), seal.output

    ingest_args = [
        "ingest",
        "regular_axis_dense_coord",
        "--target",
        f"file://{cube}",
        "--product-name",
        cube.name,
        "--storage-type",
        "local",
        "--storage-driver",
        "fsspec",
        "--write-mode",
        "direct",
    ]
    result = CliRunner().invoke(cli, ingest_args)
    assert result.exit_code != 0, (
        f"ingest against a backfill-sealed cube must fail; got exit=0 output:\n{result.output}"
    )
    combined = f"{result.output}\n{result.exception!s}"
    assert "sealed" in combined.lower(), combined
    assert "further ingest is blocked" in combined.lower(), combined
    assert _SEALED_AT in combined, (
        f"seal error must cite the backfilled timestamp {_SEALED_AT}; got:\n{combined}"
    )


def test_seal_binds_under_explicit_product_name_not_basename(tmp_path: Path) -> None:
    """The seal must bind to the explicit ``--product-name``, not the target basename.

    A product name that differs from the cube's directory name is the exact
    shape the required flag exists for: were any basename inference left in
    the consolidate path, the seal event would be recorded under the wrong
    product and the subsequent ingest (using the explicit name) would slip
    past the seal check.
    """
    cube = tmp_path / "other_name.zarr"
    product = "my_product"
    _build_sealed_cube(cube, stamp_consolidated_at=True)

    seal = CliRunner().invoke(cli, _consolidate_args(cube, product))
    assert seal.exit_code == 0, seal.output

    events = _list_seal_events(cube, product)
    assert len(events) == 1, f"seal event must be recorded under {product!r}; got {events!r}"

    ingest_args = [
        "ingest",
        "regular_axis_dense_coord",
        "--target",
        f"file://{cube}",
        "--product-name",
        product,
        "--storage-type",
        "local",
        "--storage-driver",
        "fsspec",
        "--write-mode",
        "direct",
    ]
    result = CliRunner().invoke(cli, ingest_args)
    assert result.exit_code != 0, (
        f"ingest under the explicit product name must hit the seal; output:\n{result.output}"
    )
    combined = f"{result.output}\n{result.exception!s}"
    assert "sealed" in combined.lower(), combined


def test_preallocated_only_cube_is_not_backfilled(tmp_path: Path) -> None:
    """Blocker B guard: ``firecube_preallocated=True`` alone MUST NOT trigger
    backfill; only ``firecube_consolidated_at`` presence signals a genuine
    prior consolidation. Backfilling a preallocated-but-never-consolidated
    cube would spuriously seal a live cube and block all further ingest."""
    cube = tmp_path / "preallocated_never_consolidated.zarr"
    _build_sealed_cube(cube, stamp_consolidated_at=False)

    result = CliRunner().invoke(cli, _consolidate_args(cube))
    assert result.exit_code == 0, result.output
    assert "already sealed" in result.output.lower(), result.output
    assert "backfilled" not in result.output.lower(), (
        f"preallocated-but-never-consolidated cube must NOT be backfilled; output:\n{result.output}"
    )

    events = _list_seal_events(cube)
    assert events == [], (
        f"Blocker B: no WAL event may be recorded for a cube missing "
        f"firecube_consolidated_at; got {events!r}"
    )

    arr = _time_array(cube)
    assert ATTR_CONSOLIDATED_AT not in arr.attrs, (
        "consolidate must not stamp firecube_consolidated_at during backfill guard"
    )
