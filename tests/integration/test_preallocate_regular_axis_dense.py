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

"""Contracts for dense ``RegularTimeAxis`` coord materialization in ``preallocate``.

``firecube zarr preallocate`` materializes the coordinate array for
``RegularTimeAxis`` groups densely at ``{group}/{axis.coordinate}``. Prefill
and sealing are ``axis.mode``-aware:

* ``mode="exact"`` -- the nominal grid IS the coordinate. Values are prefilled
  and ``firecube_preallocated=True`` is stamped, so ingest timestamp writes are
  verify-only no-ops (race-free at any pod parallelism).
* ``mode="floor"`` -- stored values are real observation times only knowable at
  ingest. The array is created at the dense chunk shape but left NaT and
  unsealed; ingest writes the values. Sealing a floor axis would reject every
  off-grid sensing time with ``SchemaDriftError`` (the MTG FCI blocker).

Two spec shapes exist for the exact-mode prefill:

* Plugin declares a ``(time,)`` ``ZarrArraySpec`` with ``chunks=None`` -->
  the spec loop resolves dense chunks via ``resolve_coord_chunks`` BEFORE
  creating the shell, then the materializer fills the values.
* Plugin declares no ``(time,)`` coord spec --> the materializer creates the
  coord array from scratch with ``resolve_coord_chunks(None, slot_count)``.

Both paths must produce dense chunks (``ceil(slot_count / 256)`` files) and
identical stamped values. ``--dry-run`` must not touch the store on either
path.

Fixtures come from ``regular_axis_test_plugin`` (installed via the fixture
install lines in ``AGENTS.md``). Tests exercise the real CLI wiring -- no mocks.
"""

from __future__ import annotations

import datetime as dt
import importlib
import math
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any, ClassVar, Literal, cast

import irregular_axis_test_plugin as _irregular_plugin_module
import numpy as np
import pytest
import regular_axis_test_plugin as _regular_plugin_module
import zarr
from click.testing import CliRunner

from firecube.cli.main import cli
from firecube.core.api import IndexSpec, ItemInfo, RegularTimeAxis
from firecube.core.errors import SchemaDriftError
from firecube.core.zarr._sealing_markers import ATTR_COORD_MANAGED, ATTR_PREALLOCATED
from firecube.core.zarr.region_writer import RegionZarrWriter
from firecube.ingestor.api import (
    DirectZarrIngestor,
    PipelineBatch,
    PluginContext,
    WriteIntent,
    ZarrArraySpec,
    ZarrGroupSpec,
)
from firecube.ingestor.registry import loader as _loader

pytestmark = pytest.mark.integration

_EPOCH = np.datetime64("2024-01-01T00:00:00", "ns")
_CADENCE_S = 600
_SLOT_COUNT = 1000
_COORD_NAME = "time"
_GROUP = "data"
_EXPECTED_CHUNK_LEN = 256
_EXPECTED_CHUNK_FILE_COUNT = math.ceil(_SLOT_COUNT / _EXPECTED_CHUNK_LEN)
_EXPECTED_VALUES = _EPOCH + np.arange(_SLOT_COUNT, dtype=np.int64) * np.timedelta64(
    _CADENCE_S, "s"
).astype("timedelta64[ns]")


@pytest.fixture(autouse=True)
def reset_plugin_registry() -> Iterator[None]:
    original_loaded = _loader._LOADED
    original_registry = dict(_loader.AVAILABLE_INGESTORS)
    _loader._LOADED = False
    _loader.AVAILABLE_INGESTORS.clear()
    importlib.reload(_regular_plugin_module)
    importlib.reload(_irregular_plugin_module)
    yield
    _loader._LOADED = original_loaded
    _loader.AVAILABLE_INGESTORS.clear()
    _loader.AVAILABLE_INGESTORS.update(original_registry)


def _preallocate_args(
    plugin: str,
    product: str,
    target_path: Path,
    *,
    dry_run: bool = False,
) -> list[str]:
    args = [
        "zarr",
        "preallocate",
        plugin,
        "--target",
        f"file://{target_path}",
        "--product-name",
        product,
        "--storage-type",
        "local",
        "--storage-driver",
        "fsspec",
        "--write-mode",
        "direct",
        "--option",
        "no_progress=true",
    ]
    if dry_run:
        args.append("--dry-run")
    return args


def _root(target_path: Path) -> Any:
    return zarr.open_group(store=str(target_path), mode="r", zarr_format=3)


def _count_chunk_files(coord_dir: Path) -> int:
    """Count Zarr v3 chunk files under a coord array directory (``c/`` subtree)."""
    chunk_root = coord_dir / "c"
    if not chunk_root.exists():
        return 0
    return sum(1 for p in chunk_root.rglob("*") if p.is_file())


def test_preallocate_with_coord_spec_produces_dense_chunks(tmp_path: Path) -> None:
    target_path = tmp_path / "cube.zarr"

    result = CliRunner().invoke(
        cli,
        _preallocate_args(
            "regular_axis_dense_coord",
            "regular_axis_dense_coord",
            target_path,
        ),
    )

    assert result.exit_code == 0, result.output
    coord = cast(Any, _root(target_path)[f"{_GROUP}/{_COORD_NAME}"])
    assert coord.shape == (_SLOT_COUNT,)
    assert tuple(coord.chunks) == (_EXPECTED_CHUNK_LEN,)
    assert coord.attrs[ATTR_PREALLOCATED] is True

    chunk_file_count = _count_chunk_files(target_path / _GROUP / _COORD_NAME)
    assert chunk_file_count == _EXPECTED_CHUNK_FILE_COUNT
    assert chunk_file_count < _SLOT_COUNT

    values = np.asarray(coord[:])
    assert np.array_equal(values, _EXPECTED_VALUES)


def test_preallocate_no_coord_spec_falls_back_to_dense_defaults(tmp_path: Path) -> None:
    target_path = tmp_path / "cube.zarr"

    result = CliRunner().invoke(
        cli,
        _preallocate_args(
            "regular_axis_no_coord_spec",
            "regular_axis_no_coord_spec",
            target_path,
        ),
    )

    assert result.exit_code == 0, result.output
    coord = cast(Any, _root(target_path)[f"{_GROUP}/{_COORD_NAME}"])
    assert coord.shape == (_SLOT_COUNT,)
    assert tuple(coord.chunks) == (_EXPECTED_CHUNK_LEN,)
    assert coord.attrs[ATTR_PREALLOCATED] is True

    chunk_file_count = _count_chunk_files(target_path / _GROUP / _COORD_NAME)
    assert chunk_file_count == _EXPECTED_CHUNK_FILE_COUNT

    values = np.asarray(coord[:])
    assert np.array_equal(values, _EXPECTED_VALUES)


def test_preallocate_dense_dry_run_performs_no_mutation(tmp_path: Path) -> None:
    target_path = tmp_path / "cube.zarr"

    result = CliRunner().invoke(
        cli,
        _preallocate_args(
            "regular_axis_dense_coord",
            "regular_axis_dense_coord",
            target_path,
            dry_run=True,
        ),
    )

    assert result.exit_code == 0, result.output
    assert not target_path.exists(), (
        f"dry-run must not create the target store; found {list(target_path.iterdir())}"
    )


def test_preallocate_is_idempotent_on_dense_coord_array(tmp_path: Path) -> None:
    target_path = tmp_path / "cube.zarr"
    runner = CliRunner()

    first = runner.invoke(
        cli,
        _preallocate_args(
            "regular_axis_dense_coord",
            "regular_axis_dense_coord",
            target_path,
        ),
    )
    second = runner.invoke(
        cli,
        _preallocate_args(
            "regular_axis_dense_coord",
            "regular_axis_dense_coord",
            target_path,
        ),
    )

    assert first.exit_code == 0, first.output
    assert second.exit_code == 0, second.output
    coord = cast(Any, _root(target_path)[f"{_GROUP}/{_COORD_NAME}"])
    assert tuple(coord.chunks) == (_EXPECTED_CHUNK_LEN,)
    assert coord.attrs[ATTR_PREALLOCATED] is True
    values = np.asarray(coord[:])
    assert np.array_equal(values, _EXPECTED_VALUES)


def _register_regular_plugin(
    name: str,
    slot_count: int,
    *,
    mode: Literal["exact", "floor"] = "exact",
    include_coord_spec: bool = True,
) -> None:
    _plugin_name = name
    _slots = slot_count

    class _DynamicRegularAxisIngestor(DirectZarrIngestor):
        PRODUCT_NAME: ClassVar[str] = _plugin_name
        time_dim_name: ClassVar[str] = _COORD_NAME

        def index_spec(self, ctx: PluginContext) -> IndexSpec:
            return IndexSpec(
                name=f"{_plugin_name}_v1",
                groups={
                    _GROUP: RegularTimeAxis(
                        coordinate=_COORD_NAME,
                        epoch="2024-01-01T00:00:00Z",
                        cadence_s=_CADENCE_S,
                        mode=mode,
                        slot_count=_slots,
                    ),
                },
            )

        def inspect_item(self, item: Any, ctx: PluginContext) -> ItemInfo | None:
            if not isinstance(item, (str, dt.datetime)):
                return None
            return ItemInfo(coordinate=item)

        def zarr_schema(self, ctx: PluginContext) -> list[ZarrGroupSpec]:
            arrays: list[ZarrArraySpec] = []
            if include_coord_spec:
                arrays.append(
                    ZarrArraySpec(
                        name=_COORD_NAME,
                        shape=(_slots,),
                        dtype="datetime64[ns]",
                        chunks=None,
                        fill_value=np.datetime64("NaT", "ns"),
                        expected_time_count=_slots,
                        time_indexed=True,
                        dimension_names=(_COORD_NAME,),
                    )
                )
            return [
                ZarrGroupSpec(
                    group=_GROUP,
                    arrays=arrays,
                )
            ]

        def build_write_intents(
            self, batch: PipelineBatch, ctx: PluginContext
        ) -> list[WriteIntent]:
            return []

    _DynamicRegularAxisIngestor.__name__ = f"DynamicRegularAxisIngestor_T{slot_count}"
    _DynamicRegularAxisIngestor.name = _plugin_name  # pyright: ignore[reportAttributeAccessIssue]
    _loader.AVAILABLE_INGESTORS[_plugin_name] = _DynamicRegularAxisIngestor


def _snapshot_time_chunks(target_path: Path) -> dict[str, bytes]:
    chunk_root = target_path / _GROUP / _COORD_NAME / "c"
    if not chunk_root.exists():
        return {}
    return {
        p.relative_to(chunk_root).as_posix(): p.read_bytes()
        for p in chunk_root.rglob("*")
        if p.is_file()
    }


_BOUNDARY_CASES: list[tuple[int, int]] = [
    (1, 1),
    (256, 1),
    (257, 2),
    (4320, 17),
]


@pytest.mark.parametrize("slot_count,expected_chunks", _BOUNDARY_CASES)
def test_preallocate_boundary_sizes_produce_ceil_chunks(
    tmp_path: Path,
    slot_count: int,
    expected_chunks: int,
) -> None:
    plugin_name = f"regular_axis_boundary_t{slot_count}"
    _register_regular_plugin(plugin_name, slot_count)
    target_path = tmp_path / "cube.zarr"

    result = CliRunner().invoke(
        cli,
        _preallocate_args(plugin_name, plugin_name, target_path),
    )

    assert result.exit_code == 0, result.output
    coord = cast(Any, _root(target_path)[f"{_GROUP}/{_COORD_NAME}"])
    assert coord.shape == (slot_count,)
    expected_chunk_shape = (min(_EXPECTED_CHUNK_LEN, slot_count),)
    assert tuple(coord.chunks) == expected_chunk_shape
    assert coord.attrs[ATTR_PREALLOCATED] is True

    chunk_file_count = _count_chunk_files(target_path / _GROUP / _COORD_NAME)
    assert chunk_file_count == expected_chunks, (
        f"expected {expected_chunks} chunk files for slot_count={slot_count}; "
        f"got {chunk_file_count}"
    )
    assert chunk_file_count == math.ceil(slot_count / expected_chunk_shape[0])


def _preallocate_dense_cube(tmp_path: Path) -> Path:
    target_path = tmp_path / "cube.zarr"
    result = CliRunner().invoke(
        cli,
        _preallocate_args(
            "regular_axis_dense_coord",
            "regular_axis_dense_coord",
            target_path,
        ),
    )
    assert result.exit_code == 0, result.output
    return target_path


def test_parallel_pods_produce_zero_time_chunk_writes(tmp_path: Path) -> None:
    target_path = _preallocate_dense_cube(tmp_path)
    before = _snapshot_time_chunks(target_path)
    assert before, "preallocate should have produced at least one time chunk file"

    n_pods = 8
    slots_per_pod = _SLOT_COUNT // n_pods
    ranges = [
        range(i * slots_per_pod, (i + 1) * slots_per_pod if i < n_pods - 1 else _SLOT_COUNT)
        for i in range(n_pods)
    ]
    assert sum(len(r) for r in ranges) == _SLOT_COUNT
    assert set().union(*ranges) == set(range(_SLOT_COUNT))

    def _pod_write(slot_range: range) -> None:
        pod_writer = RegionZarrWriter(
            str(target_path),
            coord_names=frozenset({_COORD_NAME}),
            time_coord_name=_COORD_NAME,
        )
        for slot in slot_range:
            pod_writer.write_timestamp(_GROUP, slot, _EXPECTED_VALUES[slot])

    with ThreadPoolExecutor(max_workers=n_pods) as pool:
        futures = [pool.submit(_pod_write, r) for r in ranges]
        for f in futures:
            f.result()

    after = _snapshot_time_chunks(target_path)
    assert after == before, (
        "marker-aware write_timestamp must be a no-op on matching values; "
        f"chunk files changed: added={set(after) - set(before)!r} "
        f"removed={set(before) - set(after)!r} "
        f"modified={ {k for k in before & after.keys() if before[k] != after[k]}!r}"
    )

    coord = cast(Any, _root(target_path)[f"{_GROUP}/{_COORD_NAME}"])
    assert np.array_equal(np.asarray(coord[:]), _EXPECTED_VALUES)


def test_drift_on_one_pod_isolated_from_correct_pods(tmp_path: Path) -> None:
    target_path = _preallocate_dense_cube(tmp_path)
    before = _snapshot_time_chunks(target_path)

    writer_a = RegionZarrWriter(
        str(target_path), coord_names=frozenset({_COORD_NAME}), time_coord_name=_COORD_NAME
    )
    writer_b = RegionZarrWriter(
        str(target_path), coord_names=frozenset({_COORD_NAME}), time_coord_name=_COORD_NAME
    )
    writer_c = RegionZarrWriter(
        str(target_path), coord_names=frozenset({_COORD_NAME}), time_coord_name=_COORD_NAME
    )

    writer_a.write_timestamp(_GROUP, 10, _EXPECTED_VALUES[10])

    wrong_value = _EXPECTED_VALUES[10] + np.timedelta64(1, "s")
    with pytest.raises(SchemaDriftError, match=r"slot 10"):
        writer_b.write_timestamp(_GROUP, 10, wrong_value)

    writer_c.write_timestamp(_GROUP, 20, _EXPECTED_VALUES[20])

    coord = cast(Any, _root(target_path)[f"{_GROUP}/{_COORD_NAME}"])
    values = np.asarray(coord[:])
    assert values[10] == _EXPECTED_VALUES[10], "slot 10 must retain preallocated value"
    assert values[20] == _EXPECTED_VALUES[20], "slot 20 must retain preallocated value"
    assert np.array_equal(values, _EXPECTED_VALUES)

    after = _snapshot_time_chunks(target_path)
    assert after == before, (
        "no chunk files may be added, removed, or modified by marker-aware writes; "
        f"delta added={set(after) - set(before)!r} removed={set(before) - set(after)!r} "
        f"modified={ {k for k in before & after.keys() if before[k] != after[k]}!r}"
    )


def _preallocate_floor_cube(tmp_path: Path) -> Path:
    plugin_name = "regular_axis_floor_no_coord_spec"
    _register_regular_plugin(
        plugin_name,
        _SLOT_COUNT,
        mode="floor",
        include_coord_spec=False,
    )
    target_path = tmp_path / "floor_cube.zarr"
    result = CliRunner().invoke(
        cli,
        _preallocate_args(
            plugin_name,
            plugin_name,
            target_path,
        ),
    )
    assert result.exit_code == 0, result.output
    return target_path


def test_preallocate_floor_mode_creates_coord_managed_nat_coord(tmp_path: Path) -> None:
    """A ``mode="floor"`` axis gets dense chunks, managed marker, and no prefill."""
    target_path = _preallocate_floor_cube(tmp_path)

    coord = cast(Any, _root(target_path)[f"{_GROUP}/{_COORD_NAME}"])
    assert coord.shape == (_SLOT_COUNT,)
    assert tuple(coord.chunks) == (_EXPECTED_CHUNK_LEN,)
    assert coord.attrs[ATTR_COORD_MANAGED] is True
    assert ATTR_PREALLOCATED not in coord.attrs, (
        "floor-mode coord must not get the exact-grid preallocated marker"
    )
    assert bool(np.all(np.isnat(np.asarray(coord[:])))), (
        "floor-mode coord must not be prefilled with the nominal grid"
    )


def test_floor_mode_ingest_writes_off_grid_timestamps(tmp_path: Path) -> None:
    """Regression for the MTG FCI blocker: off-grid sensing times must ingest.

    FCI coverage starts are offset from the nominal cycle boundary (observed
    ``+2s`` and ``+7s`` in real IDPFI archives). Under ``mode="floor"`` the
    coordinate must accept and store those real values instead of raising
    ``SchemaDriftError`` against a sealed nominal grid.
    """
    target_path = _preallocate_floor_cube(tmp_path)

    off_grid = _EXPECTED_VALUES + np.timedelta64(2, "s").astype("timedelta64[ns]")
    coord_rw = cast(
        Any,
        zarr.open_group(store=str(target_path), mode="a", zarr_format=3)[f"{_GROUP}/{_COORD_NAME}"],
    )
    for slot in (0, 1, 500, _SLOT_COUNT - 1):
        coord_rw[slot] = off_grid[slot]
    writer = RegionZarrWriter(
        str(target_path), coord_names=frozenset({_COORD_NAME}), time_coord_name=_COORD_NAME
    )
    for slot in (0, 1, 500, _SLOT_COUNT - 1):
        writer.write_timestamp(_GROUP, slot, off_grid[slot])

    coord = cast(Any, _root(target_path)[f"{_GROUP}/{_COORD_NAME}"])
    assert tuple(coord.chunks) == (_EXPECTED_CHUNK_LEN,), (
        "ingest writes must not degrade the dense chunk layout"
    )
    values = np.asarray(coord[:])
    for slot in (0, 1, 500, _SLOT_COUNT - 1):
        assert values[slot] == off_grid[slot], f"slot {slot} must hold the real sensing time"
    untouched = np.delete(values, [0, 1, 500, _SLOT_COUNT - 1])
    assert bool(np.all(np.isnat(untouched))), "un-ingested slots must stay NaT"


def test_floor_mode_repreallocate_preserves_ingested_values(tmp_path: Path) -> None:
    """Resume safety: re-running preallocate must not clobber ingested times."""
    target_path = _preallocate_floor_cube(tmp_path)

    off_grid_value = _EXPECTED_VALUES[10] + np.timedelta64(2, "s").astype("timedelta64[ns]")
    coord_rw = cast(
        Any,
        zarr.open_group(store=str(target_path), mode="a", zarr_format=3)[f"{_GROUP}/{_COORD_NAME}"],
    )
    coord_rw[10] = off_grid_value

    rerun = CliRunner().invoke(
        cli,
        _preallocate_args(
            "regular_axis_floor_no_coord_spec",
            "regular_axis_floor_no_coord_spec",
            target_path,
        ),
    )
    assert rerun.exit_code == 0, rerun.output

    coord = cast(Any, _root(target_path)[f"{_GROUP}/{_COORD_NAME}"])
    assert coord.attrs[ATTR_COORD_MANAGED] is True
    assert ATTR_PREALLOCATED not in coord.attrs
    assert np.asarray(coord[:])[10] == off_grid_value, (
        "re-preallocate must not overwrite ingested sensing times with NaT or the grid"
    )


_AUTO_IRREGULAR_SLOT_COUNT = 5
_AUTO_IRREGULAR_BASE = np.datetime64("2026-01-01T00:00:00", "ns")
_AUTO_IRREGULAR_STEP = np.timedelta64(600, "s").astype("timedelta64[ns]")
_AUTO_IRREGULAR_EXPECTED = np.asarray(
    [_AUTO_IRREGULAR_BASE + i * _AUTO_IRREGULAR_STEP for i in range(_AUTO_IRREGULAR_SLOT_COUNT)],
    dtype="datetime64[ns]",
)


def test_auto_irregular_axis_preallocate_stamps_marker_and_allows_matching_writes(
    tmp_path: Path,
) -> None:
    target_path = tmp_path / "auto_irregular.zarr"
    result = CliRunner().invoke(
        cli,
        [
            "zarr",
            "preallocate",
            "irregular_axis_safe",
            "--target",
            f"file://{target_path}",
            "--product-name",
            "irregular_axis_safe",
            "--storage-type",
            "local",
            "--storage-driver",
            "fsspec",
            "--write-mode",
            "direct",
            "--option",
            "no_progress=true",
        ],
    )
    assert result.exit_code == 0, result.output

    coord = cast(Any, _root(target_path)["data/timestamp"])
    assert coord.shape == (_AUTO_IRREGULAR_SLOT_COUNT,)
    assert coord.dtype == np.dtype("datetime64[ns]")
    assert np.array_equal(np.asarray(coord[:]), _AUTO_IRREGULAR_EXPECTED)
    assert coord.attrs[ATTR_PREALLOCATED] is True, (
        "preallocate must stamp ATTR_PREALLOCATED on AUTO IrregularTimeAxis coord arrays"
    )

    def _snapshot_all(root: Path) -> dict[str, bytes]:
        return {
            p.relative_to(root).as_posix(): p.read_bytes() for p in root.rglob("*") if p.is_file()
        }

    before = _snapshot_all(target_path)
    writer = RegionZarrWriter(str(target_path))
    writer.write_timestamp("data", 0, _AUTO_IRREGULAR_EXPECTED[0])
    after = _snapshot_all(target_path)
    assert after == before, (
        "marker-aware write_timestamp on AUTO irregular preallocated coord must "
        f"not mutate the store; delta added={set(after) - set(before)!r} "
        f"removed={set(before) - set(after)!r} "
        f"modified={ {k for k in before & after.keys() if before[k] != after[k]}!r}"
    )
