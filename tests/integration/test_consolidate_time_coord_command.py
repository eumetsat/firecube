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

"""End-to-end contracts for ``firecube zarr consolidate-time-coord`` (live mode).

The dry-run behaviours are exercised by
``tests/integration/test_consolidate_time_coord_dry_run.py``; partial-failure
recovery is exercised by ``tests/integration/test_consolidate_partial_recovery.py``.

This module covers the round-trip contracts that only fire once the live
consolidation actually mutates the store and writes a WAL sealing event:

* legacy ``chunks=(1,)`` cube collapses to the dense chunk layout, stamps the
  ``firecube_preallocated`` / ``firecube_consolidated_at`` markers, and writes
  a ``ConsolidatedTimeCoord`` WAL event;
* a second consolidation is idempotent (no chunk changes, no drift error,
  ``already sealed`` message per group);
* an ingest attempt against a sealed cube is refused by ``ResumeGuard``'s
  seal check with the standard sealing message;
* a cube whose persisted resolved-index disagrees with the coord values makes
  live consolidation fail loudly with ``SchemaDriftError`` and leaves the
  legacy chunks untouched;
* a multi-group cube gets every group sealed in a single invocation;
* ``xarray.open_zarr`` still opens the consolidated cube and its metadata
  reflects the collapsed chunk layout (file-count proxy for read cost).
"""

from __future__ import annotations

import hashlib
import json
import math
import subprocess
import sys
from pathlib import Path
from typing import Any

import numpy as np
import pytest
import xarray as xr
import zarr
from click.testing import CliRunner

from firecube.cli.main import cli
from firecube.core.config import StorageConfig
from firecube.core.errors import ConfigurationError, SchemaDriftError
from firecube.core.zarr._sealing_markers import (
    ATTR_CONSOLIDATED_AT,
    ATTR_COORD_MANAGED,
    ATTR_PREALLOCATED,
)
from firecube.core.zarr.coord_consolidation import consolidate_time_coords
from tests.integration.test_cli_index_show import stamp_label

pytestmark = pytest.mark.integration

_LEGACY_SLOTS = 1000
_EPOCH = np.datetime64("2024-01-01T00:00:00", "ns")
_CADENCE_NS = np.timedelta64(600_000_000_000, "ns")
_EXPECTED_CHUNK_LEN = 256
_EXPECTED_CHUNK_FILE_COUNT = math.ceil(_LEGACY_SLOTS / _EXPECTED_CHUNK_LEN)


def _create_legacy_cube(cube: Path, total: int = _LEGACY_SLOTS) -> None:
    """Invoke the shared factory script that writes ``data/time`` at ``chunks=(1,)``."""
    script = Path(__file__).resolve().parent.parent / "scripts" / "create_legacy_cube.py"
    subprocess.run(
        [sys.executable, str(script), str(cube), str(total)],
        check=True,
    )


def _create_multi_group_legacy_cube(
    cube: Path, group_names: tuple[str, ...], total: int = _LEGACY_SLOTS
) -> None:
    """Create a cube with a legacy ``chunks=(1,)`` time coord under each named group."""
    root = zarr.open_group(store=str(cube), mode="w", zarr_format=3)
    values = _EPOCH + np.arange(total, dtype=np.int64) * _CADENCE_NS
    for group_name in group_names:
        group = root.require_group(group_name)
        group.create_array(
            "time",
            data=values,
            chunks=(1,),
            overwrite=True,
            dimension_names=("time",),
        )


def _write_drifted_reference(cube: Path, *, size: int) -> None:
    """Persist a resolved-index record whose params disagree with the cube values.

    Copied verbatim from ``test_consolidate_time_coord_dry_run.py`` so both
    files exercise the same drift trigger; keeping the payload in sync with
    the loader's canonical shape is enforced by ``compute_resolved_index_identity_hash``.
    """
    from firecube.core.controlplane.types import compute_resolved_index_identity_hash

    control_dir = cube / ".firecube" / "index"
    control_dir.mkdir(parents=True, exist_ok=True)
    index_payload: dict[str, object] = {
        "schema_version": "v1",
        "name": "drift-test",
        "groups": {
            "data": {
                "kind": "regular_time",
                "size": size,
                "params": {
                    "epoch": "2024-01-01T01:00:00",
                    "cadence_s": 600,
                    "mode": "floor",
                },
            }
        },
    }
    identity_hash = compute_resolved_index_identity_hash(index_payload, None)
    record = {
        "schema_version": "v1",
        "recorded_at": "2024-01-01T00:00:00Z",
        "recorded_by_run_id": "drift-test",
        "identity_hash": identity_hash,
        "index": index_payload,
    }
    (control_dir / "current.json").write_text(json.dumps(record))


def _consolidate_args(cube: Path) -> list[str]:
    """Return the CLI arguments; the cube records its directory name as its label first."""
    stamp_label(cube, cube.name)
    return [
        "zarr",
        "consolidate-time-coord",
        "--target",
        f"file://{cube}",
        "--storage-type",
        "local",
        "--storage-driver",
        "fsspec",
    ]


def _count_time_chunk_files(cube: Path, group: str = "data") -> int:
    """Count Zarr v3 chunk files under ``{group}/time/c/``."""
    chunk_root = cube / group / "time" / "c"
    if not chunk_root.exists():
        return 0
    return sum(1 for entry in chunk_root.rglob("*") if entry.is_file())


def _md5_of_files(directory: Path) -> dict[str, str]:
    """Hash every file under a directory to detect any byte-level change."""
    result: dict[str, str] = {}
    for entry in sorted(directory.rglob("*")):
        if entry.is_file():
            result[str(entry.relative_to(directory))] = hashlib.md5(entry.read_bytes()).hexdigest()
    return result


def _open_time_array(cube: Path, group: str = "data") -> Any:
    root = zarr.open_group(store=str(cube), mode="r", zarr_format=3)
    return root[f"{group}/time"]


def test_legacy_cube_consolidates_to_dense_layout(tmp_path: Path) -> None:
    """A ``chunks=(1,)`` legacy cube collapses to dense chunks with sealing markers."""
    cube = tmp_path / "cube.zarr"
    _create_legacy_cube(cube)
    assert _count_time_chunk_files(cube) == _LEGACY_SLOTS

    result = CliRunner().invoke(cli, _consolidate_args(cube))
    assert result.exit_code == 0, result.output

    arr = _open_time_array(cube)
    assert tuple(arr.chunks) == (_EXPECTED_CHUNK_LEN,)
    assert arr.attrs.get(ATTR_PREALLOCATED) is True
    assert arr.attrs.get(ATTR_CONSOLIDATED_AT) is not None
    assert _count_time_chunk_files(cube) == _EXPECTED_CHUNK_FILE_COUNT

    expected = _EPOCH + np.arange(_LEGACY_SLOTS, dtype=np.int64) * _CADENCE_NS
    assert np.array_equal(np.asarray(arr[:]), expected)
    # xarray still opens the consolidated cube (chunk files are its read cost).
    with xr.open_zarr(str(cube / "data"), consolidated=False) as ds:
        assert "time" in ds.coords
        assert ds.sizes["time"] == _LEGACY_SLOTS
        assert np.array_equal(ds["time"].values, expected)

    wal_root = cube / ".firecube"
    assert wal_root.exists(), (
        f"consolidate-time-coord must create the control-plane WAL root; missing {wal_root}"
    )


def test_second_consolidation_is_idempotent(tmp_path: Path) -> None:
    """Re-running consolidate on a sealed cube must exit 0 and mutate nothing."""
    cube = tmp_path / "cube.zarr"
    _create_legacy_cube(cube)

    first = CliRunner().invoke(cli, _consolidate_args(cube))
    assert first.exit_code == 0, first.output

    before = _md5_of_files(cube)
    assert before, "sealed cube should contain persisted files to compare"

    second = CliRunner().invoke(cli, _consolidate_args(cube))
    assert second.exit_code == 0, second.output
    assert "already sealed" in second.output.lower(), second.output

    after = _md5_of_files(cube)
    # The idempotent no-op path may still append a WAL bookkeeping event, so
    # the assertion is scoped to the sealed time array (metadata + c/*
    # chunks), not the whole cube directory.
    time_dir_key = "data/time/"
    before_time = {k: v for k, v in before.items() if k.startswith(time_dir_key)}
    after_time = {k: v for k, v in after.items() if k.startswith(time_dir_key)}
    assert before_time == after_time, (
        "second consolidation must not rewrite the sealed time array; "
        f"differing entries: {sorted(set(before_time) ^ set(after_time))}"
    )
    assert _count_time_chunk_files(cube) == _EXPECTED_CHUNK_FILE_COUNT


def test_consolidated_cube_blocks_ingest(tmp_path: Path) -> None:
    """After consolidation, ``firecube ingest`` refuses with the sealing message.

    The seal check lives in ``ResumeGuard._check_time_coord_seal`` and reads the
    ``ConsolidatedTimeCoord`` WAL event written by the consolidate command.
    Matching product identity is required: both invocations must resolve the
    same explicit ``product_name``.
    """
    cube = tmp_path / "cube.zarr"
    _create_legacy_cube(cube)

    seal = CliRunner().invoke(cli, _consolidate_args(cube))
    assert seal.exit_code == 0, seal.output

    ingest_args = [
        "ingest",
        "regular_axis_dense_coord",
        "--target",
        f"file://{cube}",
        "--product-name",
        "cube.zarr",
        "--storage-type",
        "local",
        "--storage-driver",
        "fsspec",
        "--write-mode",
        "direct",
    ]
    result = CliRunner().invoke(cli, ingest_args)
    assert result.exit_code != 0, (
        f"ingest against a sealed cube must fail; got exit=0 output:\n{result.output}"
    )
    combined = f"{result.output}\n{result.exception!s}"
    assert "sealed" in combined.lower(), combined
    assert "further ingest is blocked" in combined.lower(), combined


def test_live_consolidation_on_drifted_cube_raises_schema_drift(tmp_path: Path) -> None:
    """Drift is fatal in live mode and leaves the legacy chunk layout untouched."""

    cube = tmp_path / "cube.zarr"
    _create_legacy_cube(cube)
    _write_drifted_reference(cube, size=_LEGACY_SLOTS)

    args = _consolidate_args(cube)  # records the label, so the snapshot below includes it
    before = _md5_of_files(cube)

    result = CliRunner().invoke(cli, args)

    assert result.exit_code != 0, (
        f"drift-refused consolidation must not exit 0; output:\n{result.output}"
    )
    # Drift is wrapped at the CLI boundary: message rendered, no traceback.
    message = result.output + result.stderr
    assert "Traceback" not in message, message
    assert "DRIFT DETECTED" in message, message
    assert "Refusing to consolidate" in message, message

    after = _md5_of_files(cube)
    assert before == after, (
        "drift-refused consolidation must not rewrite any file; "
        f"differing entries: {sorted(set(before) ^ set(after))}"
    )
    arr = _open_time_array(cube)
    assert tuple(arr.chunks) == (1,)
    assert ATTR_PREALLOCATED not in arr.attrs
    assert _count_time_chunk_files(cube) == _LEGACY_SLOTS


def test_multi_group_cube_consolidates_every_group(tmp_path: Path) -> None:
    """Every ``time`` array under every group is sealed by a single invocation."""
    cube = tmp_path / "cube.zarr"
    groups = ("data_1km", "data_500m")
    _create_multi_group_legacy_cube(cube, groups)

    for group in groups:
        assert _count_time_chunk_files(cube, group=group) == _LEGACY_SLOTS

    result = CliRunner().invoke(cli, _consolidate_args(cube))
    assert result.exit_code == 0, result.output

    for group in groups:
        arr = _open_time_array(cube, group=group)
        assert tuple(arr.chunks) == (_EXPECTED_CHUNK_LEN,), (
            f"group {group!r} time coord not consolidated"
        )
        assert arr.attrs.get(ATTR_PREALLOCATED) is True
        assert arr.attrs.get(ATTR_CONSOLIDATED_AT) is not None
        assert _count_time_chunk_files(cube, group=group) == _EXPECTED_CHUNK_FILE_COUNT


def _local_storage() -> StorageConfig:
    return StorageConfig(storage_type="local", storage_driver="fsspec")


def test_library_consolidation_reports_through_callable_not_stdout(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """A live library run densifies the coordinate, keeps its values and prints nothing itself."""
    cube = tmp_path / "cube.zarr"
    _create_legacy_cube(cube)
    before = np.asarray(_open_time_array(cube)[:])
    lines: list[str] = []

    summary = consolidate_time_coords(
        target=f"file://{cube}",
        product_name=cube.name,
        storage_config=_local_storage(),
        report=lines.append,
    )

    assert summary.consolidated_groups == ("data",)
    arr = _open_time_array(cube)
    assert tuple(arr.chunks) == (_EXPECTED_CHUNK_LEN,)
    assert arr.attrs.get(ATTR_PREALLOCATED) is True
    np.testing.assert_array_equal(np.asarray(arr[:]), before)
    assert lines, "progress must reach the report callable"
    captured = capsys.readouterr()
    assert captured.out == "" and captured.err == ""


def _snapshotted_cube(cube: Path) -> np.ndarray:
    """Write a 4-slot ``data/time`` at ``chunks=(1,)`` under a root consolidated snapshot."""
    root = zarr.open_group(store=str(cube), mode="w", zarr_format=3)
    values = _EPOCH + np.arange(4, dtype=np.int64) * _CADENCE_NS
    root.require_group("data").create_array(
        "time", data=values, chunks=(1,), dimension_names=("time",)
    )
    zarr.consolidate_metadata(str(cube))
    return values


def _consolidate(cube: Path, **kwargs: Any) -> None:
    consolidate_time_coords(
        target=f"file://{cube}", product_name=cube.name, storage_config=_local_storage(), **kwargs
    )


def test_consolidation_keeps_root_consolidated_snapshot_readable(tmp_path: Path) -> None:
    """A default (consolidated) reader reads the coordinate after a rewrite, not a stale snapshot."""
    cube = tmp_path / "cube.zarr"
    values = _snapshotted_cube(cube)

    _consolidate(cube)

    default_root = zarr.open_group(store=str(cube), mode="r")
    assert default_root.metadata.consolidated_metadata is not None
    coord = default_root["data/time"]
    assert isinstance(coord, zarr.Array)
    np.testing.assert_array_equal(np.asarray(coord[:]), values)


def test_consolidation_heals_a_failed_snapshot_refresh_on_rerun(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Two failed snapshot refreshes must not let the third run report healed over a stale snapshot."""
    from firecube.core.zarr import coord_consolidation

    cube = tmp_path / "cube.zarr"
    values = _snapshotted_cube(cube)
    real_refresh = coord_consolidation._refresh_consolidated_metadata
    calls: list[int] = []

    def _flaky_refresh(*args: Any, **kwargs: Any) -> Any:
        calls.append(1)
        if len(calls) <= 2:
            raise OSError("injected refresh failure")
        return real_refresh(*args, **kwargs)

    monkeypatch.setattr(coord_consolidation, "_refresh_consolidated_metadata", _flaky_refresh)
    sinks: list[list[str]] = [[], [], []]
    for sink in sinks[:2]:
        with pytest.raises(OSError, match="injected refresh failure"):
            _consolidate(cube, report=sink.append)
    _consolidate(cube, report=sinks[2].append)

    assert not [ln for sink in sinks[:2] for ln in sink if "backfilled consolidated WAL" in ln]
    assert sum("backfilled consolidated WAL event" in ln for ln in sinks[2]) == 1
    default_root = zarr.open_group(store=str(cube), mode="r")
    coord = default_root["data/time"]
    assert isinstance(coord, zarr.Array)
    assert tuple(coord.chunks) == (4,)
    np.testing.assert_array_equal(np.asarray(coord[:]), values)


def test_failed_backfill_record_does_not_report_the_backfill(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A backfill whose WAL event fails to record must not be reported as backfilled."""
    from firecube.core.controlplane.manager import ChunkManager
    from firecube.core.zarr import coord_consolidation as cc

    def _boom(*args: Any, **kwargs: Any) -> None:
        raise OSError("injected failure")

    cube, lines = tmp_path / "cube.zarr", []
    _snapshotted_cube(cube)
    monkeypatch.setattr(cc, "_refresh_consolidated_metadata", _boom)
    with pytest.raises(OSError, match="injected"):
        _consolidate(cube)
    monkeypatch.undo()
    monkeypatch.setattr(ChunkManager, "record_time_coord_consolidation", _boom)
    with pytest.raises(OSError, match="injected"):
        _consolidate(cube, report=lines.append)
    assert not [ln for ln in lines if "backfilled consolidated WAL" in ln]


def test_library_dry_run_on_drifted_cube_reports_blocked_and_writes_nothing(
    tmp_path: Path,
) -> None:
    """A dry run counts a drifted group as blocked and leaves every store byte alone."""
    cube = tmp_path / "cube.zarr"
    _create_legacy_cube(cube)
    _write_drifted_reference(cube, size=_LEGACY_SLOTS)
    before = _md5_of_files(cube)

    summary = consolidate_time_coords(
        target=f"file://{cube}",
        product_name=cube.name,
        storage_config=_local_storage(),
        dry_run=True,
    )

    assert summary.blocked_by_drift == 1
    assert summary.consolidated_groups == ()
    assert _md5_of_files(cube) == before


def test_library_refuses_s3_before_opening_any_store(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An s3 storage config raises ConfigurationError before the store factory is called."""

    def _boom(*args: object, **kwargs: object) -> object:
        raise AssertionError("store must not be opened for s3")

    monkeypatch.setattr("firecube.core.filesystem.store_factory.create_zarr_store", _boom)

    with pytest.raises(ConfigurationError, match="s3"):
        consolidate_time_coords(
            target="s3://example-bucket/cube.zarr",
            product_name="cube.zarr",
            storage_config=StorageConfig(storage_type="s3", storage_driver="fsspec"),
        )


def test_library_refuses_marker_hidden_by_subgroup_consolidated_snapshot(tmp_path: Path) -> None:
    """A stale consolidated snapshot on a subgroup must not hide a live coord-managed marker."""
    cube = tmp_path / "cube.zarr"
    _create_legacy_cube(cube)
    # The snapshot lives on the subgroup, taken before the marker exists; the root has none.
    zarr.consolidate_metadata(str(cube), path="data")
    writable = zarr.open_group(store=str(cube), mode="r+", zarr_format=3, use_consolidated=False)
    writable["data/time"].attrs[ATTR_COORD_MANAGED] = True
    before = np.asarray(_open_time_array(cube)[:])

    with pytest.raises(SchemaDriftError):
        consolidate_time_coords(
            target=f"file://{cube}",
            product_name=cube.name,
            storage_config=_local_storage(),
        )

    arr = _open_time_array(cube)
    assert arr.attrs.get(ATTR_COORD_MANAGED) is True
    assert ATTR_PREALLOCATED not in arr.attrs
    assert tuple(arr.chunks) == (1,)
    np.testing.assert_array_equal(np.asarray(arr[:]), before)


@pytest.mark.contract
def test_library_two_time_coordinate_candidates_carry_group_and_time_dim_option(
    tmp_path: Path,
) -> None:
    """The refusal exposes the group and the `time_dim` option as context, not as text to parse."""
    cube = tmp_path / "cube.zarr"
    _create_multi_group_legacy_cube(cube, ("data",))
    values = _EPOCH + np.arange(4, dtype=np.int64) * _CADENCE_NS
    zarr.open_group(str(cube), mode="r+").require_group("data").create_array(
        "valid_time", data=values, dimension_names=("valid_time",)
    )

    with pytest.raises(ConfigurationError) as refused:
        consolidate_time_coords(
            target=f"file://{cube}", product_name=cube.name, storage_config=_local_storage()
        )

    assert (refused.value.context.group, refused.value.context.options) == ("/data", ("time_dim",))


def test_consolidate_time_coord_on_calendar_store_is_a_safe_noop(tmp_path: Path) -> None:
    """A preallocated calendar coordinate is found by discovery, reported sealed, and left alone."""
    cube = tmp_path / "cube.zarr"
    group = zarr.open_group(str(cube), mode="w", zarr_format=3).require_group("data")
    fill = int(np.iinfo(np.int64).min)
    coord = group.create_array(
        "time", shape=(4,), chunks=(4,), dtype=np.int64, fill_value=fill,
        dimension_names=("time",),
        attributes={"units": "seconds since 2049-01-01 00:00:00", "calendar": "360_day"},
    )  # fmt: skip
    coord[:] = np.arange(4, dtype=np.int64) * 86400
    coord.attrs[ATTR_PREALLOCATED] = True
    before = _md5_of_files(cube / "data")

    result = CliRunner().invoke(cli, _consolidate_args(cube))

    assert result.exit_code == 0, result.output
    assert "Group /data: already sealed, skipping" in result.output
    assert _md5_of_files(cube / "data") == before


def test_no_time_coord_anywhere_exits_1(tmp_path: Path) -> None:
    """A cube with no time coordinate in any group fails with exit 1 and names --time-dim."""
    cube = tmp_path / "cube.zarr"
    group = zarr.open_group(str(cube), mode="w", zarr_format=3).require_group("data")
    group.create_array("lat", data=np.arange(4, dtype=np.float64), dimension_names=("lat",))

    result = CliRunner().invoke(cli, _consolidate_args(cube))

    assert result.exit_code == 1, result.output + result.stderr
    assert "--time-dim" in result.stderr, result.stderr


def test_consolidate_time_coord_refused_under_exclusive_maintenance(tmp_path: Path) -> None:
    """A held exclusive maintenance claim refuses the command and leaves the store as it was."""
    from firecube.core.controlplane import ChunkManager
    from tests.helpers.storage import make_test_binding

    cube = tmp_path / "cube.zarr"
    _create_legacy_cube(cube, total=10)
    (tmp_path / "ws").mkdir()
    manager = ChunkManager(binding=make_test_binding(tmp_path, product="cube.zarr"), workspace=tmp_path / "ws")  # fmt: skip
    claim = manager.acquire_exclusive_maintenance(product="cube.zarr", run_id="other-maintenance-run")  # fmt: skip

    def snapshot() -> dict[str, str]:
        return {k: v for k, v in _md5_of_files(cube).items() if not k.startswith(".firecube")}

    before = snapshot()
    try:
        result = CliRunner().invoke(cli, _consolidate_args(cube))
    finally:
        claim.release()
        manager.close()

    assert result.exit_code == 1, result.output
    assert "maintenance" in result.output.lower()
    assert snapshot() == before
