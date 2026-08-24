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

import datetime
import filecmp
import shutil
from pathlib import Path

import pytest
from click.testing import CliRunner

from firecube.cli.main import cli
from firecube.core.controlplane.types import (
    INDEX_CURRENT_FILENAME,
    INDEX_DIRNAME,
    ResolvedIndexRecord,
    compute_resolved_index_identity_hash,
)

pytestmark = pytest.mark.integration


def _seed_regular_time_record(
    cube_dir: Path,
    *,
    epoch: str = "2024-01-01T00:00:00Z",
    cadence_s: int = 600,
    slot_count: int = 3,
    product_name: str = "test_product",
) -> ResolvedIndexRecord:
    index: dict = {
        "schema_version": "v1",
        "name": product_name,
        "groups": {
            "data": {
                "kind": "regular_time",
                "size": slot_count,
                "params": {
                    "epoch": epoch,
                    "cadence_s": cadence_s,
                    "mode": "exact",
                },
            }
        },
    }
    identity_hash = compute_resolved_index_identity_hash(index)
    record = ResolvedIndexRecord(
        recorded_at="2026-08-24T00:00:00Z",
        recorded_by_run_id="test-run",
        identity_hash=identity_hash,
        index=index,
    )
    index_dir = cube_dir / ".firecube" / INDEX_DIRNAME
    index_dir.mkdir(parents=True, exist_ok=True)
    (index_dir / INDEX_CURRENT_FILENAME).write_bytes(record.to_json_bytes())
    return record


def _seed_irregular_time_record(cube_dir: Path) -> ResolvedIndexRecord:
    index: dict = {
        "schema_version": "v1",
        "name": "test_product",
        "groups": {
            "data": {
                "kind": "irregular_time",
                "size": 2,
                "params": {
                    "coordinate": "time",
                    "values": ["2024-01-01T00:00:00.000000000Z", "2024-01-02T00:00:00.000000000Z"],
                },
            }
        },
    }
    identity_hash = compute_resolved_index_identity_hash(index)
    record = ResolvedIndexRecord(
        recorded_at="2026-08-24T00:00:00Z",
        recorded_by_run_id="test-run",
        identity_hash=identity_hash,
        index=index,
    )
    index_dir = cube_dir / ".firecube" / INDEX_DIRNAME
    index_dir.mkdir(parents=True, exist_ok=True)
    (index_dir / INDEX_CURRENT_FILENAME).write_bytes(record.to_json_bytes())
    return record


def _seed_integer_record(cube_dir: Path) -> ResolvedIndexRecord:
    index: dict = {
        "schema_version": "v1",
        "name": "test_product",
        "groups": {
            "data": {
                "kind": "integer",
                "size": 4,
                "params": {},
            }
        },
    }
    identity_hash = compute_resolved_index_identity_hash(index)
    record = ResolvedIndexRecord(
        recorded_at="2026-08-24T00:00:00Z",
        recorded_by_run_id="test-run",
        identity_hash=identity_hash,
        index=index,
    )
    index_dir = cube_dir / ".firecube" / INDEX_DIRNAME
    index_dir.mkdir(parents=True, exist_ok=True)
    (index_dir / INDEX_CURRENT_FILENAME).write_bytes(record.to_json_bytes())
    return record


def _seed_regular_time_record_missing_epoch(cube_dir: Path) -> ResolvedIndexRecord:
    index: dict = {
        "schema_version": "v1",
        "name": "test_product",
        "groups": {
            "data": {
                "kind": "regular_time",
                "size": 3,
                "params": {
                    "cadence_s": 600,
                    "mode": "exact",
                },
            }
        },
    }
    identity_hash = compute_resolved_index_identity_hash(index)
    record = ResolvedIndexRecord(
        recorded_at="2026-08-24T00:00:00Z",
        recorded_by_run_id="test-run",
        identity_hash=identity_hash,
        index=index,
    )
    index_dir = cube_dir / ".firecube" / INDEX_DIRNAME
    index_dir.mkdir(parents=True, exist_ok=True)
    (index_dir / INDEX_CURRENT_FILENAME).write_bytes(record.to_json_bytes())
    return record


def _show_args(cube_dir: Path, *extras: str) -> list[str]:
    return [
        "zarr",
        "index",
        "show",
        "--target",
        f"file://{cube_dir}",
        "--product-name",
        "test_product",
        *extras,
    ]


def test_show_help_includes_derived_flag() -> None:
    result = CliRunner().invoke(cli, ["zarr", "index", "show", "--help"])

    assert result.exit_code == 0, result.output
    assert "--derived" in result.output


def test_derived_regular_time_produces_correct_coordinates(tmp_path: Path) -> None:
    cube = tmp_path / "cube"
    cube.mkdir()
    epoch = "2024-01-01T00:00:00Z"
    cadence_s = 600
    slot_count = 3
    _seed_regular_time_record(cube, epoch=epoch, cadence_s=cadence_s, slot_count=slot_count)

    result = CliRunner().invoke(cli, _show_args(cube, "--derived"))

    assert result.exit_code == 0, f"stdout={result.stdout!r} stderr={result.stderr!r}"
    epoch_dt = datetime.datetime(2024, 1, 1, tzinfo=datetime.UTC)
    delta = datetime.timedelta(seconds=cadence_s)
    expected = [
        (epoch_dt + i * delta).isoformat().replace("+00:00", "Z") for i in range(slot_count)
    ]
    for coord in expected:
        assert coord in result.stdout, f"Expected {coord!r} in output"
    assert "derived_coordinates" in result.stdout


def test_derived_does_not_persist_anything(tmp_path: Path) -> None:
    cube = tmp_path / "cube"
    cube.mkdir()
    _seed_regular_time_record(cube)

    snapshot_before = tmp_path / "snapshot_before"
    shutil.copytree(cube, snapshot_before)

    result = CliRunner().invoke(cli, _show_args(cube, "--derived"))
    assert result.exit_code == 0, f"stdout={result.stdout!r} stderr={result.stderr!r}"

    comparison = filecmp.dircmp(str(cube), str(snapshot_before))
    assert not comparison.left_only, f"New files after --derived: {comparison.left_only}"
    assert not comparison.right_only, f"Files removed after --derived: {comparison.right_only}"
    assert not comparison.diff_files, f"Files changed after --derived: {comparison.diff_files}"


def test_derived_irregular_time_is_noop_with_note(tmp_path: Path) -> None:
    cube = tmp_path / "cube"
    cube.mkdir()
    _seed_irregular_time_record(cube)

    result = CliRunner().invoke(cli, _show_args(cube, "--derived"))

    assert result.exit_code == 0, f"stdout={result.stdout!r} stderr={result.stderr!r}"
    assert "derived_coordinates" not in result.stdout
    assert "no-op" in result.stderr or "note" in result.stderr


def test_derived_integer_axis_is_noop_with_note(tmp_path: Path) -> None:
    cube = tmp_path / "cube"
    cube.mkdir()
    _seed_integer_record(cube)

    result = CliRunner().invoke(cli, _show_args(cube, "--derived"))

    assert result.exit_code == 0, f"stdout={result.stdout!r} stderr={result.stderr!r}"
    assert "derived_coordinates" not in result.stdout
    assert "no-op" in result.stderr or "note" in result.stderr


def test_derived_missing_epoch_fails_with_actionable_error(tmp_path: Path) -> None:
    cube = tmp_path / "cube"
    cube.mkdir()
    _seed_regular_time_record_missing_epoch(cube)

    result = CliRunner().invoke(cli, _show_args(cube, "--derived"))

    assert result.exit_code != 0, f"Expected failure; stdout={result.stdout!r}"
    assert "epoch" in result.output or "epoch" in (result.stderr or "")


def test_no_derived_flag_output_unchanged(tmp_path: Path) -> None:
    cube = tmp_path / "cube"
    cube.mkdir()
    _seed_regular_time_record(cube)

    result_plain = CliRunner().invoke(cli, _show_args(cube))
    result_no_derived = CliRunner().invoke(cli, _show_args(cube, "--no-derived"))

    assert result_plain.exit_code == 0
    assert result_no_derived.exit_code == 0
    assert result_plain.stdout == result_no_derived.stdout
    assert "derived_coordinates" not in result_plain.stdout
