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

"""CLI contract tests for ``firecube zarr index verify``.

Exercises the read-only verification path across valid, corrupt, legacy-only,
and missing-record cubes.  Assertions target exit codes and required message
fragments rather than exact whitespace.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest
import zarr
from click.testing import CliRunner

from firecube.cli.main import cli
from firecube.core.controlplane.types import (
    INDEX_CURRENT_FILENAME,
    INDEX_DIRNAME,
    RESOLVED_INDEX_IDENTITY_HASH_ATTR,
    SLOT_INDEX_CURRENT_FILENAME,
    SLOT_INDEX_DIRNAME,
    ResolvedIndexRecord,
    canonical_index_bytes,
)

pytestmark = pytest.mark.integration


def _index_payload(size: int = 3) -> dict[str, object]:
    return {
        "groups": {
            "data": {
                "axes": {"time": {"kind": "integer", "size": size}},
                "items": [
                    {"key": "a", "coordinates": {"time": 0}},
                    {"key": "b", "coordinates": {"time": 1}},
                ],
            }
        },
        "name": "index_verify_test_spec",
    }


def _seed_record(cube_dir: Path, run_id: str = "seed-run") -> ResolvedIndexRecord:
    payload = _index_payload()
    identity_hash = hashlib.sha256(canonical_index_bytes(payload)).hexdigest()
    record = ResolvedIndexRecord(
        recorded_at="2026-08-20T00:00:00+00:00",
        recorded_by_run_id=run_id,
        identity_hash=identity_hash,
        index=payload,
    )
    index_dir = cube_dir / ".firecube" / INDEX_DIRNAME
    index_dir.mkdir(parents=True, exist_ok=True)
    (index_dir / INDEX_CURRENT_FILENAME).write_bytes(record.to_json_bytes())
    return record


def _seed_corrupt_record(cube_dir: Path) -> None:
    payload = _index_payload()
    identity_hash = hashlib.sha256(canonical_index_bytes(payload)).hexdigest()
    tampered = {
        "schema_version": "v1",
        "recorded_at": "2026-08-20T00:00:00+00:00",
        "recorded_by_run_id": "tampered-run",
        # Stored hash intentionally does not match the recomputed canonical bytes.
        "identity_hash": "0" * 64,
        "index": payload,
    }
    assert tampered["identity_hash"] != identity_hash
    index_dir = cube_dir / ".firecube" / INDEX_DIRNAME
    index_dir.mkdir(parents=True, exist_ok=True)
    (index_dir / INDEX_CURRENT_FILENAME).write_bytes(
        json.dumps(tampered, sort_keys=True, separators=(",", ":")).encode("utf-8")
    )


def _seed_legacy_only(cube_dir: Path) -> None:
    legacy_dir = cube_dir / ".firecube" / SLOT_INDEX_DIRNAME
    legacy_dir.mkdir(parents=True, exist_ok=True)
    (legacy_dir / SLOT_INDEX_CURRENT_FILENAME).write_text("{}")


def _verify_args(cube_dir: Path, product_name: str = "test_product", *extras: str) -> list[str]:
    args = [
        "zarr",
        "index",
        "verify",
        "--target",
        f"file://{cube_dir}",
        "--product-name",
        product_name,
    ]
    args.extend(extras)
    return args


def _rebuild_args(cube_dir: Path, plugin: str, product_name: str) -> list[str]:
    return [
        "zarr",
        "index",
        "rebuild",
        "--target",
        f"file://{cube_dir}",
        "--plugin",
        plugin,
        "--product-name",
        product_name,
    ]


def _rebuild_cube(cube_dir: Path, plugin: str, product_name: str) -> ResolvedIndexRecord:
    result = CliRunner().invoke(cli, _rebuild_args(cube_dir, plugin, product_name))
    assert result.exit_code == 0, f"stdout={result.stdout!r} stderr={result.stderr!r}"
    return ResolvedIndexRecord.from_json_bytes(
        (cube_dir / ".firecube" / INDEX_DIRNAME / INDEX_CURRENT_FILENAME).read_bytes()
    )


def _set_resolved_index_attr(cube_dir: Path, value: str) -> None:
    root = zarr.open(str(cube_dir), mode="a")
    root.attrs[RESOLVED_INDEX_IDENTITY_HASH_ATTR] = value


def _delete_resolved_index_attr(cube_dir: Path) -> None:
    root = zarr.open(str(cube_dir), mode="a")
    del root.attrs[RESOLVED_INDEX_IDENTITY_HASH_ATTR]


def test_verify_help_lists_required_flags() -> None:
    result = CliRunner().invoke(cli, ["zarr", "index", "verify", "--help"])

    assert result.exit_code == 0, result.output
    assert "--target" in result.output
    assert "--product-name" in result.output
    assert "--plugin" in result.output


def test_verify_valid_record_exits_zero(tmp_path: Path) -> None:
    cube = tmp_path / "valid_cube"
    cube.mkdir()
    record = _seed_record(cube)

    result = CliRunner().invoke(cli, _verify_args(cube))

    assert result.exit_code == 0, f"stdout={result.stdout!r} stderr={result.stderr!r}"
    assert "VERIFIED" in result.stdout
    # Truncated identity_hash prefix helps operators grep logs.
    assert record.identity_hash[:16] in result.stdout


def test_verify_corrupt_record_exits_one_with_identity_hash_mismatch(tmp_path: Path) -> None:
    cube = tmp_path / "corrupt_cube"
    cube.mkdir()
    _seed_corrupt_record(cube)

    result = CliRunner().invoke(cli, _verify_args(cube))

    assert result.exit_code == 1, f"stdout={result.stdout!r} stderr={result.stderr!r}"
    # ManifestError from ResolvedIndexRecord.from_json_bytes surfaces the mismatch phrase.
    assert "identity-hash mismatch" in result.stderr


def test_verify_legacy_only_cube_exits_one_with_rebuild_guidance(tmp_path: Path) -> None:
    cube = tmp_path / "legacy_only_cube"
    cube.mkdir()
    _seed_legacy_only(cube)

    result = CliRunner().invoke(cli, _verify_args(cube))

    assert result.exit_code == 1, f"stdout={result.stdout!r} stderr={result.stderr!r}"
    assert "firecube zarr index rebuild" in result.stderr


def test_verify_no_record_exits_3(tmp_path: Path) -> None:
    fresh = tmp_path / "fresh_cube"
    fresh.mkdir()

    result = CliRunner().invoke(cli, _verify_args(fresh))

    # Exit 3 distinguishes "no record" from "error" (1) and Click UsageError (2).
    assert result.exit_code == 3, f"stdout={result.stdout!r} stderr={result.stderr!r}"
    assert "No index record" in result.stderr


def test_verify_plugin_match_exits_0(tmp_path: Path) -> None:
    cube = tmp_path / "plugin_match_cube"
    cube.mkdir()
    record = _rebuild_cube(cube, "index_spec_single", "index_spec_single")

    result = CliRunner().invoke(
        cli, _verify_args(cube, "index_spec_single", "--plugin", "index_spec_single")
    )

    assert result.exit_code == 0, f"stdout={result.stdout!r} stderr={result.stderr!r}"
    assert "VERIFIED" in result.stdout
    assert record.identity_hash[:16] in result.stdout


def test_verify_plugin_mismatch_exits_1(tmp_path: Path) -> None:
    cube = tmp_path / "plugin_mismatch_cube"
    cube.mkdir()
    record = _rebuild_cube(cube, "index_spec_single", "index_spec_single")

    result = CliRunner().invoke(
        cli, _verify_args(cube, "index_spec_single", "--plugin", "index_spec_integer_test")
    )

    assert result.exit_code == 1, f"stdout={result.stdout!r} stderr={result.stderr!r}"
    assert "index drift" in result.stderr
    assert record.identity_hash[:8] in result.stderr


def test_verify_attrs_drift_exits_1(tmp_path: Path) -> None:
    cube = tmp_path / "attrs_drift_cube"
    cube.mkdir()
    _rebuild_cube(cube, "index_spec_single", "index_spec_single")
    _set_resolved_index_attr(cube, "f" * 64)

    result = CliRunner().invoke(cli, _verify_args(cube, "index_spec_single"))

    assert result.exit_code == 1, f"stdout={result.stdout!r} stderr={result.stderr!r}"
    assert "attrs drift" in result.stderr
    assert "firecube zarr index rebuild" in result.stderr


def test_verify_attrs_absent_exits_0(tmp_path: Path) -> None:
    cube = tmp_path / "attrs_absent_cube"
    cube.mkdir()
    record = _rebuild_cube(cube, "index_spec_single", "index_spec_single")
    _delete_resolved_index_attr(cube)

    result = CliRunner().invoke(cli, _verify_args(cube, "index_spec_single"))

    assert result.exit_code == 0, f"stdout={result.stdout!r} stderr={result.stderr!r}"
    assert "root attr will be re-mirrored on next" in result.stdout
    assert "not drift" in result.stdout
    assert record.identity_hash[:16] in result.stdout


def test_verify_plugin_uses_lowercase_time_dim_name(tmp_path: Path) -> None:
    cube = tmp_path / "custom_dim_cube"
    cube.mkdir()
    record = _rebuild_cube(cube, "index_spec_custom_dim", "index_spec_custom_dim")

    result = CliRunner().invoke(
        cli,
        _verify_args(cube, "index_spec_custom_dim", "--plugin", "index_spec_custom_dim"),
    )

    assert result.exit_code == 0, f"stdout={result.stdout!r} stderr={result.stderr!r}"
    assert "VERIFIED" in result.stdout
    assert record.identity_hash[:16] in result.stdout


def test_verify_missing_product_name_raises_click_usage_error(tmp_path: Path) -> None:
    cube = tmp_path / "any_cube"
    cube.mkdir()
    # No --product-name; Click's own validator must reject before the command body runs.
    args = ["zarr", "index", "verify", "--target", f"file://{cube}"]

    result = CliRunner().invoke(cli, args)

    assert result.exit_code == 2, f"stdout={result.stdout!r} stderr={result.stderr!r}"
    assert "--product-name" in result.stderr


def test_verify_valid_record_does_not_write_to_disk(tmp_path: Path) -> None:
    cube = tmp_path / "readonly_cube"
    cube.mkdir()
    record = _seed_record(cube)
    current_json = cube / ".firecube" / INDEX_DIRNAME / INDEX_CURRENT_FILENAME
    original_bytes = current_json.read_bytes()
    original_mtime = current_json.stat().st_mtime_ns

    result = CliRunner().invoke(cli, _verify_args(cube))

    assert result.exit_code == 0, f"stdout={result.stdout!r} stderr={result.stderr!r}"
    assert current_json.read_bytes() == original_bytes
    assert current_json.stat().st_mtime_ns == original_mtime
    assert record.identity_hash[:16] in result.stdout
