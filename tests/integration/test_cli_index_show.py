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

"""CLI contract tests for ``firecube zarr index show``.

Exercises the read-only subcommand against fresh, populated, and corrupt
resolved-index records.  Assertions target semantic behaviour (exit codes,
required output fragments, JSON key set) rather than exact whitespace.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest
from click.testing import CliRunner

from firecube.cli.main import cli
from firecube.core.controlplane.types import (
    INDEX_CURRENT_FILENAME,
    INDEX_DIRNAME,
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
        "name": "index_show_test_spec",
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
        # Store a hash that will NOT match the recomputed canonical bytes.
        "identity_hash": "0" * 64,
        "index": payload,
    }
    assert tampered["identity_hash"] != identity_hash
    index_dir = cube_dir / ".firecube" / INDEX_DIRNAME
    index_dir.mkdir(parents=True, exist_ok=True)
    (index_dir / INDEX_CURRENT_FILENAME).write_bytes(
        json.dumps(tampered, sort_keys=True, separators=(",", ":")).encode("utf-8")
    )


def _show_args(cube_dir: Path, product_name: str = "test_product", *extras: str) -> list[str]:
    args = [
        "zarr",
        "index",
        "show",
        "--target",
        f"file://{cube_dir}",
        "--product-name",
        product_name,
    ]
    args.extend(extras)
    return args


def test_show_help_lists_required_flags() -> None:
    result = CliRunner().invoke(cli, ["zarr", "index", "show", "--help"])

    assert result.exit_code == 0, result.output
    assert "--target" in result.output
    assert "--product-name" in result.output
    assert "--json" in result.output


def test_show_fresh_cube_exits_three_with_message(tmp_path: Path) -> None:
    fresh_cube = tmp_path / "fresh_cube"
    fresh_cube.mkdir()

    result = CliRunner().invoke(cli, _show_args(fresh_cube))

    assert result.exit_code == 3, f"stdout={result.stdout!r} stderr={result.stderr!r}"
    assert "No index record found" in result.stderr


def test_show_populated_cube_returns_human_readable_output(tmp_path: Path) -> None:
    cube = tmp_path / "populated_cube"
    cube.mkdir()
    record = _seed_record(cube)

    result = CliRunner().invoke(cli, _show_args(cube))

    assert result.exit_code == 0, f"stdout={result.stdout!r} stderr={result.stderr!r}"
    # Human-readable format must mention the identity_hash label and a group table hint.
    assert "identity_hash" in result.stdout
    assert "kind" in result.stdout
    # Truncated hash prefix is enough for readability; the full hash still exists.
    assert record.identity_hash[:16] in result.stdout


def test_show_populated_cube_json_has_five_keys(tmp_path: Path) -> None:
    cube = tmp_path / "populated_cube_json"
    cube.mkdir()
    record = _seed_record(cube)

    result = CliRunner().invoke(cli, _show_args(cube, "test_product", "--json"))

    assert result.exit_code == 0, f"stdout={result.stdout!r} stderr={result.stderr!r}"
    parsed = json.loads(result.stdout)
    assert set(parsed.keys()) == {
        "schema_version",
        "recorded_at",
        "recorded_by_run_id",
        "identity_hash",
        "index",
    }
    assert parsed["identity_hash"] == record.identity_hash
    assert parsed["schema_version"] == "v1"


def test_show_corrupt_record_exits_one(tmp_path: Path) -> None:
    cube = tmp_path / "corrupt_cube"
    cube.mkdir()
    _seed_corrupt_record(cube)

    result = CliRunner().invoke(cli, _show_args(cube))

    assert result.exit_code == 1, f"stdout={result.stdout!r} stderr={result.stderr!r}"
    # ManifestError message from ResolvedIndexRecord.from_json_bytes surfaces in stderr.
    assert "identity-hash mismatch" in result.stderr


def test_show_missing_product_name_raises_click_usage_error(tmp_path: Path) -> None:
    cube = tmp_path / "any_cube"
    cube.mkdir()
    # Do NOT include --product-name; Click must reject before command body runs.
    args = ["zarr", "index", "show", "--target", f"file://{cube}"]

    result = CliRunner().invoke(cli, args)

    # Click's UsageError uses exit code 2 (distinct from our 1=error and 3=not-found).
    assert result.exit_code == 2, f"stdout={result.stdout!r} stderr={result.stderr!r}"
    assert "--product-name" in result.stderr
