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

import importlib
import json
from collections.abc import Iterator
from pathlib import Path

import pytest
from click.testing import CliRunner

from firecube.cli.main import cli
from firecube.ingestor.registry import loader as _loader

pytestmark = pytest.mark.integration

PLUGIN_NAME = "irregular_axis_safe"
PRODUCT_NAME = "irregular_axis_safe"


@pytest.fixture(autouse=True)
def reset_plugin_registry() -> Iterator[None]:
    original_loaded = _loader._LOADED
    original_registry = dict(_loader.AVAILABLE_INGESTORS)
    _loader._LOADED = False
    _loader.AVAILABLE_INGESTORS.clear()
    importlib.reload(importlib.import_module("irregular_axis_test_plugin"))
    yield
    _loader._LOADED = original_loaded
    _loader.AVAILABLE_INGESTORS.clear()
    _loader.AVAILABLE_INGESTORS.update(original_registry)


def _dry_run_args(target_dir: Path) -> list[str]:
    return [
        "zarr",
        "preallocate",
        PLUGIN_NAME,
        "--target",
        f"file://{target_dir}",
        "--product-name",
        PRODUCT_NAME,
        "--storage-type",
        "local",
        "--storage-driver",
        "fsspec",
        "--write-mode",
        "staged",
        "--dry-run",
    ]


def _real_preallocate_args(target_dir: Path) -> list[str]:
    return [
        "zarr",
        "preallocate",
        PLUGIN_NAME,
        "--target",
        f"file://{target_dir}",
        "--product-name",
        PRODUCT_NAME,
        "--storage-type",
        "local",
        "--storage-driver",
        "fsspec",
        "--write-mode",
        "staged",
    ]


def _index_show_args(target_dir: Path) -> list[str]:
    return [
        "zarr",
        "index",
        "show",
        "--target",
        f"file://{target_dir}",
        "--product-name",
        PRODUCT_NAME,
        "--json",
    ]


def test_dry_run_help_shows_flag() -> None:
    result = CliRunner().invoke(cli, ["zarr", "preallocate", "--help"])
    assert result.exit_code == 0, result.output
    assert "--dry-run" in result.output


def test_dry_run_exits_zero(tmp_path: Path) -> None:
    target_dir = tmp_path / "out.zarr"
    result = CliRunner().invoke(cli, _dry_run_args(target_dir))
    assert result.exit_code == 0, result.output


def test_dry_run_output_is_valid_json_with_required_keys(tmp_path: Path) -> None:
    target_dir = tmp_path / "out.zarr"
    result = CliRunner().invoke(cli, _dry_run_args(target_dir))
    assert result.exit_code == 0, result.output
    parsed = json.loads(result.stdout)
    assert "identity_hash" in parsed
    assert "index" in parsed
    assert len(parsed["identity_hash"]) == 64


def test_dry_run_makes_no_filesystem_mutations(tmp_path: Path) -> None:
    target_dir = tmp_path / "out.zarr"
    before = set(tmp_path.rglob("*"))
    result = CliRunner().invoke(cli, _dry_run_args(target_dir))
    assert result.exit_code == 0, result.output
    after = set(tmp_path.rglob("*"))
    new_files = after - before
    assert new_files == set(), (
        f"dry-run created unexpected filesystem entries: {sorted(str(p) for p in new_files)}"
    )


def test_dry_run_output_is_deterministic(tmp_path: Path) -> None:
    target_dir = tmp_path / "out.zarr"
    r1 = CliRunner().invoke(cli, _dry_run_args(target_dir))
    r2 = CliRunner().invoke(cli, _dry_run_args(target_dir))
    assert r1.exit_code == 0, r1.output
    assert r2.exit_code == 0, r2.output
    assert r1.stdout == r2.stdout
    payload = json.loads(r1.stdout)
    assert payload["recorded_at"] == "dry-run"
    assert len(payload["identity_hash"]) == 64


def test_dry_run_identity_hash_equals_real_preallocate(tmp_path: Path) -> None:
    """Safety contract 7: dry-run identity_hash must equal the real preallocate identity_hash."""
    target_dir = tmp_path / "out.zarr"

    dry_result = CliRunner().invoke(cli, _dry_run_args(target_dir))
    assert dry_result.exit_code == 0, dry_result.output
    dry_hash = json.loads(dry_result.stdout)["identity_hash"]

    real_result = CliRunner().invoke(cli, _real_preallocate_args(target_dir))
    assert real_result.exit_code == 0, real_result.output

    show_result = CliRunner().invoke(cli, _index_show_args(target_dir))
    assert show_result.exit_code == 0, show_result.output
    real_hash = json.loads(show_result.output)["identity_hash"]

    assert dry_hash == real_hash, (
        f"dry-run identity_hash {dry_hash[:16]}... != "
        f"real preallocate identity_hash {real_hash[:16]}..."
    )
