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

"""Label contract for ``firecube zarr index show --derived``.

Observed regular-time axes need an extra human-readable clarifier because the
stored values are real observation times, not the nominal grid labels.
Exact/grid axes should keep the existing label unchanged.
"""

from __future__ import annotations

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
    mode: str,
    product_name: str = "test_product",
) -> ResolvedIndexRecord:
    index: dict = {
        "schema_version": "v1",
        "name": product_name,
        "groups": {
            "data": {
                "kind": "regular_time",
                "size": 3,
                "params": {
                    "epoch": "2024-01-01T00:00:00Z",
                    "cadence_s": 600,
                    "mode": mode,
                },
            }
        },
    }
    record = ResolvedIndexRecord(
        recorded_at="2026-08-24T00:00:00Z",
        recorded_by_run_id="test-run",
        identity_hash=compute_resolved_index_identity_hash(index),
        index=index,
    )
    index_dir = cube_dir / ".firecube" / INDEX_DIRNAME
    index_dir.mkdir(parents=True, exist_ok=True)
    (index_dir / INDEX_CURRENT_FILENAME).write_bytes(record.to_json_bytes())
    return record


def _show_args(cube_dir: Path) -> list[str]:
    return [
        "zarr",
        "index",
        "show",
        "--target",
        f"file://{cube_dir}",
        "--product-name",
        "test_product",
        "--derived",
    ]


def test_derived_observed_regular_time_includes_clarifier(tmp_path: Path) -> None:
    cube = tmp_path / "cube"
    cube.mkdir()
    _seed_regular_time_record(cube, mode="floor")

    result = CliRunner().invoke(cli, _show_args(cube))

    assert result.exit_code == 0, result.output
    assert (
        "derived_coordinates['data'] (nominal grid labels — stored values are observed times):"
        in result.stdout
    )


def test_derived_exact_regular_time_keeps_existing_label(tmp_path: Path) -> None:
    cube = tmp_path / "cube"
    cube.mkdir()
    _seed_regular_time_record(cube, mode="exact")

    result = CliRunner().invoke(cli, _show_args(cube))

    assert result.exit_code == 0, result.output
    assert "derived_coordinates['data']:" in result.stdout
    assert "nominal grid labels — stored values are observed times" not in result.stdout
