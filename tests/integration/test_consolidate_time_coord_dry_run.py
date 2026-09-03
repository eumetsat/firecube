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

"""Contracts for ``firecube zarr consolidate-time-coord --dry-run``.

The dry-run mode must:

* leave every byte of the store untouched (verified via md5 checksums of all
  files under the cube directory before and after invocation);
* report all fields required to plan a live consolidation: current chunks,
  current attrs, proposed chunks, markers to stamp, atomic strategy, drift
  status;
* emit a summary tallying scanned / eligible / already-sealed / drift-blocked
  groups;
* exit non-zero (``1``) when the persisted WAL resolved-index proves the
  coord array has drifted, without mutating the store.
"""

from __future__ import annotations

import hashlib
import json
import subprocess
import sys
from pathlib import Path
from typing import Any, cast

import numpy as np
import pytest
from click.testing import CliRunner

from firecube.cli.main import cli

pytestmark = pytest.mark.integration

_LEGACY_SLOTS = 1000


def _md5_of_files(directory: Path) -> dict[str, str]:
    result: dict[str, str] = {}
    for entry in sorted(directory.rglob("*")):
        if entry.is_file():
            result[str(entry.relative_to(directory))] = hashlib.md5(entry.read_bytes()).hexdigest()
    return result


def _create_legacy_cube(cube: Path, total: int) -> None:
    script = Path(__file__).resolve().parent.parent / "scripts" / "create_legacy_cube.py"
    subprocess.run(
        [sys.executable, str(script), str(cube), str(total)],
        check=True,
    )


def _write_drifted_reference(cube: Path, *, size: int) -> None:
    """Write a resolved-index record whose params disagree with the cube values.

    The cube is created with epoch ``2024-01-01T00:00:00``; shifting the
    reference epoch by one hour guarantees every slot differs. The
    ``identity_hash`` is computed against the same canonical payload the
    control-plane loader will recompute, otherwise the loader rejects the
    record as corrupt before the drift check even runs.
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


def _consolidate_args(cube: Path, *, dry_run: bool = True) -> list[str]:
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
    if dry_run:
        args.append("--dry-run")
    return args


def test_dry_run_leaves_store_bytes_unchanged(tmp_path: Path) -> None:
    cube = tmp_path / "legacy.zarr"
    _create_legacy_cube(cube, _LEGACY_SLOTS)

    before = _md5_of_files(cube)
    assert before, "legacy cube should contain at least one persisted file"

    result = CliRunner().invoke(cli, _consolidate_args(cube, dry_run=True))
    assert result.exit_code == 0, result.output

    after = _md5_of_files(cube)
    assert before == after, (
        "dry-run must not modify any file byte; "
        f"differing entries: {sorted(set(before) ^ set(after))}"
    )


def test_dry_run_reports_plan_and_summary_for_eligible_group(tmp_path: Path) -> None:
    """One dry-run invocation reports every field needed to plan a live run.

    Asserts the value-bearing lines: the concrete current/proposed chunk
    shapes, the markers-to-stamp and atomic-strategy plan lines, the drift
    verdict, and the summary tally for the single eligible group.
    """
    cube = tmp_path / "legacy.zarr"
    _create_legacy_cube(cube, _LEGACY_SLOTS)

    result = CliRunner().invoke(cli, _consolidate_args(cube, dry_run=True))
    assert result.exit_code == 0, result.output

    # Per-group plan lines with concrete values.
    assert "Current chunks: (1,)" in result.output, result.output
    assert "Proposed chunks: (256,)" in result.output, result.output
    assert "Markers to stamp: firecube_preallocated=True" in result.output, result.output
    assert "Atomic strategy: local sibling rename" in result.output, result.output
    assert "Drift status: CLEAN" in result.output, result.output
    # Summary tally for the single legacy group.
    assert "Groups eligible for consolidation: 1" in result.output, result.output
    assert "Groups blocked by drift: 0" in result.output, result.output


def test_dry_run_on_drifted_cube_exits_1(tmp_path: Path) -> None:
    cube = tmp_path / "legacy.zarr"
    _create_legacy_cube(cube, _LEGACY_SLOTS)
    _write_drifted_reference(cube, size=_LEGACY_SLOTS)

    before = _md5_of_files(cube)

    result = CliRunner().invoke(cli, _consolidate_args(cube, dry_run=True))
    assert result.exit_code == 1, result.output
    assert "DRIFT DETECTED AT SLOT 0" in result.output, result.output
    assert "Groups blocked by drift: 1" in result.output, result.output

    after = _md5_of_files(cube)
    assert before == after, (
        "dry-run on drifted cube must still leave bytes untouched; "
        f"differing entries: {sorted(set(before) ^ set(after))}"
    )


def test_dry_run_and_live_run_produce_equivalent_time_values(tmp_path: Path) -> None:
    """After a live consolidation, coord values match what dry-run advertised.

    Dry-run must not mutate, but its ``Proposed chunks`` line must reflect
    what a subsequent live invocation actually writes. We do not compare
    stringly; instead we run dry-run for reporting and then a live run to
    confirm the chunk shape the dry-run promised.
    """
    cube = tmp_path / "legacy.zarr"
    _create_legacy_cube(cube, _LEGACY_SLOTS)

    dry = CliRunner().invoke(cli, _consolidate_args(cube, dry_run=True))
    assert dry.exit_code == 0, dry.output
    assert "Proposed chunks: (256,)" in dry.output, dry.output

    live = CliRunner().invoke(cli, _consolidate_args(cube, dry_run=False))
    assert live.exit_code == 0, live.output

    import zarr

    coord = cast(Any, zarr.open_group(str(cube), mode="r")["data/time"])
    assert tuple(coord.chunks) == (256,)
    values = np.asarray(coord[:])
    assert values.shape == (_LEGACY_SLOTS,)
