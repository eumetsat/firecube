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

"""``chunks runs {list,abandon} --target`` routes backend resolution explicitly.

When config or env points at a backend that does NOT hold the targeted run,
``--target`` overrides resolution and routes the command at the URI the
operator named. Without ``--target`` the commands keep their original
``resolve_cli_product`` fall-through.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest
from click.testing import CliRunner

from firecube.cli.main import cli
from firecube.core.controlplane import ChunkManager
from tests.helpers.storage import make_test_binding

pytestmark = pytest.mark.integration


_RUN_META = {
    "schema_version": "v2",
    "status": "started",
    "parts": 0,
    "events": 0,
    "started_at": 1.0,
    "updated_at": 1.0,
    "run_uri": "file:///tmp/placeholder",
    "run_stale_threshold_s": 3600,
}


def _write_run_json(root: Path, *, product: str, run_id: str, status: str = "started") -> Path:
    run_dir = root / ".firecube" / "runs" / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    payload = {
        **_RUN_META,
        "product": product,
        "run_id": run_id,
        "status": status,
    }
    run_json = run_dir / "run.json"
    run_json.write_text(json.dumps(payload, separators=(",", ":"), sort_keys=True))
    return run_json


def _populate_started_run(root: Path, *, product: str, run_id: str, workspace: Path) -> None:
    binding = make_test_binding(root, product=product)
    manager = ChunkManager(binding=binding, workspace=workspace)
    try:
        manager.record_run_started(
            product=product,
            run_id=run_id,
            output_path=str(root / product),
            output_format="zarr",
            size=0,
            meta={"plugin": "test"},
        )
    finally:
        manager.close()


def _list_runs(root: Path, *, product: str, workspace: Path) -> list:
    binding = make_test_binding(root, product=product)
    manager = ChunkManager(binding=binding, workspace=workspace)
    try:
        return list(manager.list_runs(product=product))
    finally:
        manager.close()


def test_abandon_with_target_routes_to_explicit_backend(tmp_path: Path) -> None:
    """``--target`` abandons the run in the targeted backend, not config's default.

    Two parallel product roots A and B exist on disk; only A holds a stuck
    run. ``--target file://<A>/prod1.zarr`` must route to A's control plane;
    B must remain untouched.
    """
    product = "prod1.zarr"
    root_a = tmp_path / "A"
    root_a.mkdir()
    root_b = tmp_path / "B"
    root_b.mkdir()
    workspace = tmp_path / "ws"
    workspace.mkdir()

    _populate_started_run(root_a, product=product, run_id="run-stuck", workspace=workspace)

    target_uri = (root_a / product).as_uri()
    result = CliRunner().invoke(
        cli,
        [
            "chunks",
            "--workspace",
            str(workspace),
            "runs",
            "abandon",
            "--product-name",
            product,
            "--target",
            target_uri,
            "--run-id",
            "run-stuck",
            "--reason",
            "operator: process died",
            "--yes-i-really-mean-it",
        ],
    )

    assert result.exit_code == 0, result.output

    runs_a = _list_runs(root_a, product=product, workspace=workspace)
    statuses_a = {run.run_id: run.status for run in runs_a}
    assert statuses_a == {"run-stuck": "abandoned"}, (statuses_a, result.output)

    runs_b = _list_runs(root_b, product=product, workspace=workspace)
    assert runs_b == [], (runs_b, result.output)


def test_list_with_target_filters_to_target_backend(tmp_path: Path) -> None:
    """``--target`` on ``list`` restricts inspection to the targeted backend.

    A holds a started run; B holds none. ``list --target file://<A>/prod1.zarr``
    must surface A's run; ``list --target file://<B>/prod1.zarr`` must return
    an empty list.
    """
    product = "prod1.zarr"
    root_a = tmp_path / "A"
    root_a.mkdir()
    root_b = tmp_path / "B"
    root_b.mkdir()
    workspace = tmp_path / "ws"
    workspace.mkdir()

    _populate_started_run(root_a, product=product, run_id="run-a-001", workspace=workspace)

    target_a_uri = (root_a / product).as_uri()
    result_a = CliRunner().invoke(
        cli,
        [
            "chunks",
            "--workspace",
            str(workspace),
            "runs",
            "list",
            "--product-name",
            product,
            "--target",
            target_a_uri,
            "--format",
            "json",
        ],
    )

    assert result_a.exit_code == 0, result_a.output
    assert "run-a-001" in result_a.output, result_a.output

    target_b_uri = (root_b / product).as_uri()
    result_b = CliRunner().invoke(
        cli,
        [
            "chunks",
            "--workspace",
            str(workspace),
            "runs",
            "list",
            "--product-name",
            product,
            "--target",
            target_b_uri,
            "--format",
            "json",
        ],
    )

    assert result_b.exit_code == 0, result_b.output
    assert "run-a-001" not in result_b.output, result_b.output


def test_abandon_without_target_accepts_product_uri(tmp_path: Path) -> None:
    """Without ``--target``, a product URI in ``--product-name`` still routes."""
    workspace = tmp_path / "ws"
    workspace.mkdir()
    root = tmp_path / "store"
    root.mkdir()
    product = "prod1.zarr"
    _populate_started_run(root, product=product, run_id="run-x", workspace=workspace)

    product_uri = (root / product).as_uri()
    result = CliRunner().invoke(
        cli,
        [
            "chunks",
            "--workspace",
            str(workspace),
            "runs",
            "abandon",
            "--product-name",
            product_uri,
            "--run-id",
            "run-x",
            "--reason",
            "test",
            "--yes-i-really-mean-it",
        ],
    )

    assert result.exit_code == 0, result.output
    statuses = {
        run.run_id: run.status for run in _list_runs(root, product=product, workspace=workspace)
    }
    assert statuses == {"run-x": "abandoned"}


def test_abandon_target_rejects_bare_path(tmp_path: Path) -> None:
    """``--target`` must be a full URI; bare paths are rejected up front."""
    workspace = tmp_path / "ws"
    workspace.mkdir()

    result = CliRunner().invoke(
        cli,
        [
            "chunks",
            "--workspace",
            str(workspace),
            "runs",
            "abandon",
            "--product-name",
            "prod1",
            "--target",
            "/tmp/not-a-uri",
            "--run-id",
            "run-x",
            "--reason",
            "test",
            "--yes-i-really-mean-it",
        ],
    )

    assert result.exit_code != 0
    assert "--target" in result.output


def test_runs_list_basename_mismatch(tmp_path: Path) -> None:
    product_name = "my_product"
    target_root = tmp_path / "cube-data.zarr"
    _write_run_json(target_root, product=product_name, run_id="run-001")

    result = CliRunner().invoke(
        cli,
        [
            "chunks",
            "runs",
            "list",
            "--product-name",
            product_name,
            "--target",
            target_root.as_uri(),
            "--format",
            "json",
        ],
    )

    assert result.exit_code == 0, result.output
    output_lines = result.output.splitlines()
    if any(line.strip() == "[]" for line in output_lines):
        runs = []
    else:
        json_start = next(i for i, line in enumerate(output_lines) if line.strip() == "[")
        json_text = "\n".join(output_lines[json_start:]).split("\n--- Logging error ---", 1)[0]
        runs = json.loads(json_text)
    assert len(runs) == 1, runs
    assert runs[0]["run_id"] == "run-001"
    assert runs[0]["status"] == "started"


def test_runs_abandon_basename_mismatch_writes_to_target(tmp_path: Path) -> None:
    product_name = "my_product"
    target_root = tmp_path / "cube-data.zarr"
    _write_run_json(target_root, product=product_name, run_id="run-001")

    result = CliRunner().invoke(
        cli,
        [
            "chunks",
            "runs",
            "abandon",
            "--product-name",
            product_name,
            "--target",
            target_root.as_uri(),
            "--run-id",
            "run-001",
            "--reason",
            "regression",
            "--yes-i-really-mean-it",
        ],
    )

    assert result.exit_code == 0, result.output
    run_json = target_root / ".firecube" / "runs" / "run-001" / "run.json"
    assert json.loads(run_json.read_text())["status"] == "abandoned"
    assert not (tmp_path / "my_product").exists()


def test_runs_abandon_basename_mismatch_does_not_mutate_existing_sibling(tmp_path: Path) -> None:
    product_name = "my_product"
    target_root = tmp_path / "cube-data.zarr"
    sibling_root = tmp_path / product_name
    target_run_json = _write_run_json(target_root, product=product_name, run_id="run-001")
    sibling_run_json = _write_run_json(sibling_root, product=product_name, run_id="run-001")
    sibling_before = hashlib.md5(sibling_run_json.read_bytes()).hexdigest()

    result = CliRunner().invoke(
        cli,
        [
            "chunks",
            "runs",
            "abandon",
            "--product-name",
            product_name,
            "--target",
            target_root.as_uri(),
            "--run-id",
            "run-001",
            "--reason",
            "regression",
            "--yes-i-really-mean-it",
        ],
    )

    assert result.exit_code == 0, result.output
    assert json.loads(target_run_json.read_text())["status"] == "abandoned"
    assert hashlib.md5(sibling_run_json.read_bytes()).hexdigest() == sibling_before
    assert not list((sibling_root / ".firecube").rglob("events-*.jsonl"))
    assert not list((sibling_root / ".firecube").glob("schema.json"))
