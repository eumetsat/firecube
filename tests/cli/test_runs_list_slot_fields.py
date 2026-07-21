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

import json
from pathlib import Path

from click.testing import CliRunner

from firecube.cli.main import cli
from firecube.core.controlplane import ChunkManager
from tests.helpers.storage import make_test_binding


def _record_run(
    root: Path,
    *,
    workspace: Path,
    product: str,
    run_id: str,
    slot_range: tuple[int, int] | None = None,
    slot_group: str | None = None,
) -> None:
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
            slot_range=slot_range,
            slot_group=slot_group,
        )
    finally:
        manager.close()


def test_runs_list_json_includes_slot_fields(tmp_path: Path) -> None:
    workspace = tmp_path / "ws"
    workspace.mkdir()
    root = tmp_path / "store"
    root.mkdir()
    product = "prod1.zarr"

    _record_run(
        root,
        workspace=workspace,
        product=product,
        run_id="run-slot",
        slot_range=(100, 200),
        slot_group="GRP",
    )
    _record_run(root, workspace=workspace, product=product, run_id="run-base")

    result = CliRunner().invoke(
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
            (root / product).as_uri(),
            "-f",
            "json",
        ],
    )

    assert result.exit_code == 0, result.output

    try:
        runs = json.loads(result.output)
    except json.JSONDecodeError:
        json_start = result.output.rfind("\n[")
        assert json_start != -1, result.output
        runs = json.loads(result.output[json_start + 1 :])
    runs_by_id = {run["run_id"]: run for run in runs}

    assert runs_by_id["run-slot"]["slot_range"] == [100, 200]
    assert runs_by_id["run-slot"]["slot_group"] == "GRP"
    assert runs_by_id["run-base"]["slot_range"] is None
    assert runs_by_id["run-base"]["slot_group"] is None
