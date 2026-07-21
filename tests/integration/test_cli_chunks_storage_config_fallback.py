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

"""T16: ``chunks runs list`` still honors the storage-config fallback path.

This pins the historical ``resolve_manager`` behavior for the runs
subcommand when ``--target`` is omitted: the CLI must resolve the product
from the storage config and surface control-plane state under that base.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from click.testing import CliRunner

from firecube.cli.main import cli

pytestmark = pytest.mark.integration


_CONFIG_TEMPLATE = """\
[storage]
type = "local"
target_path = "{target_path}"
"""

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


def _write_config(tmp_path: Path) -> Path:
    config_file = tmp_path / "firecube-test.toml"
    config_file.write_text(
        _CONFIG_TEMPLATE.format(target_path=tmp_path),
        encoding="utf-8",
    )
    return config_file


def _clean_storage_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in (
        "FIRECUBE_STORAGE_TYPE",
        "FIRECUBE_TARGET_PATH",
        "FIRECUBE_BUCKET",
        "FIRECUBE_CONFIG",
        "FIRECUBE_ENDPOINT_URL",
        "FIRECUBE_ACCESS_KEY",
        "FIRECUBE_SECRET_KEY",
    ):
        monkeypatch.delenv(name, raising=False)


def _write_run_json(root: Path, *, product: str, run_id: str) -> Path:
    run_dir = root / product / ".firecube" / "runs" / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    payload = {
        **_RUN_META,
        "product": product,
        "run_id": run_id,
    }
    run_json = run_dir / "run.json"
    run_json.write_text(json.dumps(payload, separators=(",", ":"), sort_keys=True))
    return run_json


def test_runs_list_storage_config_fallback_finds_run(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    product_name = "my_product"
    workspace = tmp_path / "ws"
    workspace.mkdir()
    _clean_storage_env(monkeypatch)
    config_file = _write_config(tmp_path)
    _write_run_json(tmp_path, product=product_name, run_id="run-001")

    result = CliRunner().invoke(
        cli,
        [
            "--config-file",
            str(config_file),
            "chunks",
            "--workspace",
            str(workspace),
            "runs",
            "list",
            "--product-name",
            product_name,
            "--format",
            "json",
        ],
    )

    assert result.exit_code == 0, result.output
    output = result.stdout.strip()
    payload = json.loads(output)
    assert len(payload) == 1, payload
    assert payload[0]["run_id"] == "run-001"
    assert payload[0]["status"] == "started"
