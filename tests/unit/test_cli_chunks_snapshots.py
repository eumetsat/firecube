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

import pytest
from click.testing import CliRunner

from firecube.cli.main import cli
from firecube.core.storage.uri import StorageUri


class _FakeSnapshotsManager:
    def __init__(self) -> None:
        self.rebuilt: list[str] = []

    def rebuild_compact(self, product: str):
        self.rebuilt.append(product)
        return {"generation": 1, "records": 0}


def test_rebuild_help_advertises_dry_run_only():
    r = CliRunner().invoke(cli, ["chunks", "compact", "rebuild", "--help"])

    assert r.exit_code == 0
    assert "--dry-run" in r.output
    assert "--yes-i-really-mean-it" not in r.output


def test_rebuild_dry_run_does_not_invoke_manager(monkeypatch):
    manager = _FakeSnapshotsManager()
    monkeypatch.setattr(
        "firecube.cli.chunks._compact.resolve_manager",
        lambda *args, **kwargs: manager,
    )

    r = CliRunner().invoke(
        cli,
        [
            "chunks",
            "compact",
            "rebuild",
            "--product-name",
            "file:///tmp/wk/TEST_PRODUCT.zarr",
            "--dry-run",
        ],
    )

    assert r.exit_code == 0, r.output
    assert "[dry-run]" in r.output
    assert manager.rebuilt == []


def test_rebuild_without_dry_run_does_not_require_confirmation(monkeypatch):
    manager = _FakeSnapshotsManager()
    monkeypatch.setattr(
        "firecube.cli.chunks._compact.resolve_manager",
        lambda *args, **kwargs: manager,
    )

    r = CliRunner().invoke(
        cli,
        [
            "chunks",
            "compact",
            "rebuild",
            "--product-name",
            "file:///tmp/wk/TEST_PRODUCT.zarr",
        ],
    )

    assert r.exit_code == 0, r.output
    assert "yes-i-really-mean-it" not in r.output.lower()
    assert manager.rebuilt == ["TEST_PRODUCT.zarr"]


@pytest.mark.contract
@pytest.mark.parametrize("group", ["compact", "snapshots"])
def test_compact_and_alias_rebuild_and_status_use_compact_dir(group, tmp_path):
    uri = StorageUri.from_local_path(tmp_path / "P.zarr").to_str()
    base = ["chunks", group]
    opts = ["--product-name", uri, "--workspace", str(tmp_path / "ws"), "-f", "json"]

    rebuilt = CliRunner().invoke(cli, [*base, "rebuild", *opts])
    status = CliRunner().invoke(cli, [*base, "status", *opts])

    assert rebuilt.exit_code == 0 and status.exit_code == 0, (rebuilt.output, status.output)
    rebuilt_json, status_json = json.loads(rebuilt.output), json.loads(status.output)
    compact = tmp_path / "P.zarr" / ".firecube" / "compact"
    assert Path(rebuilt_json["snapshot_path"].removeprefix("file://")).parent == compact
    assert (compact / f"compact-{rebuilt_json['generation']}.jsonl").is_file()
    assert (compact / "LATEST.json").is_file()
    assert status_json["exists"] is True
    assert status_json["generation"] == rebuilt_json["generation"]


@pytest.mark.contract
def test_snapshots_alias_is_hidden_from_chunks_help():
    listed = CliRunner().invoke(cli, ["chunks", "--help"]).output

    assert "compact" in listed and "snapshots" not in listed
