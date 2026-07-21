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

from click.testing import CliRunner

from firecube.cli.main import cli


class _FakeSnapshotsManager:
    def __init__(self) -> None:
        self.rebuilt: list[str] = []

    def rebuild_snapshot(self, product: str):
        self.rebuilt.append(product)
        return {"generation": 1, "records": 0}


def test_rebuild_help_advertises_dry_run_only():
    r = CliRunner().invoke(cli, ["chunks", "snapshots", "rebuild", "--help"])

    assert r.exit_code == 0
    assert "--dry-run" in r.output
    assert "--yes-i-really-mean-it" not in r.output


def test_rebuild_dry_run_does_not_invoke_manager(monkeypatch):
    manager = _FakeSnapshotsManager()
    monkeypatch.setattr(
        "firecube.cli.chunks._snapshots.resolve_manager",
        lambda *args, **kwargs: manager,
    )

    r = CliRunner().invoke(
        cli,
        [
            "chunks",
            "snapshots",
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
        "firecube.cli.chunks._snapshots.resolve_manager",
        lambda *args, **kwargs: manager,
    )

    r = CliRunner().invoke(
        cli,
        [
            "chunks",
            "snapshots",
            "rebuild",
            "--product-name",
            "file:///tmp/wk/TEST_PRODUCT.zarr",
        ],
    )

    assert r.exit_code == 0, r.output
    assert "yes-i-really-mean-it" not in r.output.lower()
    assert manager.rebuilt == ["TEST_PRODUCT.zarr"]
