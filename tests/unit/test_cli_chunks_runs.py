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


class _FakeRunsManager:
    def __init__(self) -> None:
        self.abandoned: list[tuple[str, str, str]] = []

    def abandon_run(self, *, product: str, run_id: str, reason: str):
        self.abandoned.append((product, run_id, reason))
        return {"abandoned": True, "status": "abandoned"}


def test_abandon_help_advertises_yes_and_dry_run():
    r = CliRunner().invoke(cli, ["chunks", "runs", "abandon", "--help"])

    assert r.exit_code == 0
    assert "--dry-run" in r.output
    assert "--yes-i-really-mean-it" in r.output


def test_abandon_non_tty_without_yes_exits_nonzero():
    r = CliRunner().invoke(
        cli,
        [
            "chunks",
            "runs",
            "abandon",
            "--product-name",
            "file:///tmp/wk/TEST_PRODUCT.zarr",
            "--run-id",
            "run-xyz",
            "--reason",
            "crashed",
        ],
    )

    assert r.exit_code != 0
    assert "yes-i-really-mean-it" in r.output.lower() or "confirmation" in r.output.lower()


def test_abandon_dry_run_does_not_invoke_manager(monkeypatch):
    manager = _FakeRunsManager()
    monkeypatch.setattr(
        "firecube.cli.chunks._runs.resolve_manager",
        lambda *args, **kwargs: manager,
    )

    r = CliRunner().invoke(
        cli,
        [
            "chunks",
            "runs",
            "abandon",
            "--product-name",
            "file:///tmp/wk/TEST_PRODUCT.zarr",
            "--run-id",
            "run-xyz",
            "--reason",
            "crashed",
            "--dry-run",
        ],
    )

    assert r.exit_code == 0, r.output
    assert "[dry-run]" in r.output
    assert "run-xyz" in r.output
    assert manager.abandoned == []


def test_abandon_with_yes_invokes_manager(monkeypatch):
    manager = _FakeRunsManager()
    monkeypatch.setattr(
        "firecube.cli.chunks._runs.resolve_manager",
        lambda *args, **kwargs: manager,
    )

    r = CliRunner().invoke(
        cli,
        [
            "chunks",
            "runs",
            "abandon",
            "--product-name",
            "file:///tmp/wk/TEST_PRODUCT.zarr",
            "--run-id",
            "run-xyz",
            "--reason",
            "crashed",
            "--yes-i-really-mean-it",
        ],
    )

    assert r.exit_code == 0, r.output
    assert len(manager.abandoned) == 1
    assert manager.abandoned[0][1] == "run-xyz"
