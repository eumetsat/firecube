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


class _FakeClaimsManager:
    def __init__(self) -> None:
        self.cleared: list[tuple[str, str, bool]] = []

    def clear_claim(self, *, product: str, domain_id: str, force: bool) -> bool:
        self.cleared.append((product, domain_id, force))
        return True


def test_clear_help_advertises_both_force_and_yes():
    r = CliRunner().invoke(cli, ["chunks", "claims", "clear", "--help"])

    assert r.exit_code == 0
    assert "--force" in r.output
    assert "--yes-i-really-mean-it" in r.output
    assert "operational bypass" in r.output.lower() or "is_stale" in r.output.lower()


def test_clear_force_alone_non_tty_exits_nonzero():
    r = CliRunner().invoke(
        cli,
        [
            "chunks",
            "claims",
            "clear",
            "-n",
            "file:///tmp/wk/TEST_PRODUCT.zarr",
            "--domain",
            "TEST_PRODUCT.zarr/maintenance/op",
            "--force",
        ],
    )

    assert r.exit_code != 0
    assert "yes-i-really-mean-it" in r.output.lower() or "confirmation" in r.output.lower()


def test_clear_with_force_and_yes_invokes_manager(monkeypatch):
    manager = _FakeClaimsManager()
    monkeypatch.setattr(
        "firecube.cli.chunks._claims.resolve_manager",
        lambda *args, **kwargs: manager,
    )

    r = CliRunner().invoke(
        cli,
        [
            "chunks",
            "claims",
            "clear",
            "-n",
            "file:///tmp/wk/TEST_PRODUCT.zarr",
            "--domain",
            "TEST_PRODUCT.zarr/maintenance/op",
            "--force",
            "--yes-i-really-mean-it",
        ],
    )

    assert r.exit_code == 0, r.output
    assert len(manager.cleared) == 1
    assert manager.cleared[0][2] is True


def test_clear_with_yes_only_invokes_manager_without_force(monkeypatch):
    manager = _FakeClaimsManager()
    monkeypatch.setattr(
        "firecube.cli.chunks._claims.resolve_manager",
        lambda *args, **kwargs: manager,
    )

    r = CliRunner().invoke(
        cli,
        [
            "chunks",
            "claims",
            "clear",
            "-n",
            "file:///tmp/wk/TEST_PRODUCT.zarr",
            "--domain",
            "TEST_PRODUCT.zarr/maintenance/op",
            "--yes-i-really-mean-it",
        ],
    )

    assert r.exit_code == 0, r.output
    assert len(manager.cleared) == 1
    assert manager.cleared[0][2] is False
