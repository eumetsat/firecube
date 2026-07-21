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
from firecube.core.controlplane.types import ClearSweepResult


class _FakeClaimsManager:
    def __init__(self, result: ClearSweepResult | None = None) -> None:
        self.result = result if result is not None else ClearSweepResult()
        self.clear_stale_calls: list[tuple[str, bool]] = []
        self.clear_calls: list[tuple[str, str, bool]] = []

    def clear_stale_claims(self, *, product: str, dry_run: bool) -> ClearSweepResult:
        self.clear_stale_calls.append((product, dry_run))
        return self.result

    def clear_claim(self, *, product: str, domain_id: str, force: bool) -> bool:
        self.clear_calls.append((product, domain_id, force))
        return True


def test_clear_help_shows_domain_and_all_stale():
    runner = CliRunner()

    result = runner.invoke(cli, ["chunks", "claims", "clear", "--help"])

    assert result.exit_code == 0
    assert "--domain" in result.output
    assert "--all-stale" in result.output


def test_clear_domain_and_all_stale_are_mutually_exclusive(monkeypatch):
    runner = CliRunner()
    manager = _FakeClaimsManager()
    monkeypatch.setattr(
        "firecube.cli.chunks._claims.resolve_manager", lambda *args, **kwargs: manager
    )

    result = runner.invoke(
        cli,
        [
            "chunks",
            "claims",
            "clear",
            "-n",
            "file:///tmp/TEST_PRODUCT.zarr",
            "--domain",
            "TEST_PRODUCT.zarr:zarr_group:F024",
            "--all-stale",
        ],
    )

    assert result.exit_code != 0
    assert "mutually exclusive" in result.output.lower() or "exactly one" in result.output.lower()
    assert manager.clear_stale_calls == []
    assert manager.clear_calls == []


def test_clear_all_stale_defaults_to_dry_run(monkeypatch):
    runner = CliRunner()
    manager = _FakeClaimsManager(
        ClearSweepResult(
            previewed=["TEST_PRODUCT.zarr:zarr_group:F001", "TEST_PRODUCT.zarr:zarr_group:F002"],
        )
    )
    monkeypatch.setattr(
        "firecube.cli.chunks._claims.resolve_manager", lambda *args, **kwargs: manager
    )

    result = runner.invoke(
        cli,
        ["chunks", "claims", "clear", "-n", "file:///tmp/TEST_PRODUCT.zarr", "--all-stale"],
    )

    assert result.exit_code == 0, result.output
    assert manager.clear_stale_calls == [("TEST_PRODUCT.zarr", True)]
    assert "Previewed stale claims:" in result.output
    assert "TEST_PRODUCT.zarr:zarr_group:F001" in result.output
    assert "TEST_PRODUCT.zarr:zarr_group:F002" in result.output
    assert "Dry run only; no claims cleared." in result.output


def test_clear_all_stale_commits_with_yes(monkeypatch):
    runner = CliRunner()
    manager = _FakeClaimsManager(
        ClearSweepResult(
            previewed=["TEST_PRODUCT.zarr:zarr_group:F001"],
            cleared=["TEST_PRODUCT.zarr:zarr_group:F001"],
        )
    )
    monkeypatch.setattr(
        "firecube.cli.chunks._claims.resolve_manager", lambda *args, **kwargs: manager
    )

    result = runner.invoke(
        cli,
        [
            "chunks",
            "claims",
            "clear",
            "-n",
            "file:///tmp/TEST_PRODUCT.zarr",
            "--all-stale",
            "--yes-i-really-mean-it",
        ],
    )

    assert result.exit_code == 0, result.output
    assert manager.clear_stale_calls == [("TEST_PRODUCT.zarr", False)]
    assert "Cleared stale claims:" in result.output
    assert "TEST_PRODUCT.zarr:zarr_group:F001" in result.output
    assert "Skipped fresh claims: 0" in result.output
    assert "Skipped missing claims: 0" in result.output


def test_clear_all_stale_empty_product_prints_no_stale_message(monkeypatch):
    runner = CliRunner()
    manager = _FakeClaimsManager(ClearSweepResult())
    monkeypatch.setattr(
        "firecube.cli.chunks._claims.resolve_manager", lambda *args, **kwargs: manager
    )

    result = runner.invoke(
        cli,
        ["chunks", "claims", "clear", "-n", "file:///tmp/TEST_PRODUCT.zarr", "--all-stale"],
    )

    assert result.exit_code == 0
    assert "No stale claims found" in result.output
    assert manager.clear_stale_calls == [("TEST_PRODUCT.zarr", True)]


def test_clear_domain_single_item_path_still_works(monkeypatch):
    runner = CliRunner()
    manager = _FakeClaimsManager()
    monkeypatch.setattr(
        "firecube.cli.chunks._claims.resolve_manager", lambda *args, **kwargs: manager
    )

    result = runner.invoke(
        cli,
        [
            "chunks",
            "claims",
            "clear",
            "-n",
            "file:///tmp/TEST_PRODUCT.zarr",
            "--domain",
            "TEST_PRODUCT.zarr:zarr_group:F024",
            "--yes-i-really-mean-it",
        ],
    )

    assert result.exit_code == 0, result.output
    assert manager.clear_calls == [
        ("TEST_PRODUCT.zarr", "TEST_PRODUCT.zarr:zarr_group:F024", False)
    ]
    assert "Cleared claim for TEST_PRODUCT.zarr:zarr_group:F024" in result.output
