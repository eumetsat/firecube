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

from pathlib import Path
from typing import Any

import pytest
from click.testing import CliRunner

from firecube.cli.main import cli
from firecube.core.controlplane.types import AbandonSweepResult
from tests.helpers.chunks import bound
from tests.helpers.rows import table_row


class _FakeRunsManager:
    def __init__(self, sweep_result: AbandonSweepResult | None = None) -> None:
        self.sweep_result = sweep_result or AbandonSweepResult()
        self.sweep_calls: list[tuple[str, str, bool]] = []
        self.run_calls: list[tuple[str, str, str]] = []

    def abandon_stale_runs(self, *, product: str, reason: str, dry_run: bool = True):
        self.sweep_calls.append((product, reason, dry_run))
        return self.sweep_result

    def abandon_run(self, *, product: str, run_id: str, reason: str):
        self.run_calls.append((product, run_id, reason))
        return {"abandoned": True, "status": "abandoned"}


def test_runs_abandon_all_stale(
    subtests: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    with table_row(subtests, "abandon_help_shows_run_id_all_stale_and_required_reason", tmp_path):
        r = CliRunner().invoke(cli, ["chunks", "runs", "abandon", "--help"])

        assert r.exit_code == 0
        assert "--run-id" in r.output
        assert "--all-stale" in r.output
        assert "--reason" in r.output
        assert "[required]" in r.output

    with table_row(
        subtests, "abandon_all_stale_defaults_to_dry_run_and_previews", tmp_path, monkeypatch
    ) as (_, mp):
        assert mp is not None
        manager = _FakeRunsManager(
            AbandonSweepResult(previewed=["run-a", "run-b"]),
        )
        mp.setattr(
            "firecube.cli.chunks._runs.target_product", lambda *args, **kwargs: bound(manager)
        )

        r = CliRunner().invoke(
            cli,
            [
                "chunks",
                "runs",
                "abandon",
                "--target",
                "file:///tmp/wk/TEST_PRODUCT.zarr",
                "--all-stale",
                "--reason",
                "heartbeat stalled",
            ],
        )

        assert r.exit_code == 0, r.output
        assert manager.sweep_calls == [("TEST_PRODUCT.zarr", "heartbeat stalled", True)]
        assert "[dry-run] Would abandon stale runs" in r.output
        assert "run-a" in r.output and "run-b" in r.output

    with table_row(subtests, "abandon_all_stale_with_yes_commits", tmp_path, monkeypatch) as (
        _,
        mp,
    ):
        assert mp is not None
        manager = _FakeRunsManager(
            AbandonSweepResult(previewed=["run-a"], abandoned=["run-a"]),
        )
        mp.setattr(
            "firecube.cli.chunks._runs.target_product", lambda *args, **kwargs: bound(manager)
        )

        r = CliRunner().invoke(
            cli,
            [
                "chunks",
                "runs",
                "abandon",
                "--target",
                "file:///tmp/wk/TEST_PRODUCT.zarr",
                "--all-stale",
                "--reason",
                "heartbeat stalled",
                "--yes-i-really-mean-it",
            ],
        )

        assert r.exit_code == 0, r.output
        assert manager.sweep_calls == [("TEST_PRODUCT.zarr", "heartbeat stalled", False)]
        assert "Abandoned stale runs for TEST_PRODUCT.zarr" in r.output
        assert "run-a" in r.output

    with table_row(subtests, "abandon_all_stale_missing_reason_fails", tmp_path):
        r = CliRunner().invoke(
            cli,
            [
                "chunks",
                "runs",
                "abandon",
                "--target",
                "file:///tmp/wk/TEST_PRODUCT.zarr",
                "--all-stale",
                "--yes-i-really-mean-it",
            ],
        )

        assert r.exit_code != 0
        assert "--reason" in r.output

    with table_row(subtests, "abandon_run_id_and_all_stale_are_mutually_exclusive", tmp_path):
        r = CliRunner().invoke(
            cli,
            [
                "chunks",
                "runs",
                "abandon",
                "--target",
                "file:///tmp/wk/TEST_PRODUCT.zarr",
                "--run-id",
                "run-xyz",
                "--all-stale",
                "--reason",
                "crashed",
            ],
        )

        assert r.exit_code != 0
        assert "exactly one" in r.output.lower() or "mutually exclusive" in r.output.lower()

    with table_row(
        subtests, "abandon_all_stale_with_no_stale_runs_reports_none", tmp_path, monkeypatch
    ) as (_, mp):
        assert mp is not None
        manager = _FakeRunsManager(AbandonSweepResult())
        mp.setattr(
            "firecube.cli.chunks._runs.target_product", lambda *args, **kwargs: bound(manager)
        )

        r = CliRunner().invoke(
            cli,
            [
                "chunks",
                "runs",
                "abandon",
                "--target",
                "file:///tmp/wk/TEST_PRODUCT.zarr",
                "--all-stale",
                "--reason",
                "heartbeat stalled",
            ],
        )

        assert r.exit_code == 0, r.output
        assert "No stale runs found." in r.output

    with table_row(
        subtests, "abandon_run_id_with_yes_still_invokes_manager", tmp_path, monkeypatch
    ) as (_, mp):
        assert mp is not None
        manager = _FakeRunsManager()
        mp.setattr(
            "firecube.cli.chunks._runs.target_product", lambda *args, **kwargs: bound(manager)
        )

        r = CliRunner().invoke(
            cli,
            [
                "chunks",
                "runs",
                "abandon",
                "--target",
                "file:///tmp/wk/TEST_PRODUCT.zarr",
                "--run-id",
                "run-xyz",
                "--reason",
                "crashed",
                "--yes-i-really-mean-it",
            ],
        )

        assert r.exit_code == 0, r.output
        assert manager.run_calls == [("TEST_PRODUCT.zarr", "run-xyz", "crashed")]
        assert "Abandoned run run-xyz for TEST_PRODUCT.zarr" in r.output


def test_sweep_prints_claims_released_without_abandoned_runs(monkeypatch: pytest.MonkeyPatch):
    manager = _FakeRunsManager(AbandonSweepResult(released_claims=["P.zarr:zarr_region:grp"]))
    monkeypatch.setattr("firecube.cli.chunks._runs.target_product", lambda *a, **k: bound(manager))
    args = "chunks runs abandon --target file:///tmp/wk/P.zarr --all-stale --reason crashed"
    r = CliRunner().invoke(cli, [*args.split(), "--yes-i-really-mean-it"])
    assert r.exit_code == 0, r.output
    assert "Released claim P.zarr:zarr_region:grp" in r.output


def test_dry_run_prints_claims_the_sweep_would_release(monkeypatch: pytest.MonkeyPatch):
    sweep = AbandonSweepResult(previewed=["r"], released_claims=["P:a"], skipped_claims=["P:b"])
    manager = _FakeRunsManager(sweep)
    monkeypatch.setattr("firecube.cli.chunks._runs.target_product", lambda *a, **k: bound(manager))
    args = "chunks runs abandon --target file:///tmp/wk/P.zarr --all-stale --reason crashed"
    r = CliRunner().invoke(cli, args.split())
    assert "Would release claim P:a" in r.output and "Would leave claim P:b" in r.output
    assert "Released claim" not in r.output
