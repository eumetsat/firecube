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

"""Tests for CLI rename-hint layer."""

from __future__ import annotations

import click
import pytest
from click.testing import CliRunner

from firecube.cli.main import cli


def test_ingest_source_shows_input_data_hint() -> None:
    runner = CliRunner()
    result = runner.invoke(cli, ["ingest", "cli_test_plugin", "--source", "/tmp/data"])
    assert result.exit_code == 2
    output = result.output
    assert "No such option '--source'" in output
    assert "Hint:" in output
    assert "--input-data (-i)" in output
    assert "strict-URI refactor" in output


def test_archive_create_target_shows_archive_hint() -> None:
    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["archive", "create", "--target", "/tmp/x.tgm", "--source", "file:///tmp/p.zarr"],
    )
    assert result.exit_code == 2
    output = result.output
    assert "No such option '--target'" in output
    assert "Hint:" in output
    assert "--archive (-a)" in output
    assert "tgm artifact output" in output


def test_archive_restore_source_shows_archive_hint() -> None:
    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["archive", "restore", "--source", "/tmp/x.tgm", "--target", "file:///tmp/p.zarr"],
    )
    assert result.exit_code == 2
    output = result.output
    assert "No such option '--source'" in output
    assert "Hint:" in output
    assert "--archive (-a)" in output
    assert "tgm artifact path" in output


def test_unknown_flag_without_registry_entry_has_no_hint() -> None:
    runner = CliRunner()
    result = runner.invoke(cli, ["ingest", "cli_test_plugin", "--definitely-not-a-flag", "x"])
    assert result.exit_code == 2
    assert "Hint:" not in result.output


def test_chunks_product_still_uses_click_native_suggestion() -> None:
    """--product -> --product-name is auto-suggested by Click; we don't override."""
    runner = CliRunner()
    result = runner.invoke(cli, ["chunks", "list", "--product", "x"])
    assert result.exit_code == 2
    output = result.output
    assert "No such option '--product'" in output
    assert "Did you mean" in output
    assert "--product-name" in output
    assert "Hint:" not in output


def test_old_target_on_archive_restore_no_hint() -> None:
    """--target is a valid product URI flag on archive restore."""
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "archive",
            "restore",
            "--archive",
            "file:///tmp/x.tgm",
            "--target",
            "file:///tmp/p.zarr",
            "--dry-run",
        ],
    )
    assert result.exit_code == 0, result.output
    assert "[dry-run] Would restore:" in result.output
    assert "Hint:" not in result.output


def test_old_source_on_archive_create_no_hint() -> None:
    """--source is the valid product URI flag on archive create."""
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "archive",
            "create",
            "--source",
            "file:///tmp/p.zarr",
            "--archive",
            "file:///tmp/x.tgm",
            "--dry-run",
        ],
    )
    assert result.exit_code == 0, result.output
    assert "[dry-run] Would archive:" in result.output
    assert "Hint:" not in result.output


def test_hint_fires_under_real_prog_name() -> None:
    """Regression: hint must fire when invoked as the ``firecube`` binary, not only CliRunner.

    Catches the ``build_command_path`` bug where filtering by hardcoded ``"cli"``
    info_name silently breaks production (where info_name is ``"firecube"``)
    while tests pass.
    """
    with pytest.raises(click.NoSuchOption) as excinfo:
        cli(
            args=["ingest", "cli_test_plugin", "--source", "/tmp"],
            prog_name="firecube",
            standalone_mode=False,
        )
    assert "Hint:" in excinfo.value.message
    assert "--input-data (-i)" in excinfo.value.message
