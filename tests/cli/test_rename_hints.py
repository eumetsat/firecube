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

import zipfile

import click
import pytest
import zarr
from click.testing import CliRunner

from firecube.cli.main import cli


def test_renamed_flags_show_replacement_hint(subtests) -> None:
    tgt = ("Use --target (-t) instead", "product URI with --target")
    out = ("Leave it out", "records the product label")
    rows = {
        "ingest_source": ("ingest cli_test_plugin --source /d", "--input-data (-i)", "strict-URI"),
        "restore_source": ("archive restore --source /x.tgm", "--archive (-a)", "tgm artifact"),
        "archive_create_source": ("archive create --source /p", *tgt),
        "archive_create_short_s": ("archive create -s /p", *tgt),
        "zarr_validate_product": ("zarr validate --product /p", *tgt),
        "advise_short_p": ("advise batch-size -p /p", *tgt),
        "chunks_short_n": ("chunks list -n /p", *tgt),
        "chunks_product_name": ("chunks list --product-name /p", *tgt),
        "ingest_short_n": ("ingest cli_test_plugin -n x", "--product-name", "no short form"),
        "snapshot_product_name": ("snapshot take --product-name x", *out),
        "chunks_quiet": ("chunks list --quiet", "Leave it out", "no longer take it"),
    }
    for row_id, (command, replacement, reason) in rows.items():
        flag = next(word for word in command.split() if word.startswith("-"))
        with subtests.test(msg=row_id):
            result = CliRunner().invoke(cli, command.split())
            assert result.exit_code == 2
            for expected in ("No such option", flag, "Hint:", replacement, reason):
                assert expected in result.output


def test_unknown_flag_without_registry_entry_has_no_hint() -> None:
    runner = CliRunner()
    result = runner.invoke(cli, ["ingest", "cli_test_plugin", "--definitely-not-a-flag", "x"])
    assert result.exit_code == 2
    assert "Hint:" not in result.output


def test_chunks_product_has_no_registry_hint_and_gets_clicks_suggestion() -> None:
    """The registry has no ``--product`` entry for ``chunks``, so Click's own suggestion shows.

    Plan 2026-11-06 Decisions row "Product URI" says each removed ``-p/--product`` gets a hint;
    ``chunks`` never took ``--product``, and the registry (``_rename_hints.py``) has no row for
    it. The suggestion is Click's, ``--all-products``, which is not the replacement (``--target``).
    """
    result = CliRunner().invoke(cli, ["chunks", "list", "--product", "x"])
    assert result.exit_code == 2
    output = result.output
    assert "No such option" in output
    assert "--product" in output
    assert "Did you mean" in output
    assert "--all-products" in output
    assert "Hint:" not in output


def test_valid_flags_on_archive_commands_get_no_hint(subtests, tmp_path) -> None:
    """--target is valid on archive restore and archive create."""
    store = tmp_path / "p.zarr"
    zarr.open_group(str(store), mode="w")
    (store / ".firecube").mkdir()
    (store / ".firecube" / "product.json").write_text('{"product": "LBL"}')
    zip_args = ["--archive-format", "zip"]
    with zipfile.ZipFile(tmp_path / "made.zip", "w") as archive:  # a minimal labelled archive
        archive.writestr("zarr.json", '{"zarr_format": 3, "node_type": "group", "attributes": {}}')
        archive.writestr(".firecube/product.json", '{"product": "LBL"}')
    rows = {
        "target_on_archive_restore_no_hint": (
            [
                "restore",
                "--archive",
                (tmp_path / "made.zip").as_uri(),
                "--target",
                (tmp_path / "restored.zarr").as_uri(),
            ],
            "[dry-run] Would restore:",
        ),
        "target_on_archive_create_no_hint": (
            [
                "create",
                "--target",
                store.as_uri(),
                "--archive",
                (tmp_path / "x.zip").as_uri(),
                *zip_args,
            ],
            "[dry-run] Would archive:",
        ),
    }
    for row_id, (args, message) in rows.items():
        with subtests.test(msg=row_id):
            result = CliRunner().invoke(cli, ["archive", *args, "--dry-run"])
            assert result.exit_code == 0, result.output
            assert message in result.output
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
