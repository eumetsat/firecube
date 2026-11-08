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

"""Ingest accepts the shared storage/write flags at the Click boundary."""

from __future__ import annotations

from pathlib import Path
from typing import Any
from unittest.mock import patch

import click
from click.testing import CliRunner

from firecube.cli.main import cli, ingest
from firecube.ingestor.api import IngestContext, IngestResult, OutputPaths
from tests.helpers.rows import table_row


def _ingest_option(name: str) -> click.Option:
    for param in ingest.params:
        if isinstance(param, click.Option) and param.name == name:
            return param
    raise AssertionError(f"--{name.replace('_', '-')} not registered on ingest command")


_ROWS = [
    (
        "test_ingest_storage_type_uses_shared_decorator_case_insensitive",
        "storage_type",
        ("local", "s3"),
    ),
    (
        "test_ingest_storage_driver_uses_shared_decorator_case_insensitive",
        "storage_driver",
        ("fsspec", "obstore"),
    ),
    (
        "test_ingest_write_mode_uses_shared_decorator_case_insensitive",
        "write_mode",
        ("staged", "direct"),
    ),
]


def test_ingest_shared_decorator_options_are_case_insensitive(
    subtests: Any, tmp_path: Path
) -> None:
    """The shared flags on ``firecube ingest`` are case-insensitive, non-required Choices.

    They stay non-required so ``IngestCommandConfig.__post_init__`` can
    aggregate all missing-flag errors at once.
    """
    for row_id, name, choices in _ROWS:
        with table_row(subtests, row_id, tmp_path):
            option = _ingest_option(name)
            assert isinstance(option.type, click.Choice)
            assert tuple(option.type.choices) == choices
            assert option.type.case_sensitive is False
            assert option.required is False


def test_ingest_accepts_uppercase_storage_flags(tmp_path: Path) -> None:
    """End-to-end: ``--storage-type LOCAL --storage-driver FSSPEC --write-mode STAGED``
    must not raise ``Invalid value`` at the Click parsing boundary.
    """
    captured: dict[str, IngestContext] = {}

    def fake_run(self: Any, ctx: IngestContext) -> IngestResult:
        captured["ctx"] = ctx
        return IngestResult(
            outputs=OutputPaths(primary=str(ctx.target)),
            output_format=str(ctx.output_format),
        )

    config = tmp_path / "config.toml"
    config.write_text("", encoding="utf-8")

    args = [
        "--config-file",
        str(config),
        "ingest",
        "cli_test_plugin",
        "--target",
        (tmp_path / "qa.zarr").as_uri(),
        "--product-name",
        "qa",
        "--storage-type",
        "LOCAL",
        "--storage-driver",
        "FSSPEC",
        "--write-mode",
        "STAGED",
    ]

    with patch("cli_test_plugin.CliTestIngestor.run", autospec=True, side_effect=fake_run):
        result = CliRunner().invoke(cli, args)

    assert "Invalid value" not in result.output, result.output
    assert "is not one of" not in result.output, result.output
    assert result.exit_code == 0, result.output
    assert captured["ctx"].execution.write_mode == "staged", (
        "Click normalises Choice values to canonical (lowercase) form when "
        "case_sensitive=False, so downstream code must keep seeing 'staged'"
    )
