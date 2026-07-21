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

import click
from click.testing import CliRunner

from firecube.cli._typed_options import display_format_option, output_path_option


def test_display_format_option_default_table() -> None:
    @click.command()
    @display_format_option(default="table")
    def cmd(output_format: str) -> None:
        click.echo(f"fmt={output_format}")

    result = CliRunner().invoke(cmd, [])
    assert result.exit_code == 0
    assert "fmt=table" in result.output


def test_display_format_option_json_default() -> None:
    @click.command()
    @display_format_option(default="json")
    def cmd(output_format: str) -> None:
        click.echo(f"fmt={output_format}")

    result = CliRunner().invoke(cmd, [])
    assert result.exit_code == 0
    assert "fmt=json" in result.output


def test_display_format_option_help_shows_choices() -> None:
    @click.command()
    @display_format_option()
    def cmd(output_format: str) -> None:
        pass

    result = CliRunner().invoke(cmd, ["--help"])
    assert result.exit_code == 0
    assert "-f" in result.output or "--format" in result.output
    assert "table" in result.output
    assert "json" in result.output
    assert "csv" in result.output


def test_display_format_option_explicit_choice() -> None:
    @click.command()
    @display_format_option()
    def cmd(output_format: str) -> None:
        click.echo(f"fmt={output_format}")

    result = CliRunner().invoke(cmd, ["-f", "csv"])
    assert result.exit_code == 0
    assert "fmt=csv" in result.output


def test_display_format_option_rejects_invalid_choice() -> None:
    @click.command()
    @display_format_option()
    def cmd(output_format: str) -> None:
        pass

    result = CliRunner().invoke(cmd, ["-f", "yaml"])
    assert result.exit_code != 0
    assert "Invalid value for '-f' / '--format'" in result.output
    assert "'yaml' is not one of 'table', 'json', 'csv'" in result.output


def test_output_path_option_required() -> None:
    @click.command()
    @output_path_option(required=True)
    def cmd(output_path: str) -> None:
        pass

    result = CliRunner().invoke(cmd, [])
    assert result.exit_code != 0
    assert "Missing option '-o' / '--output'" in result.output


def test_output_path_option_accepts_value() -> None:
    @click.command()
    @output_path_option(required=True)
    def cmd(output_path: str) -> None:
        click.echo(f"out={output_path}")

    result = CliRunner().invoke(cmd, ["-o", "/tmp/foo.bin"])
    assert result.exit_code == 0
    assert "out=/tmp/foo.bin" in result.output


def test_output_path_option_optional_when_not_required() -> None:
    @click.command()
    @output_path_option(required=False)
    def cmd(output_path: str | None) -> None:
        click.echo(f"out={output_path}")

    result = CliRunner().invoke(cmd, [])
    assert result.exit_code == 0
    assert "out=None" in result.output


def test_output_path_option_help_marker() -> None:
    @click.command()
    @output_path_option(required=True)
    def cmd(output_path: str) -> None:
        pass

    result = CliRunner().invoke(cmd, ["--help"])
    assert result.exit_code == 0
    assert "-o" in result.output or "--output" in result.output


def test_output_path_option_custom_help_text() -> None:
    @click.command()
    @output_path_option(required=False, help_text="custom destination help")
    def cmd(output_path: str | None) -> None:
        pass

    result = CliRunner().invoke(cmd, ["--help"])
    assert result.exit_code == 0
    assert "custom destination help" in result.output
