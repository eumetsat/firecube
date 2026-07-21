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

import click
from click.testing import CliRunner

from firecube.cli._shared_options import (
    config_file_option,
    dry_run_flag,
    format_option,
    product_filter_option,
    product_name_option,
    storage_driver_option,
    storage_type_option,
    workspace_option,
    yes_flag,
)


def test_workspace_option_help_and_path_type() -> None:
    @click.command()
    @workspace_option
    def cmd(workspace: Path | None) -> None:
        click.echo(type(workspace).__name__ if workspace is not None else "none")

    help_result = CliRunner().invoke(cmd, ["--help"])
    assert "--workspace PATH" in help_result.output
    assert "workspace directory override" in help_result.output

    run_result = CliRunner().invoke(cmd, ["--workspace", "/tmp/firecube"])
    assert run_result.exit_code == 0
    assert "PosixPath" in run_result.output


def test_config_file_option_help_and_path_type() -> None:
    @click.command()
    @config_file_option
    def cmd(config_file: Path | None) -> None:
        click.echo(type(config_file).__name__ if config_file is not None else "none")

    help_result = CliRunner().invoke(cmd, ["--help"])
    assert "--config-file PATH" in help_result.output
    assert "Firecube TOML config file" in help_result.output

    run_result = CliRunner().invoke(cmd, ["--config-file", "/tmp/config.toml"])
    assert run_result.exit_code == 0
    assert "PosixPath" in run_result.output


def test_storage_type_option_choices_required_and_case_insensitive() -> None:
    @click.command()
    @storage_type_option
    def cmd(storage_type: str) -> None:
        click.echo(storage_type)

    help_result = CliRunner().invoke(cmd, ["--help"])
    assert "--storage-type [local|s3]" in help_result.output
    assert "Storage locality/class." in help_result.output
    assert "[required]" in help_result.output

    missing_result = CliRunner().invoke(cmd, [])
    assert missing_result.exit_code != 0
    assert "Missing option '--storage-type'" in missing_result.output

    upper_result = CliRunner().invoke(cmd, ["--storage-type", "S3"])
    assert upper_result.exit_code == 0
    assert "s3" in upper_result.output


def test_storage_driver_option_choices_required_and_case_insensitive() -> None:
    @click.command()
    @storage_driver_option
    def cmd(storage_driver: str) -> None:
        click.echo(storage_driver)

    help_result = CliRunner().invoke(cmd, ["--help"])
    assert "--storage-driver [fsspec|obstore]" in help_result.output
    assert "Storage backend driver." in help_result.output
    assert "[required]" in help_result.output

    missing_result = CliRunner().invoke(cmd, [])
    assert missing_result.exit_code != 0
    assert "Missing option '--storage-driver'" in missing_result.output

    upper_result = CliRunner().invoke(cmd, ["--storage-driver", "OBSTORE"])
    assert upper_result.exit_code == 0
    assert "obstore" in upper_result.output


def test_format_option_default_table() -> None:
    @click.command()
    @format_option(default="table")
    def cmd(output_format: str) -> None:
        click.echo(output_format)

    help_result = CliRunner().invoke(cmd, ["--help"])
    assert "-f, --format [table|json|csv]" in help_result.output
    assert "[default: table]" in help_result.output
    assert "Output format." in help_result.output

    run_result = CliRunner().invoke(cmd, [])
    assert run_result.exit_code == 0
    assert "table" in run_result.output


def test_format_option_accepts_non_table_default() -> None:
    @click.command()
    @format_option(default="json")
    def cmd(output_format: str) -> None:
        click.echo(output_format)

    help_result = CliRunner().invoke(cmd, ["--help"])
    assert "[default: json]" in help_result.output

    run_result = CliRunner().invoke(cmd, [])
    assert run_result.exit_code == 0
    assert "json" in run_result.output


def test_product_filter_option_optional_by_default() -> None:
    @click.command()
    @product_filter_option()
    def cmd(product_name: str | None) -> None:
        click.echo(product_name or "none")

    help_result = CliRunner().invoke(cmd, ["--help"])
    assert "-n, --product-name TEXT" in help_result.output
    assert "Filter by product name." in help_result.output
    assert "[required]" not in help_result.output

    run_result = CliRunner().invoke(cmd, [])
    assert run_result.exit_code == 0
    assert "none" in run_result.output


def test_product_filter_option_can_be_required() -> None:
    @click.command()
    @product_filter_option(required=True)
    def cmd(product_name: str) -> None:
        click.echo(product_name)

    result = CliRunner().invoke(cmd, [])
    assert result.exit_code != 0
    assert "Missing option '-n' / '--product-name'" in result.output


def test_product_name_option_required_by_default() -> None:
    @click.command()
    @product_name_option()
    def cmd(product_name: str) -> None:
        click.echo(product_name)

    help_result = CliRunner().invoke(cmd, ["--help"])
    assert "--product-name TEXT" in help_result.output
    assert "Logical product name." in help_result.output
    assert "[required]" in help_result.output

    result = CliRunner().invoke(cmd, [])
    assert result.exit_code != 0
    assert "Missing option '--product-name'" in result.output


def test_product_name_option_can_be_optional() -> None:
    @click.command()
    @product_name_option(required=False)
    def cmd(product_name: str | None) -> None:
        click.echo(product_name or "none")

    result = CliRunner().invoke(cmd, [])
    assert result.exit_code == 0
    assert "none" in result.output


def test_dry_run_flag_no_arg() -> None:
    @click.command()
    @dry_run_flag
    def cmd(dry_run: bool) -> None:
        click.echo(f"dry_run={dry_run}")

    help_result = CliRunner().invoke(cmd, ["--help"])
    assert "--dry-run" in help_result.output
    assert "Show what would happen without making any changes." in help_result.output

    false_result = CliRunner().invoke(cmd, [])
    assert false_result.exit_code == 0
    assert "dry_run=False" in false_result.output

    true_result = CliRunner().invoke(cmd, ["--dry-run"])
    assert true_result.exit_code == 0
    assert "dry_run=True" in true_result.output


def test_yes_flag_exact_name() -> None:
    @click.command()
    @yes_flag
    def cmd(yes_i_really_mean_it: bool) -> None:
        click.echo(f"yes={yes_i_really_mean_it}")

    help_result = CliRunner().invoke(cmd, ["--help"])
    assert "--yes-i-really-mean-it" in help_result.output
    assert "Skip confirmation prompts. Required for destructive" in help_result.output
    assert "operations in non-TTY." in help_result.output

    run_result = CliRunner().invoke(cmd, ["--yes-i-really-mean-it"])
    assert run_result.exit_code == 0
    assert "yes=True" in run_result.output


def test_all_decorators_exported() -> None:
    from firecube.cli._shared_options import __all__

    assert __all__ == [
        "archive_uri_option",
        "config_file_option",
        "dry_run_flag",
        "format_option",
        "product_filter_option",
        "product_name_option",
        "product_uri_option",
        "storage_driver_option",
        "storage_type_option",
        "target_uri_option",
        "workspace_option",
        "write_mode_option",
        "yes_flag",
    ]
