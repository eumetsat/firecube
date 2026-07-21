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
from unittest.mock import patch

import pytest
from click.testing import CliRunner

from firecube.cli.main import cli
from firecube.ingestor.api import IngestContext, IngestResult, OutputPaths


def _base_args(tmp_path: Path) -> list[str]:
    config = tmp_path / "config.toml"
    config.write_text("", encoding="utf-8")
    return [
        "--config-file",
        str(config),
        "ingest",
        "cli_test_plugin",
    ]


def _required_args(tmp_path: Path) -> list[str]:
    return [
        *_base_args(tmp_path),
        "--target",
        (tmp_path / "qa.zarr").as_uri(),
        "--product-name",
        "qa",
        "--storage-type",
        "local",
        "--storage-driver",
        "fsspec",
        "--write-mode",
        "direct",
    ]


def test_missing_required_ingest_flags_are_reported_together(tmp_path: Path) -> None:
    result = CliRunner().invoke(cli, _base_args(tmp_path))

    assert result.exit_code == 2, result.output
    assert "Invalid ingest configuration" in result.output
    assert "--target" in result.output
    assert "--write-mode" in result.output


def test_all_required_ingest_flags_reach_ingestor(tmp_path: Path) -> None:
    captured: dict[str, IngestContext] = {}

    def fake_run(self: Any, ctx: IngestContext) -> IngestResult:
        captured["ctx"] = ctx
        return IngestResult(
            outputs=OutputPaths(primary=str(ctx.target)),
            output_format=str(ctx.output_format),
        )

    with patch("cli_test_plugin.CliTestIngestor.run", autospec=True, side_effect=fake_run):
        result = CliRunner().invoke(cli, _required_args(tmp_path))

    assert result.exit_code == 0, result.output
    assert "ctx" in captured
    ctx = captured["ctx"]
    assert ctx.target == (tmp_path / "qa.zarr").as_uri()
    assert ctx.output_format == "zarr"
    assert ctx.options["write_mode"] == "direct"
    assert ctx.storage is not None
    assert ctx.storage.output is not None
    assert ctx.storage.output.product.product_name == "qa"
    assert ctx.storage.output.product.product_uri.to_str() == (tmp_path / "qa.zarr").as_uri()
    assert ctx.storage.output.driver.driver == "fsspec"


def test_empty_product_name_is_rejected_at_config_boundary(tmp_path: Path) -> None:
    args = _required_args(tmp_path)
    args[args.index("qa")] = ""

    result = CliRunner().invoke(cli, args)

    assert result.exit_code == 2, result.output
    assert "--product-name was provided but is empty" in result.output


def test_unknown_option_key_reports_plugin_and_valid_keys(tmp_path: Path) -> None:
    result = CliRunner().invoke(
        cli,
        [*_required_args(tmp_path), "--option", "totally_unknown=1"],
    )

    assert result.exit_code == 2, result.output
    assert "totally_unknown" in result.output
    assert "cli_test_plugin" in result.output
    assert "Valid keys" in result.output
    assert "test_int" in result.output
    assert "test_str" in result.output
    assert "test_bool" in result.output


def test_valid_option_value_is_coerced_before_ingest(tmp_path: Path) -> None:
    captured: dict[str, IngestContext] = {}

    def fake_run(self: Any, ctx: IngestContext) -> IngestResult:
        captured["ctx"] = ctx
        return IngestResult(
            outputs=OutputPaths(primary=str(ctx.target)),
            output_format=str(ctx.output_format),
        )

    with patch("cli_test_plugin.CliTestIngestor.run", autospec=True, side_effect=fake_run):
        result = CliRunner().invoke(
            cli,
            [*_required_args(tmp_path), "--option", "test_int=42"],
        )

    assert result.exit_code == 0, result.output
    assert captured["ctx"].options["test_int"] == 42


def test_output_format_defaults_to_zarr_when_flag_omitted(tmp_path: Path) -> None:
    """IngestCommandConfig.output_format default ('zarr') must flow into IngestContext when --output-format is omitted (regression guard: the CLI must not drop the typed default by using the raw Click param)."""
    captured: dict[str, IngestContext] = {}

    def fake_run(self: Any, ctx: IngestContext) -> IngestResult:
        captured["ctx"] = ctx
        return IngestResult(
            outputs=OutputPaths(primary=str(ctx.target)),
            output_format=str(ctx.output_format),
        )

    args = _required_args(tmp_path)
    assert "--output-format" not in args, (
        "test precondition: --output-format must be omitted to exercise the default"
    )

    with patch("cli_test_plugin.CliTestIngestor.run", autospec=True, side_effect=fake_run):
        result = CliRunner().invoke(cli, args)

    assert result.exit_code == 0, result.output
    assert "ctx" in captured
    assert captured["ctx"].output_format == "zarr", (
        f"expected typed-config default 'zarr' to flow into IngestContext, "
        f"got {captured['ctx'].output_format!r}"
    )


@pytest.mark.parametrize(
    ("target", "storage_type", "expected_substrings"),
    [
        ("file:///tmp/x.zarr", "s3", ["file", "s3"]),
        ("s3://bucket/x.zarr", "local", ["s3", "local"]),
        ("gs://bucket/x.zarr", "local", ["not supported", "unsupported"]),
    ],
)
def test_mismatch_target_and_storage_type_is_rejected(
    tmp_path: Path,
    target: str,
    storage_type: str,
    expected_substrings: list[str],
) -> None:
    """Regression guard for commit dda65ab: CLI rejects incoherent target URI scheme + --storage-type combinations at config-validation time (no I/O), with an informative error covering scheme and storage-type names (or the unsupported-scheme phrasing)."""
    args = [
        *_base_args(tmp_path),
        "--target",
        target,
        "--product-name",
        "qa",
        "--storage-type",
        storage_type,
        "--storage-driver",
        "fsspec",
        "--write-mode",
        "direct",
    ]

    result = CliRunner().invoke(cli, args)

    assert result.exit_code != 0, result.output
    output_lower = result.output.lower()
    is_unsupported_case = any("supported" in s for s in expected_substrings)
    if is_unsupported_case:
        assert any(s.lower() in output_lower for s in expected_substrings), result.output
    else:
        for substring in expected_substrings:
            assert substring.lower() in output_lower, result.output


def test_coherent_s3_target_and_s3_storage_type_passes_validation(tmp_path: Path) -> None:
    """Negative control for test_mismatch_target_and_storage_type_is_rejected: a coherent s3://+--storage-type s3 invocation passes URI/storage-type validation and reaches the (mocked) ingestor."""
    args = [
        *_base_args(tmp_path),
        "--target",
        "s3://bucket/qa.zarr",
        "--product-name",
        "qa",
        "--storage-type",
        "s3",
        "--storage-driver",
        "fsspec",
        "--write-mode",
        "direct",
    ]

    def fake_run(self: Any, ctx: IngestContext) -> IngestResult:
        return IngestResult(
            outputs=OutputPaths(primary=str(ctx.target)),
            output_format=str(ctx.output_format),
        )

    with patch("cli_test_plugin.CliTestIngestor.run", autospec=True, side_effect=fake_run):
        result = CliRunner().invoke(cli, args)

    assert "incompatible with --storage-type" not in result.output, result.output
    assert "is not supported" not in result.output, result.output
    assert result.exit_code == 0, result.output
