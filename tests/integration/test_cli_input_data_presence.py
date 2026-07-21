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

"""Behavior tests for firecube ingest --input-data presence contract.

Covers:
* Default-discovery plugin (cli_test_plugin) missing --input-data → clean ConfigurationError
* Override-discovery plugin (direct_zarr_capable_test_plugin) missing --input-data → succeeds
* --show-options without --input-data → always succeeds (informational path)
* Empty --input-data "" for default discovery → same clean ConfigurationError
* Literal --input-data None → preserved as user-provided path (not omitted-case)
"""

from __future__ import annotations

from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest
from click.testing import CliRunner

from firecube.cli.main import cli
from firecube.ingestor.api import IngestContext, IngestResult, OutputPaths

pytestmark = pytest.mark.integration


def test_default_discovery_missing_input_data_fails_clean(tmp_path: Path) -> None:
    """T3: omitting --input-data on a default-discovery plugin raises ConfigurationError.

    cli_test_plugin does NOT override discover_source_files, so it uses the base
    implementation which (after T9 fix) requires ctx.source to be non-empty.
    """
    target = tmp_path / "out.zarr"
    target.mkdir()

    result = CliRunner().invoke(
        cli,
        [
            "ingest",
            "cli_test_plugin",
            "--target",
            target.as_uri(),
            "--output-format",
            "zarr",
            "--storage-type",
            "local",
            "--storage-driver",
            "fsspec",
            "--write-mode",
            "direct",
            "--product-name",
            "cli_test_product",
            # No --input-data flag
        ],
    )

    assert result.exit_code != 0
    assert "Traceback" not in result.output, (
        f"CLI must render a clean Error: line, not a Python traceback. Got:\n{result.output}"
    )
    assert "Error:" in result.output, (
        f"Click should prefix the error with 'Error:'. Got:\n{result.output}"
    )
    assert "--input-data" in result.output, (
        f"Error message must mention --input-data; got:\n{result.output}"
    )
    assert "discover_source_files" in result.output, (
        f"Error message must hint at discover_source_files override; got:\n{result.output}"
    )


def test_override_discovery_no_input_data_succeeds(tmp_path: Path) -> None:
    """T4: override-discovery plugin (ignores ctx.source) works without --input-data.

    direct_zarr_capable_test_plugin overrides discover_source_files to return
    synthetic data (list(range(200))), so the guard in the base implementation
    is never reached.
    """
    captured: dict[str, IngestContext] = {}
    target = tmp_path / "out.zarr"
    target.mkdir()

    def fake_run(self: Any, ctx: IngestContext) -> IngestResult:
        captured["ctx"] = ctx
        return IngestResult(
            outputs=OutputPaths(primary=str(ctx.target)),
            output_format=str(ctx.output_format),
        )

    with patch(
        "direct_zarr_capable_test_plugin.DirectZarrCapableTestIngestor.run",
        autospec=True,
        side_effect=fake_run,
    ):
        result = CliRunner().invoke(
            cli,
            [
                "ingest",
                "direct_zarr_capable_test_plugin",
                "--target",
                target.as_uri(),
                "--output-format",
                "zarr",
                "--storage-type",
                "local",
                "--storage-driver",
                "fsspec",
                "--write-mode",
                "direct",
                "--product-name",
                "direct_zarr_capable_test_product",
                # No --input-data flag
            ],
        )

    assert result.exit_code == 0, (
        f"Override-discovery plugin should succeed without --input-data; "
        f"got exit_code={result.exit_code} output={result.output!r}"
    )
    assert "ctx" in captured, f"run() was not called; output={result.output!r}"
    assert captured["ctx"].source == "", (
        f"After fix: omitted --input-data should produce source=''; got {captured['ctx'].source!r}"
    )


def test_show_options_without_input_data_succeeds() -> None:
    """T5: --show-options informational path always works without --input-data.

    firecube ingest <plugin> --show-options skips all run-time validation
    (IngestCommandConfig is not constructed when show_options=True).
    """
    result = CliRunner().invoke(
        cli,
        [
            "ingest",
            "cli_test_plugin",
            "--show-options",
        ],
    )

    assert result.exit_code == 0, (
        f"--show-options must succeed without --input-data; "
        f"got exit_code={result.exit_code} output={result.output!r}"
    )
    # Verify we got actual option output (not an empty response)
    assert len(result.output) > 0, "Expected --show-options output to be non-empty"


def test_default_discovery_empty_input_data_fails_clean(tmp_path: Path) -> None:
    """T6: --input-data "" (empty string) treated same as omitted for default discovery.

    Without the guard, Path("") → CWD scan via fsspec. With the guard, it raises
    ConfigurationError before any I/O occurs.
    """
    target = tmp_path / "out.zarr"
    target.mkdir()

    result = CliRunner().invoke(
        cli,
        [
            "ingest",
            "cli_test_plugin",
            "--input-data",
            "",
            "--target",
            target.as_uri(),
            "--output-format",
            "zarr",
            "--storage-type",
            "local",
            "--storage-driver",
            "fsspec",
            "--write-mode",
            "direct",
            "--product-name",
            "cli_test_product",
        ],
    )

    assert result.exit_code != 0
    assert "Traceback" not in result.output, (
        f"CLI must render a clean Error: line, not a Python traceback. Got:\n{result.output}"
    )
    assert "Error:" in result.output, (
        f"Click should prefix the error with 'Error:'. Got:\n{result.output}"
    )
    assert "--input-data" in result.output, (
        f"Error message must mention --input-data; got:\n{result.output}"
    )
    assert "discover_source_files" in result.output, (
        f"Error message must hint at discover_source_files override; got:\n{result.output}"
    )


def test_literal_none_string_preserved_as_user_provided_input(tmp_path: Path) -> None:
    """T7 (SHOULD): --input-data None (literal string) preserved, not collapsed to omitted.

    When a user passes --input-data None, it's the literal path "None" (which doesn't
    exist), NOT the omitted case. The CLI's local-path-not-found check fires before
    discover_source_files, so we see a ClickException, not ConfigurationError.
    """
    target = tmp_path / "out.zarr"

    result = CliRunner().invoke(
        cli,
        [
            "ingest",
            "cli_test_plugin",
            "--input-data",
            "None",
            "--target",
            target.as_uri(),
            "--output-format",
            "zarr",
            "--storage-type",
            "local",
            "--storage-driver",
            "fsspec",
            "--write-mode",
            "direct",
            "--product-name",
            "cli_test_product",
        ],
    )

    assert result.exit_code != 0
    # The error should come from the local-path-not-found check (ClickException),
    # NOT from the ConfigurationError guard
    assert "Input data not found: None" in result.output, (
        f"Expected 'Input data not found: None' in output; got {result.output!r}"
    )
    assert "discover_source_files" not in result.output, (
        "Should NOT reach the discovery guard for user-provided --input-data None"
    )
