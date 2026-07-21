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

"""Architecture tombstones for removed CLI inference behavior."""

from __future__ import annotations

import subprocess
from pathlib import Path
from typing import Any
from unittest.mock import patch

import click
import pytest
from click.testing import CliRunner

from firecube.cli._command_schemas import IngestCommandConfig
from firecube.cli._uri_policy import apply_smart_default, parse_product_uri
from firecube.cli.main import cli
from firecube.core.product.identity import ProductIdentity
from firecube.core.storage.uri import StorageUri
from firecube.ingestor.api import IngestContext, IngestResult, OutputPaths


def test_product_name_is_explicit_not_uri_basename() -> None:
    """ProductIdentity uses the provided product name, never the URI basename."""
    uri = StorageUri.parse("s3://bucket/data/from-uri-basename.zarr")

    identity = ProductIdentity.from_uri(uri, "zarr", product_name="configured_product")

    assert identity.product_name == "configured_product"
    assert identity.product_name != "from-uri-basename.zarr"
    with pytest.raises(ValueError, match="product_name is required"):
        ProductIdentity.from_uri(uri, "zarr", product_name="")


def test_storage_type_smart_default_uses_uri_scheme_mapping() -> None:
    """Inspect-tier storage defaults come from the explicit scheme map."""
    assert apply_smart_default(parse_product_uri("file:///tmp/product.zarr"), None) == "local"
    assert apply_smart_default(parse_product_uri("s3://bucket/product.zarr"), None) == "s3"

    with pytest.raises(click.UsageError, match="incompatible with URI scheme 'file'"):
        apply_smart_default(parse_product_uri("file:///tmp/product.zarr"), "s3")


def test_write_mode_required_not_inferred_from_local_target() -> None:
    """A local file target still requires explicit --write-mode."""
    with pytest.raises(click.UsageError, match="No inference from target locality"):
        IngestCommandConfig(
            plugin="cli_test_plugin",
            input_data=None,
            target="file:///tmp/local-product.zarr",
            write_mode=None,
            storage_type="local",
            storage_driver="fsspec",
            product_name="configured_product",
        )


def test_no_default_output_name_in_source() -> None:
    """Source must not reference the legacy default_output_name config key.

    The rejection logic in ``src/firecube/core/config.py`` is the single
    authorised reference site (it raises ``click.UsageError`` pointing at the
    new ``default_product_name`` key); all other references must be removed.
    """
    result = subprocess.run(
        [
            "grep",
            "-rn",
            "--exclude-dir=__pycache__",
            "--exclude=config.py",
            "default_output_name",
            "src/firecube/",
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.stdout.strip() == "", f"default_output_name still in source:\n{result.stdout}"


def test_omitted_input_data_does_not_become_literal_none(tmp_path: Path) -> None:
    """Omitting --input-data must never fabricate ctx.source = 'None'.

    cli/main.py used source=str(input_data) which produces the literal string "None"
    when input_data is None (omitted). This tombstone asserts that omitted --input-data
    produces ctx.source == "" (the safe sentinel), never "None".
    """
    captured: dict[str, IngestContext] = {}
    target = tmp_path / "no-source-product.zarr"
    target.mkdir()

    def fake_run(self: Any, ctx: IngestContext) -> IngestResult:
        captured["ctx"] = ctx
        return IngestResult(
            outputs=OutputPaths(primary=str(ctx.target)),
            output_format=str(ctx.output_format),
        )

    with patch("cli_test_plugin.CliTestIngestor.run", autospec=True, side_effect=fake_run):
        CliRunner().invoke(
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
            ],
        )

    captured_ctx = captured.get("ctx")
    assert captured_ctx is not None, "run() was not called — check plugin discovery"
    assert captured_ctx.source != "None", (
        f"omitted --input-data must never fabricate source='None'; got {captured_ctx.source!r}"
    )
    assert captured_ctx.source == "", (
        f"omitted --input-data should produce source=''; got {captured_ctx.source!r}"
    )
