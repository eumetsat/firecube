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

from unittest.mock import patch

import pytest
from click.testing import CliRunner

from firecube.cli.main import cli
from firecube.core.config import StorageConfig
from firecube.core.storage.uri import StorageUri
from firecube.core.zarr.multires import ZarrMultiresBuilder


def test_multires_subcommand_registered() -> None:
    result = CliRunner().invoke(cli, ["zarr", "--help"])

    assert result.exit_code == 0
    assert "multires" in result.output


def test_multires_help() -> None:
    result = CliRunner().invoke(cli, ["zarr", "multires", "--help"])

    assert result.exit_code == 0
    assert len(result.output) > 0
    assert "Build multi-resolution Zarr pyramid" in result.output


def test_multires_rejects_missing_target() -> None:
    result = CliRunner().invoke(cli, ["zarr", "multires"])

    assert result.exit_code != 0
    assert "Missing option '-t' / '--target'" in result.output


def test_multires_rejects_missing_product_name() -> None:
    result = CliRunner().invoke(
        cli,
        [
            "zarr",
            "multires",
            "--target",
            "file:///tmp/fake.zarr",
            "--storage-type",
            "local",
            "--storage-driver",
            "fsspec",
        ],
    )

    assert result.exit_code != 0
    assert "--product-name" in result.output


def test_multires_invokes_builder() -> None:
    with patch("firecube.core.zarr.multires.ZarrMultiresBuilder.build") as mock_build:
        mock_build.return_value = {"layers": ["0.5"]}

        result = CliRunner().invoke(
            cli,
            [
                "zarr",
                "multires",
                "--target",
                "file:///tmp/fake.zarr",
                "--product-name",
                "fake.zarr",
                "--storage-type",
                "local",
                "--storage-driver",
                "fsspec",
                "--resolutions",
                "0.5",
            ],
        )

    assert result.exit_code == 0, result.output
    mock_build.assert_called_once()
    cfg = mock_build.call_args.args[0]
    assert cfg.product == "fake.zarr"
    assert cfg.group == ""
    assert cfg.resolutions == [0.5]


def test_multires_cli_passes_explicit_product_name_distinct_from_basename() -> None:
    """CLI uses --product-name verbatim; URI basename is never inspected."""
    with patch("firecube.core.zarr.multires.ZarrMultiresBuilder.build") as mock_build:
        mock_build.return_value = {"layers": ["0.5"]}

        result = CliRunner().invoke(
            cli,
            [
                "zarr",
                "multires",
                "--target",
                "file:///data/store_v2.zarr",
                "--product-name",
                "logical_name",
                "--storage-type",
                "local",
                "--storage-driver",
                "fsspec",
                "--resolutions",
                "0.5",
            ],
        )

    assert result.exit_code == 0, result.output
    cfg = mock_build.call_args.args[0]
    assert cfg.product == "logical_name"


def test_multires_builder_store_uri_uses_explicit_product_name() -> None:
    """Regression guard: URI basename must NOT influence store-URI resolution."""
    storage_config = StorageConfig(storage_type="local", storage_driver="fsspec")
    product_uri = StorageUri.parse("file:///data/store_v2.zarr")
    explicit_name = "logical_name"

    builder = ZarrMultiresBuilder(
        storage_config,
        product_name=explicit_name,
        product_uri=product_uri,
    )

    assert builder._store_uri_for(explicit_name) == "file:///data/store_v2.zarr"

    sibling_uri = builder._store_uri_for("other_product")
    assert sibling_uri == "file:///data/other_product"

    matched_basename = builder._store_uri_for("store_v2.zarr")
    assert matched_basename == "file:///data/store_v2.zarr"


def test_multires_builder_rejects_empty_product_name() -> None:
    storage_config = StorageConfig(storage_type="local", storage_driver="fsspec")
    product_uri = StorageUri.parse("file:///data/store.zarr")

    with pytest.raises(ValueError, match="product_name"):
        ZarrMultiresBuilder(
            storage_config,
            product_name="",
            product_uri=product_uri,
        )
