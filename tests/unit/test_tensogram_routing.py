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

"""CLI routing tests for Tensogram output selection."""

from __future__ import annotations

from typing import Any, ClassVar

import pytest
import xarray as xr
from click.testing import CliRunner

from firecube.cli.main import cli
from firecube.ingestor.api import (
    GenericParquetIngestor,
    GenericTensogramIngestor,
    GenericZarrIngestor,
    IngestContext,
    IngestResult,
    OutputPaths,
    PipelineBatch,
    PluginContext,
)
from tests.helpers.storage import make_test_context


def _ingest_args(tmp_path, plugin: str) -> list[str]:
    return [
        "ingest",
        plugin,
        "--input-data",
        f"file://{tmp_path}",
        "--target",
        f"file://{tmp_path / 'out.tgm'}",
        "--product-name",
        "test-product",
        "--storage-type",
        "local",
        "--storage-driver",
        "fsspec",
        "--write-mode",
        "direct",
    ]


class DirectTensogramProducer(GenericTensogramIngestor):
    PRODUCT_NAME: ClassVar[str] = "direct-tensogram-producer"

    def build_dataset(self, group: str, items: list[Any], ctx: PluginContext) -> xr.Dataset | None:
        return xr.Dataset()

    def run(self, ctx: IngestContext) -> IngestResult:
        return IngestResult(
            outputs=OutputPaths(primary=str(ctx.target)),
            output_format=str(ctx.output_format),
        )


class ZarrDatasetProducer(GenericZarrIngestor):
    PRODUCT_NAME: ClassVar[str] = "zarr-dataset-producer"
    seen_class_name = ""

    def build_dataset(self, group: str, items: list[Any], ctx: PluginContext) -> xr.Dataset | None:
        return xr.Dataset()

    def run(self, ctx: IngestContext) -> IngestResult:
        type(self).seen_class_name = type(self).__name__
        return IngestResult(
            outputs=OutputPaths(primary=str(ctx.target)),
            output_format=str(ctx.output_format),
        )


def test_direct_generic_tensogram_subclass_is_cli_routable(tmp_path, monkeypatch):
    monkeypatch.setattr(
        "firecube.cli.main.discover_ingestors",
        lambda: {"direct_tgm": DirectTensogramProducer},
    )

    result = CliRunner().invoke(cli, _ingest_args(tmp_path, "direct_tgm"))

    assert result.exit_code == 0, result.output
    assert '"output_format": "tensogram"' in result.output


def test_zarr_ingestor_runs_as_its_own_class_through_the_cli(tmp_path, monkeypatch):
    ZarrDatasetProducer.seen_class_name = ""
    monkeypatch.setattr(
        "firecube.cli.main.discover_ingestors",
        lambda: {"zarr_producer": ZarrDatasetProducer},
    )

    result = CliRunner().invoke(cli, _ingest_args(tmp_path, "zarr_producer"))

    assert result.exit_code == 0, result.output
    assert ZarrDatasetProducer.seen_class_name == "ZarrDatasetProducer"
    assert not ZarrDatasetProducer.seen_class_name.endswith("Tensogram")


class ParquetDatasetProducer(GenericParquetIngestor):
    PRODUCT_NAME: ClassVar[str] = "parquet-dataset-producer"

    def build_dataset(  # pyright: ignore[reportIncompatibleMethodOverride]
        self, group: str, batch: PipelineBatch, ctx: PluginContext
    ) -> Any | None:
        return None


def test_parquet_plugin_refuses_a_context_naming_tensogram(tmp_path):
    from firecube.ingestor.errors import ConfigurationError

    ctx = make_test_context(tmp_path, product="out.parquet", format="parquet")
    ctx.output_format = "tensogram"

    with pytest.raises(ConfigurationError, match="writes 'parquet'"):
        ParquetDatasetProducer().run(ctx)
