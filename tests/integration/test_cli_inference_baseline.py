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

from click.testing import CliRunner

from firecube.cli.main import cli
from firecube.ingestor.api import IngestContext, IngestResult, OutputPaths


def _run_ingest(
    tmp_path: Path,
    target: str,
    *extra_args: str,
) -> tuple[Any, IngestContext]:
    source = tmp_path / "source"
    source.mkdir()
    config = tmp_path / "missing-config.toml"
    config.touch()
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
            [
                "--config-file",
                str(config),
                "ingest",
                "cli_test_plugin",
                "--input-data",
                str(source),
                "--target",
                target,
                "--output-format",
                "zarr",
                "--storage-type",
                "local" if target.startswith("file://") else "s3",
                "--storage-driver",
                "fsspec",
                "--write-mode",
                "direct",
                *extra_args,
            ],
        )

    assert "ctx" in captured, result.output
    return result, captured["ctx"]


def test_file_target_without_product_name_uses_plugin_product_name(tmp_path: Path) -> None:
    result, ctx = _run_ingest(tmp_path, (tmp_path / "inferred-product.zarr").as_uri())

    assert result.exit_code == 0, result.output
    assert ctx.storage is not None
    assert ctx.storage.output is not None
    assert ctx.storage.output.product.product_name == "cli_test_product"
