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
from firecube.core.runtime import resolve_slot_window
from firecube.ingestor.api import ExecutionParams, IngestContext, IngestResult, OutputPaths


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


def test_slot_group_default_none() -> None:
    assert ExecutionParams().slot_group is None


def test_slot_group_valid_string_accepted() -> None:
    execution = ExecutionParams(slot_group="data_1km")

    assert execution.slot_group == "data_1km"


def test_slot_group_empty_string_fails() -> None:
    with pytest.raises(ValueError, match="non-empty string"):
        ExecutionParams(slot_group="")


def test_slot_group_whitespace_only_fails() -> None:
    with pytest.raises(ValueError, match="non-empty string"):
        ExecutionParams(slot_group="   ")


def test_cli_passes_slot_group_to_execution_params(tmp_path: Path) -> None:
    captured: dict[str, IngestContext] = {}

    def fake_run(self: Any, ctx: IngestContext) -> IngestResult:
        captured["ctx"] = ctx
        assert ctx.execution.slot_group == "data_1km"
        return IngestResult(
            outputs=OutputPaths(primary=str(ctx.target)),
            output_format=str(ctx.output_format),
        )

    with patch("cli_test_plugin.CliTestIngestor.run", autospec=True, side_effect=fake_run):
        result = CliRunner().invoke(
            cli,
            [*_required_args(tmp_path), "--slot-group", "data_1km"],
        )

    assert result.exit_code == 0, result.output
    assert "ctx" in captured
    assert captured["ctx"].execution.slot_group == "data_1km"
    assert "slot_group" not in captured["ctx"].options


def test_env_slot_group_used_when_cli_absent() -> None:
    env = {"FIRECUBE_SLOT_GROUP": "data_1km"}

    assert resolve_slot_window(None, None, None, None, env=env) == (
        None,
        None,
        "data_1km",
    )


def test_cli_slot_group_overrides_env() -> None:
    env = {"FIRECUBE_SLOT_GROUP": "from_env"}

    assert resolve_slot_window(None, None, None, "from_cli", env=env) == (
        None,
        None,
        "from_cli",
    )
