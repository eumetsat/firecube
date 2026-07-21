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
from typing import Any, cast

import pytest
import zarr
from click.testing import CliRunner

from firecube.cli.main import cli

pytestmark = pytest.mark.unit


def _args(target: str) -> list[str]:
    return [
        "zarr",
        "preallocate",
        "direct_zarr_capable_test_plugin",
        "--product-name",
        "direct_zarr_capable_test_product",
        "--target",
        target,
        "--storage-type",
        "local",
        "--storage-driver",
        "fsspec",
        "--write-mode",
        "staged",
    ]


def test_preallocate_preserves_static_array_shape(tmp_path: Path) -> None:
    """TDD-RED today (bug at cli/zarr.py:699); GREEN after T8 guard is applied."""
    runner = CliRunner()
    result = runner.invoke(cli, _args(f"file://{tmp_path}"))

    assert result.exit_code == 0, f"CLI failed: {result.output}"

    root = zarr.open_group(str(tmp_path), mode="r", zarr_format=3)
    assert cast(Any, root["data/data"]).shape == (1000, 10)
    assert cast(Any, root["data/lat"]).shape == (10,)


def test_preallocate_substitutes_time_indexed_shape(tmp_path: Path) -> None:
    """GREEN baseline — time-indexed substitution must work after T8 fix."""
    runner = CliRunner()
    result = runner.invoke(cli, _args(f"file://{tmp_path}"))

    assert result.exit_code == 0, f"CLI failed: {result.output}"

    root = zarr.open_group(str(tmp_path), mode="r", zarr_format=3)
    assert cast(Any, root["data/data"]).shape == (1000, 10)
