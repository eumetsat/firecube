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

"""CLI regression test for unbounded resolved-index rebuild failures."""

from __future__ import annotations

from pathlib import Path

import pytest
from click.testing import CliRunner

from firecube.cli.main import cli

pytestmark = pytest.mark.integration


def _args(target: Path) -> list[str]:
    return [
        "zarr",
        "index",
        "rebuild",
        "--target",
        f"file://{target}",
        "--plugin",
        "regular_axis_unbounded",
        "--product-name",
        "regular_axis_unbounded_product",
    ]


def test_rebuild_unbounded_axis_fails_cleanly(tmp_path: Path) -> None:
    target = tmp_path / "unbounded.zarr"
    target.mkdir()

    result = CliRunner().invoke(cli, _args(target))

    assert result.exit_code == 1, f"stdout={result.stdout!r} stderr={result.stderr!r}"
    assert "failed to create resolved index record" in result.output
    assert "unbounded group(s) 'data'" in result.output
    assert "Traceback" not in result.output
