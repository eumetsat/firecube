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

"""Regression coverage for preallocate epilog wording on exact-grid reruns."""

from __future__ import annotations

import importlib
from collections.abc import Iterator
from pathlib import Path

import pytest
import regular_axis_test_plugin as _regular_plugin_module
from click.testing import CliRunner

from firecube.cli.main import cli
from firecube.ingestor.registry import loader as _loader

pytestmark = pytest.mark.integration


@pytest.fixture(autouse=True)
def reset_plugin_registry() -> Iterator[None]:
    original_loaded = _loader._LOADED
    original_registry = dict(_loader.AVAILABLE_INGESTORS)
    _loader._LOADED = False
    _loader.AVAILABLE_INGESTORS.clear()
    importlib.reload(_regular_plugin_module)
    yield
    _loader._LOADED = original_loaded
    _loader.AVAILABLE_INGESTORS.clear()
    _loader.AVAILABLE_INGESTORS.update(original_registry)


def _preallocate_args(target_path: Path) -> list[str]:
    return [
        "zarr",
        "preallocate",
        "regular_axis_dense_coord",
        "--target",
        f"file://{target_path}",
        "--product-name",
        "regular_axis_dense_coord",
        "--storage-type",
        "local",
        "--storage-driver",
        "fsspec",
        "--write-mode",
        "direct",
        "--option",
        "no_progress=true",
    ]


def test_preallocate_rerun_logs_no_op_for_matching_arrays(tmp_path: Path) -> None:
    target_path = tmp_path / "cube.zarr"
    runner = CliRunner()

    first = runner.invoke(cli, _preallocate_args(target_path))
    second = runner.invoke(cli, _preallocate_args(target_path))

    assert first.exit_code == 0, first.output
    assert second.exit_code == 0, second.output
    assert "no-op (matches nominal grid)" in second.output
