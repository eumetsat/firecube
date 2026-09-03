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

import importlib
from collections.abc import Iterator
from pathlib import Path

import pytest
import regular_axis_test_plugin as _plugin_module
from click.testing import CliRunner

from firecube.cli.main import cli
from firecube.ingestor.registry import loader as _loader

pytestmark = pytest.mark.integration

PLUGIN_NAME = "regular_axis_dense_coord"
PRODUCT_NAME = "regular_axis_dense_coord"


@pytest.fixture(autouse=True)
def reset_plugin_registry() -> Iterator[None]:
    original_loaded = _loader._LOADED
    original_registry = dict(_loader.AVAILABLE_INGESTORS)
    _loader._LOADED = False
    _loader.AVAILABLE_INGESTORS.clear()
    importlib.reload(_plugin_module)
    yield
    _loader._LOADED = original_loaded
    _loader.AVAILABLE_INGESTORS.clear()
    _loader.AVAILABLE_INGESTORS.update(original_registry)


def _preallocate_args(target_dir: Path, input_data: str) -> list[str]:
    return [
        "zarr",
        "preallocate",
        PLUGIN_NAME,
        "--target",
        f"file://{target_dir}",
        "--product-name",
        PRODUCT_NAME,
        "--storage-type",
        "local",
        "--storage-driver",
        "fsspec",
        "--write-mode",
        "direct",
        "--option",
        "no_progress=true",
        "--input-data",
        input_data,
    ]


def test_preallocate_input_data_routes_into_plugin_ctx(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target_dir = tmp_path / "out.zarr"
    sentinel = tmp_path / "captured-input-data.txt"
    input_path = tmp_path / "expected-input-path"
    input_path.write_text("present", encoding="utf-8")
    expected_input_data = f"file://{input_path}"

    original_index_spec = _plugin_module.RegularAxisDenseCoordIngestor.index_spec

    def record_input_data(self, ctx):
        sentinel.write_text(getattr(ctx, "source", ""), encoding="utf-8")
        return original_index_spec(self, ctx)

    monkeypatch.setattr(
        _plugin_module.RegularAxisDenseCoordIngestor,
        "index_spec",
        record_input_data,
    )

    result = CliRunner().invoke(
        cli,
        _preallocate_args(target_dir, expected_input_data),
    )

    assert result.exit_code == 0, result.output
    assert sentinel.read_text(encoding="utf-8") == expected_input_data
