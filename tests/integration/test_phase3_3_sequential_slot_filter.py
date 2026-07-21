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
from typing import Any, cast

import numpy as np
import pytest
import zarr
from click.testing import CliRunner

from firecube.cli.main import cli
from firecube.ingestor.registry import loader as _loader

pytestmark = pytest.mark.integration

_PLUGIN = "multi_group_capable_test_plugin"
_PRODUCT = "multi_group_capable_test_product"


@pytest.fixture(autouse=True)
def reset_plugin_registry() -> Iterator[None]:
    original_loaded = _loader._LOADED
    original_registry = dict(_loader.AVAILABLE_INGESTORS)
    _loader._LOADED = False
    _loader.AVAILABLE_INGESTORS.clear()
    importlib.reload(importlib.import_module(_PLUGIN))
    yield
    _loader._LOADED = original_loaded
    _loader.AVAILABLE_INGESTORS.clear()
    _loader.AVAILABLE_INGESTORS.update(original_registry)


def _base_args(target_path: Path) -> list[str]:
    return [
        "ingest",
        _PLUGIN,
        "--target",
        f"file://{target_path}",
        "--product-name",
        _PRODUCT,
        "--storage-type",
        "local",
        "--storage-driver",
        "fsspec",
        "--write-mode",
        "direct",
        "--option",
        "no_progress=true",
        "--option",
        "pipeline_batch_size=400",
    ]


def _max_value(target_path: Path, group: str, array: str) -> float:
    root = zarr.open_group(store=str(target_path), mode="r", zarr_format=3)
    group_root = cast(Any, root[group])
    array_obj = cast(Any, group_root[array])
    return float(np.max(np.asarray(array_obj[:])))


def test_default_cli_sequential_honors_slot_range(tmp_path: Path) -> None:
    target_path = tmp_path / "out.zarr"
    args = [*_base_args(target_path), "--slot-start", "0", "--slot-end", "100"]

    result = CliRunner().invoke(cli, args)

    assert result.exit_code == 0, result.output
    assert target_path.exists(), f"Zarr store not created at {target_path}"
    assert _max_value(target_path, "group_a", "primary") < 100
    assert _max_value(target_path, "group_a", "calibration") < 100
    assert _max_value(target_path, "group_b", "primary") < 100


def test_default_cli_sequential_no_slot_unchanged(tmp_path: Path) -> None:
    target_path = tmp_path / "out.zarr"

    result = CliRunner().invoke(cli, _base_args(target_path))

    assert result.exit_code == 0, result.output
    assert target_path.exists(), f"Zarr store not created at {target_path}"
    assert _max_value(target_path, "group_a", "primary") >= 100


def test_default_cli_sequential_misaligned_still_rejected(tmp_path: Path) -> None:
    target_path = tmp_path / "out.zarr"
    args = [*_base_args(target_path), "--slot-start", "0", "--slot-end", "73"]

    result = CliRunner().invoke(cli, args)

    assert result.exit_code != 0, result.output
    combined = (result.output + "\n" + (str(result.exception) if result.exception else "")).lower()
    assert "misaligned" in combined
