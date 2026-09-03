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

"""End-date-bounded ``RegularTimeAxis`` preallocate coverage."""

from __future__ import annotations

import importlib
from collections.abc import Iterator
from pathlib import Path
from typing import Any, cast

import numpy as np
import pytest
import regular_axis_test_plugin as _regular_plugin_module
import zarr
from click.testing import CliRunner

from firecube.cli.main import cli
from firecube.core.zarr._sealing_markers import ATTR_PREALLOCATED
from firecube.ingestor.registry import loader as _loader

pytestmark = pytest.mark.integration

_PLUGIN_NAME = "regular_axis_end_date"
_PRODUCT_NAME = "regular_axis_end_date"
_GROUP = "data"
_COORD = "time"
_SLOT_COUNT = 1000
_EXPECTED_CHUNK_LEN = 256
_EXPECTED_VALUES = np.datetime64("2024-01-01T00:00:00", "ns") + np.arange(
    _SLOT_COUNT, dtype=np.int64
) * np.timedelta64(600, "s").astype("timedelta64[ns]")


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


def _preallocate_args(target_path: Path, *, dry_run: bool = False) -> list[str]:
    args = [
        "zarr",
        "preallocate",
        _PLUGIN_NAME,
        "--target",
        f"file://{target_path}",
        "--product-name",
        _PRODUCT_NAME,
        "--storage-type",
        "local",
        "--storage-driver",
        "fsspec",
        "--write-mode",
        "direct",
        "--option",
        "no_progress=true",
    ]
    if dry_run:
        args.append("--dry-run")
    return args


def _root(target_path: Path) -> Any:
    return zarr.open_group(store=str(target_path), mode="r", zarr_format=3)


@pytest.mark.parametrize("dry_run", [False, True])
def test_preallocate_end_date_bounded_axis_parity(
    tmp_path: Path,
    dry_run: bool,
) -> None:
    target_path = tmp_path / "cube.zarr"
    result = CliRunner().invoke(cli, _preallocate_args(target_path, dry_run=dry_run))

    assert result.exit_code == 0, result.output
    if dry_run:
        assert not target_path.exists(), "dry-run must not create the target store"
        return

    coord = cast(Any, _root(target_path)[f"{_GROUP}/{_COORD}"])
    assert coord.shape == (_SLOT_COUNT,)
    assert tuple(coord.chunks) == (_EXPECTED_CHUNK_LEN,)
    assert coord.attrs[ATTR_PREALLOCATED] is True
    assert np.array_equal(np.asarray(coord[:]), _EXPECTED_VALUES)
