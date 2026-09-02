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

"""End-to-end proof that stable-closure callable payloads reach the store.

The safe fixture emits one ``kind="region"`` intent and one ``kind="static"``
intent, each with a ``data`` callable that closes over a module-level numpy
constant. This test runs the plugin through the public CLI and asserts that
the written Zarr arrays match the values the callables return — i.e. the
dispatch layer resolved the callables and wrote their results.
"""

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

PLUGIN = "callable_payload_safe"
PRODUCT = "callable_payload_safe"


@pytest.fixture(autouse=True)
def reset_plugin_registry() -> Iterator[None]:
    original_loaded = _loader._LOADED
    original_registry = dict(_loader.AVAILABLE_INGESTORS)
    _loader._LOADED = False
    _loader.AVAILABLE_INGESTORS.clear()
    importlib.reload(importlib.import_module("callable_payload_test_plugin"))
    _loader._LOADED = True
    yield
    _loader._LOADED = original_loaded
    _loader.AVAILABLE_INGESTORS.clear()
    _loader.AVAILABLE_INGESTORS.update(original_registry)


def _ingest_args(target_path: Path) -> list[str]:
    return [
        "ingest",
        PLUGIN,
        "--target",
        f"file://{target_path}",
        "--product-name",
        PRODUCT,
        "--storage-type",
        "local",
        "--storage-driver",
        "fsspec",
        "--write-mode",
        "direct",
        "--option",
        "no_progress=true",
    ]


def test_safe_callables_are_resolved_and_written_to_store(tmp_path: Path) -> None:
    target_path = tmp_path / "out.zarr"
    result = CliRunner().invoke(cli, _ingest_args(target_path))
    assert result.exit_code == 0, (
        f"Expected exit 0 for safe callable-payload ingest; got {result.exit_code}.\n"
        f"Output:\n{result.output}"
    )
    assert target_path.exists(), f"Zarr store not created at {target_path}"

    root = zarr.open_group(store=str(target_path), mode="r", zarr_format=3)
    values = np.asarray(cast(Any, root["data/values"])[:])
    lat = np.asarray(cast(Any, root["data/lat"])[:])

    expected_region = np.full((4, 4), 42.0, dtype=np.float32)
    expected_static = np.arange(4, dtype=np.float64) * 10.0

    np.testing.assert_array_equal(values[0], expected_region)
    np.testing.assert_array_equal(lat, expected_static)
    assert values.shape == (1, 4, 4)
    assert lat.shape == (4,)
