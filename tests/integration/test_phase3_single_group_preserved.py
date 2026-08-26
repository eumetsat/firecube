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

"""T9: Phase 3 single-group plugin behavior regression tests.

Verifies that a single-group capable plugin works correctly with slot-range
flags, both with and without an explicit --slot-group, and that an incorrect
--slot-group value fails fast with a useful error message.
"""

from __future__ import annotations

import importlib
from collections.abc import Iterator
from pathlib import Path

import pytest
from click.testing import CliRunner

from firecube.cli.main import cli
from firecube.ingestor.registry import loader as _loader

pytestmark = pytest.mark.integration

_PLUGIN = "direct_zarr_capable_test_plugin"
_PRODUCT = "direct_zarr_capable_test_product"
_GROUP = "data"


@pytest.fixture(autouse=True)
def reset_plugin_registry() -> Iterator[None]:
    original_loaded = _loader._LOADED
    original_registry = dict(_loader.AVAILABLE_INGESTORS)
    _loader._LOADED = False
    _loader.AVAILABLE_INGESTORS.clear()
    importlib.reload(importlib.import_module("direct_zarr_capable_test_plugin"))
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
        "pipeline_batch_size=300",
        "--option",
        "pipeline_workers=2",
    ]


def test_single_group_capable_without_slot_group_still_works(tmp_path: Path) -> None:
    """Single-group plugin with slot flags but no --slot-group exits 0.

    The engine must not require --slot-group for single-group plugins.
    """
    target_path = tmp_path / "out.zarr"
    args = [*_base_args(target_path), "--slot-start", "0", "--slot-end", "100"]
    result = CliRunner().invoke(cli, args)
    assert result.exit_code == 0, (
        f"Expected exit 0 for single-group plugin without --slot-group; "
        f"got {result.exit_code}.\nOutput:\n{result.output}"
    )
    assert target_path.exists(), f"Zarr store not created at {target_path}"


def test_single_group_capable_with_explicit_slot_group_works(tmp_path: Path) -> None:
    """Single-group plugin with --slot-group matching its group name exits 0."""
    target_path = tmp_path / "out.zarr"
    args = [
        *_base_args(target_path),
        "--slot-start",
        "0",
        "--slot-end",
        "100",
        "--slot-group",
        _GROUP,
    ]
    result = CliRunner().invoke(cli, args)
    assert result.exit_code == 0, (
        f"Expected exit 0 for single-group plugin with --slot-group={_GROUP!r}; "
        f"got {result.exit_code}.\nOutput:\n{result.output}"
    )
    assert target_path.exists(), f"Zarr store not created at {target_path}"


def test_single_group_capable_with_wrong_slot_group_fails_fast(tmp_path: Path) -> None:
    """Single-group plugin with an unknown --slot-group fails with a clear error.

    The error message must mention the bad group name so operators can diagnose
    the misconfiguration without reading source code.
    """
    target_path = tmp_path / "out.zarr"
    wrong_group = "wrong_name"
    args = [
        *_base_args(target_path),
        "--slot-start",
        "0",
        "--slot-end",
        "100",
        "--slot-group",
        wrong_group,
    ]
    result = CliRunner().invoke(cli, args)
    assert result.exit_code != 0, (
        f"Expected non-zero exit for --slot-group={wrong_group!r} (unknown group); "
        f"got exit 0.\nOutput:\n{result.output}"
    )
    combined = result.output + (str(result.exception) if result.exception else "")
    assert wrong_group in combined, (
        f"Expected error output or exception to mention {wrong_group!r}; got:\n{combined}"
    )
