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

"""Integration tests for fail-closed blocked-ranges behavior (Phase 3.5)."""

from __future__ import annotations

import importlib
import json
from collections.abc import Iterator
from pathlib import Path

import pytest
from click.testing import CliRunner

from firecube.cli import zarr as zarr_module
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


def _plan_args(tmp_path: Path) -> list[str]:
    return [
        "zarr",
        "slots",
        _PLUGIN,
        "--target",
        f"file://{tmp_path}/out.zarr",
        "--product-name",
        _PRODUCT,
        "--storage-type",
        "local",
        "--storage-driver",
        "fsspec",
        "--write-mode",
        "direct",
    ]


def test_plan_fails_when_one_group_blocked_other_valid(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Multi-group: group_a has blocked range, group_b has no coverage → whole command fails."""
    monkeypatch.setattr(
        zarr_module,
        "_query_slots_coverage",
        lambda ctx, **kwargs: {"group_a": [(0, 73)], "group_b": []},
    )
    result = CliRunner().invoke(cli, _plan_args(tmp_path))

    assert result.exit_code != 0, result.output
    assert "blocked" in result.output.lower(), result.output
    assert "group_a" in result.output, result.output
    assert '"schema_version"' not in result.output, result.output


def test_plan_succeeds_when_no_groups_blocked(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Valid plan with aligned coverage still emits JSON with empty blocked_ranges."""
    monkeypatch.setattr(
        zarr_module,
        "_query_slots_coverage",
        lambda ctx, **kwargs: {"group_a": [], "group_b": []},
    )
    result = CliRunner().invoke(cli, _plan_args(tmp_path))

    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    for group in payload["groups"]:
        assert "blocked_ranges" in group, f"blocked_ranges missing from group {group['name']}"
        assert group["blocked_ranges"] == [], f"expected empty blocked_ranges for {group['name']}"


def test_plan_error_message_format(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Error message includes group name, blocked intervals, and remediation hints."""
    monkeypatch.setattr(
        zarr_module,
        "_query_slots_coverage",
        lambda ctx, **kwargs: {"group_a": [(0, 73)], "group_b": []},
    )
    result = CliRunner().invoke(cli, _plan_args(tmp_path))

    assert result.exit_code != 0
    output_lower = result.output.lower()
    assert "blocked" in output_lower
    assert "group_a" in result.output
    assert "coverage" in output_lower or "global_expected" in result.output
    # Phase 3.7: error must name real remediation primitives (delete-span, not bare delete --range)
    assert "firecube chunks delete-span" in output_lower, result.output
    assert "force_reingest=true" in output_lower, result.output
    # Phase 3.8: lock the full runnable command shape (--product/--dry-run/--force/--yes-i-really-mean-it)
    assert "--product" in output_lower, result.output
    assert "--dry-run" in output_lower, result.output
    assert "--force" in output_lower, result.output
    assert "--yes-i-really-mean-it" in output_lower, result.output
