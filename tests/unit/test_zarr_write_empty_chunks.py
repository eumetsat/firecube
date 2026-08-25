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

import ast
import contextlib
from pathlib import Path

import pytest
import zarr
from click.testing import CliRunner

from firecube.cli.main import cli
from firecube.ingestor.runtime.zarr.strategies.indexed_region import IndexedRegionStrategy
from firecube.ingestor.runtime.zarr.write_context import ZarrWriteContext
from firecube.ingestor.templates.config import ZarrTemplateConfig

pytestmark = pytest.mark.unit


def test_default_is_false() -> None:
    assert ZarrTemplateConfig().zarr_write_empty_chunks is False


def test_option_flows_through_typed_config() -> None:
    assert (
        ZarrTemplateConfig.from_options({"zarr_write_empty_chunks": "true"}).zarr_write_empty_chunks
        is True
    )


def test_show_options_lists_the_field() -> None:
    result = CliRunner().invoke(
        cli, ["ingest", "direct_zarr_capable_test_plugin", "--show-options"]
    )

    assert result.exit_code == 0, result.output
    assert "--option zarr_write_empty_chunks" in result.output


def test_direct_zarr_dispatch_applies_scoped_config(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[dict[str, bool]] = []
    entered = False
    exited = False

    @contextlib.contextmanager
    def spy_config_set(config: dict[str, bool]):
        nonlocal entered, exited
        calls.append(config)
        entered = True
        try:
            yield
        finally:
            exited = True

    monkeypatch.setattr(zarr.config, "set", spy_config_set)

    result = IndexedRegionStrategy(store_uri=str(tmp_path / "test.zarr")).write_groups(
        group_to_intents={},
        zarr_write_empty_chunks=True,
    )

    assert result["zarr_write_empty_chunks_effective"] is True
    assert {"array.write_empty_chunks": True} in calls
    assert entered is True
    assert exited is True


def test_generic_write_context_applies_scoped_config(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[dict[str, bool]] = []
    entered = False
    exited = False

    @contextlib.contextmanager
    def spy_config_set(config: dict[str, bool]):
        nonlocal entered, exited
        if "array.write_empty_chunks" in config:
            calls.append(config)
            entered = True
        try:
            yield
        finally:
            if "array.write_empty_chunks" in config:
                exited = True

    monkeypatch.setattr(zarr.config, "set", spy_config_set)

    with ZarrWriteContext(write_lock=contextlib.nullcontext(), write_empty_chunks=True):  # type: ignore[arg-type]
        assert entered is True

    assert {"array.write_empty_chunks": True} in calls
    assert exited is True


def test_effective_value_recorded_in_metrics(tmp_path: Path) -> None:
    result = IndexedRegionStrategy(store_uri=str(tmp_path / "test.zarr")).write_groups(
        group_to_intents={},
        zarr_write_empty_chunks=True,
    )

    assert result["zarr_write_empty_chunks_effective"] is True


def test_array_write_empty_chunks_never_mutated_bare() -> None:
    root = Path("src/firecube")
    offenders: list[str] = []
    for path in root.rglob("*.py"):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        parents: dict[ast.AST, ast.AST] = {}
        for parent in ast.walk(tree):
            for child in ast.iter_child_nodes(parent):
                parents[child] = parent
        for node in ast.walk(tree):
            if not _is_array_write_empty_chunks_set_call(node):
                continue
            if not _has_with_ancestor(node, parents):
                offenders.append(f"{path}:{getattr(node, 'lineno', '?')}")
    assert offenders == []


def _is_array_write_empty_chunks_set_call(node: ast.AST) -> bool:
    if not isinstance(node, ast.Call):
        return False
    if not isinstance(node.func, ast.Attribute) or node.func.attr != "set":
        return False
    for arg in node.args:
        if isinstance(arg, ast.Dict):
            for key in arg.keys:
                if isinstance(key, ast.Constant) and key.value == "array.write_empty_chunks":
                    return True
    return False


def _has_with_ancestor(node: ast.AST, parents: dict[ast.AST, ast.AST]) -> bool:
    current: ast.AST | None = node
    while current is not None:
        if isinstance(current, ast.With):
            return True
        current = parents.get(current)
    return False
