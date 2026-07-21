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

"""Architectural lint: keep staged-metadata seeding owned by runtime code."""

from __future__ import annotations

import ast
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

pytestmark = pytest.mark.architecture

_REPO_ROOT = Path(__file__).resolve().parents[2]
_TEMPLATE_ROOT = _REPO_ROOT / "src" / "firecube" / "ingestor" / "templates"
_RUNTIME_ROOT = _REPO_ROOT / "src" / "firecube" / "ingestor" / "runtime"

_FORBIDDEN_TEMPLATE_IMPORTS = frozenset(
    {
        "seed_staged_metadata_for_batch",
        "seed_staged_store_metadata",
    }
)

_FORBIDDEN_TEMPLATE_CALLS = _FORBIDDEN_TEMPLATE_IMPORTS


def _read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _parse(path: Path) -> ast.AST | None:
    try:
        return ast.parse(_read_text(path), filename=str(path))
    except SyntaxError:
        return None


def _display_path(path: Path) -> str:
    try:
        return path.relative_to(_REPO_ROOT).as_posix()
    except ValueError:
        return path.as_posix()


def _call_name(node: ast.Call) -> str | None:
    func = node.func
    if isinstance(func, ast.Name):
        return func.id
    if isinstance(func, ast.Attribute):
        return func.attr
    return None


def _find_seeding_callers(
    paths: list[Path] | tuple[Path, ...],
    *,
    forbidden_imports: frozenset[str] = frozenset(),
    forbidden_calls: frozenset[str] = frozenset(),
) -> list[str]:
    violations: list[str] = []

    for path in paths:
        files = sorted(path.rglob("*.py")) if path.is_dir() else [path]
        for file_path in files:
            tree = _parse(file_path)
            if tree is None:
                continue

            rel_path = _display_path(file_path)
            for node in ast.walk(tree):
                if isinstance(node, ast.ImportFrom):
                    offenders = [
                        alias.name for alias in node.names if alias.name in forbidden_imports
                    ]
                    if offenders:
                        violations.append(
                            f"{rel_path}:{node.lineno}: imports forbidden seeding symbol(s): {', '.join(sorted(offenders))}"
                        )
                    continue

                if isinstance(node, ast.Call):
                    name = _call_name(node)
                    if name in forbidden_calls:
                        violations.append(
                            f"{rel_path}:{node.lineno}: calls forbidden seeding symbol `{name}`"
                        )

    return violations


def test_templates_do_not_call_seeding() -> None:
    """No template file may import or call staged-metadata seeding helpers."""
    violations = _find_seeding_callers(
        (_TEMPLATE_ROOT,),
        forbidden_imports=_FORBIDDEN_TEMPLATE_IMPORTS,
        forbidden_calls=_FORBIDDEN_TEMPLATE_CALLS,
    )
    assert not violations, "Template-level seeding violations found:\n" + "\n".join(violations)


def test_runtime_zarr_pre_batch_hook_passes_time_coordinate_to_seeding(monkeypatch) -> None:
    """Runtime owns staged metadata seeding and includes the declared time coord."""
    from firecube.ingestor.runtime import engine
    from firecube.ingestor.runtime.zarr import batch_runner

    calls: list[dict[str, Any]] = []

    def record_seed_call(**kwargs: Any) -> None:
        calls.append(kwargs)

    host = SimpleNamespace(
        _log=object(),
        _resolve_time_dim_name=lambda: "time",
    )
    ctx = SimpleNamespace(output_format="zarr")

    monkeypatch.setattr(batch_runner, "seed_staged_metadata_pre_batch", record_seed_call)

    hook = engine._zarr_pre_batch_hook(host, ctx)  # type: ignore[arg-type]
    assert hook is not None
    hook()

    assert calls == [
        {
            "host": host,
            "ctx": ctx,
            "logger": host._log,
            "coordinate_arrays": ["time"],
        }
    ]
