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

"""Architectural lint: prevent direct ``StorageDriverConfig(...)`` construction in src/.

The canonical seam for obtaining a ``StorageDriverConfig`` is the factory
``StorageDriverConfig.from_storage_config(cfg)`` (and the forthcoming
``from_storage_config_or_default(cfg | None)`` helper landed by T4). Direct
``StorageDriverConfig(...)`` calls anywhere in ``src/firecube/`` are a
regression unless explicitly documented in the deprecated-exemption list
below.

This test enforces that production code does not directly construct
``StorageDriverConfig(...)``; callers route through the typed factory instead.

Detection is AST-aware so the guard is not fooled by:

* type annotations and bare references (``x: StorageDriverConfig``) — not
  ``ast.Call`` nodes, never reported;
* ``from ... import StorageDriverConfig`` — ``ImportFrom`` nodes, never
  reported;
* factory method calls such as ``StorageDriverConfig.from_storage_config(...)``
  and ``StorageDriverConfig.from_storage_config_or_default(...)`` — these are
  ``Attribute`` access on the class, not bare ``Name`` calls.

Test fixtures under ``tests/`` are intentionally NOT scanned: boundary tests
need to construct driver configs explicitly. Only ``src/firecube/`` is
walked.
"""

from __future__ import annotations

import ast
from pathlib import Path
from typing import TypeGuard

_REPO_ROOT = Path(__file__).resolve().parents[2]
_SRC_ROOT = _REPO_ROOT / "src" / "firecube"

# Permanent exemption list. It is empty on purpose; production code should use
# ``StorageDriverConfig.from_storage_config*`` factories.
DEPRECATED_DIRECT_CONSTRUCTIONS: frozenset[str] = frozenset()


def _is_direct_storage_driver_config_call(node: ast.AST) -> TypeGuard[ast.Call]:
    """Return True iff ``node`` is a direct ``StorageDriverConfig(...)`` call.

    Distinguishes the bare ``Name`` call (``StorageDriverConfig(...)``) from
    factory ``Attribute`` calls (``StorageDriverConfig.from_storage_config(...)``,
    ``StorageDriverConfig.from_storage_config_or_default(...)``). Non-``Call``
    nodes — type annotations, imports, attribute access — are also excluded.
    """
    if not isinstance(node, ast.Call):
        return False
    func = node.func
    return isinstance(func, ast.Name) and func.id == "StorageDriverConfig"


def _find_direct_constructions(root: Path) -> list[str]:
    """Walk ``root`` and return ``file:line`` for every direct construction call."""
    findings: list[str] = []
    for path in sorted(root.rglob("*.py")):
        try:
            source = path.read_text(encoding="utf-8")
        except OSError:
            # Broken symlink or unreadable file — skip silently; we cannot
            # report a finding we cannot parse, and the file would also fail
            # to import in production.
            continue
        try:
            tree = ast.parse(source, filename=str(path))
        except SyntaxError:
            continue
        try:
            rel_path = path.relative_to(_REPO_ROOT).as_posix()
        except ValueError:
            # Path falls outside the repo root (unexpected); record as-is so
            # the diagnostic is still informative.
            rel_path = str(path)
        findings.extend(
            f"{rel_path}:{node.lineno}"
            for node in ast.walk(tree)
            if _is_direct_storage_driver_config_call(node)
        )
    return findings


def test_no_new_direct_storage_driver_config_constructions() -> None:
    """Fail if a new direct ``StorageDriverConfig(...)`` construction appears in src/.

    Tests under ``tests/`` are exempt (boundary tests need explicit
    construction). Only ``src/firecube/`` is scanned.
    """
    findings = _find_direct_constructions(_SRC_ROOT)
    unexpected = sorted(f for f in findings if f not in DEPRECATED_DIRECT_CONSTRUCTIONS)
    assert not unexpected, (
        "Direct StorageDriverConfig(...) construction is forbidden outside the "
        "documented deprecated-exemption list. Route the call through "
        "StorageDriverConfig.from_storage_config_or_default(cfg) instead.\n"
        "New leaks found:\n  " + "\n  ".join(unexpected)
    )
