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

"""Architecture lint: no new ``StorageConfig`` subclasses in ``src/``.

Background
----------
``StorageConfig`` (``firecube.core.config``) is the public, file-derived storage
descriptor. Storage location belongs in ``StorageBinding`` / ``ProductIdentity``,
not in subclasses that add ``target_path`` / ``bucket`` / ``target_uri`` back
onto the config.

What this test enforces
-----------------------
No subclasses of ``StorageConfig`` may appear anywhere under ``src/firecube``
outside the permanent exemption list.

Scope
-----
Scans ``src/firecube/**/*.py`` only.  Test fixtures under ``tests/`` are not
scanned — production-grade no-bridge enforcement is the goal here.
"""

from __future__ import annotations

import ast
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[2]
_SRC_ROOT = _REPO_ROOT / "src" / "firecube"

# Permanent allowlist for bridge ``StorageConfig`` subclasses. It is empty on
# purpose; direct subclassing is not part of the current storage contract.
DEPRECATED_BRIDGE_SUBCLASSES: frozenset[str] = frozenset()


def _base_is_storage_config(base: ast.expr) -> bool:
    if isinstance(base, ast.Name) and base.id == "StorageConfig":
        return True
    return isinstance(base, ast.Attribute) and base.attr == "StorageConfig"


def _find_storage_config_subclasses(path: Path) -> list[tuple[int, str]]:
    try:
        tree = ast.parse(path.read_text(encoding="utf-8"))
    except SyntaxError:
        return []

    matches: list[tuple[int, str]] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.ClassDef):
            continue
        if not any(_base_is_storage_config(b) for b in node.bases):
            continue
        matches.append((node.lineno, node.name))
    return matches


def _scan_src() -> set[str]:
    found: set[str] = set()
    for path in _SRC_ROOT.rglob("*.py"):
        if "tests" in path.parts:
            continue
        rel = path.relative_to(_REPO_ROOT).as_posix()
        for lineno, name in _find_storage_config_subclasses(path):
            found.add(f"{rel}:{lineno}:{name}")
    return found


def test_no_new_storage_config_subclasses() -> None:
    """No ``StorageConfig`` subclasses may appear in ``src/`` outside the allowlist."""

    found = _scan_src()
    new = sorted(found - DEPRECATED_BRIDGE_SUBCLASSES)
    assert not new, (
        "New StorageConfig subclass(es) detected in src/firecube:\n  "
        + "\n  ".join(new)
        + "\n\nStorageConfig subclassing is a transitional bridge pattern and is "
        "not allowed in the current contract. Use StorageBinding / "
        "StorageDriverConfig / StorageUri composition instead."
    )
