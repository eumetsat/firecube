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

"""Architectural lint: ban naked ``== fill_value`` / ``== fill`` comparisons.

Naked equality against a Zarr ``fill_value`` is wrong when the fill is NaN
(or any NaN-like sentinel) because ``NaN == NaN`` is False — the
"is this slot fresh?" probe returns False on a freshly allocated NaN array
and ingestion mis-classifies untouched slots as "real data" that diverged
from the incoming write. The canonical NaN-aware comparator is
``_array_is_all_fill(arr, fill_value)`` in
``firecube.core.zarr.region_writer``.

Scope: write-domain modules only (zarr maintenance + ingestor runtime).
Detection: AST walk over each ``*.py`` file under the scan dirs. A
comparison is flagged when:

* it is an ``ast.Compare`` with operator ``ast.Eq`` or ``ast.NotEq``, AND
* either side names ``fill_value`` / ``fill`` (``ast.Name``) or any
  attribute access ending in ``.fill_value`` (``ast.Attribute``), AND
* it does NOT appear inside one of the canonical NaN-aware comparator
  functions listed in ``_EXEMPT_FUNCTION_NAMES`` (those bodies are the
  intended home of the raw equality probe).
"""

from __future__ import annotations

import ast
from pathlib import Path

SCAN_DIRS: list[Path] = [
    Path("src/firecube/core/zarr"),
    Path("src/firecube/ingestor/runtime"),
]

# No file-path exemptions are needed once T6 routes the call site through
# ``_array_is_all_fill``. Function-name based exemption (below) covers the
# canonical NaN-aware comparators that legitimately own the raw probe.
PERMANENT_ALLOWLIST: frozenset[str] = frozenset()

# Functions whose bodies ARE the canonical NaN-aware fill comparators;
# their internal ``arr == fill_value`` lines are the intended implementation.
_EXEMPT_FUNCTION_NAMES: frozenset[str] = frozenset(
    {
        "_array_is_all_fill",
        "_fill_value_is_missing",
        "_fill_values_equal",
    }
)


class _FillValueComparisonVisitor(ast.NodeVisitor):
    """Detect naked ``== fill_value`` / ``== fill`` comparisons.

    Tracks the enclosing function name via a stack so call sites inside
    ``_EXEMPT_FUNCTION_NAMES`` are skipped (they ARE the canonical
    NaN-aware comparators).
    """

    def __init__(self) -> None:
        self.violations: list[tuple[int, str]] = []
        self._function_stack: list[str] = []

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        self._function_stack.append(node.name)
        self.generic_visit(node)
        self._function_stack.pop()

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
        self._function_stack.append(node.name)
        self.generic_visit(node)
        self._function_stack.pop()

    @staticmethod
    def _is_fill_name(node: ast.expr) -> bool:
        if isinstance(node, ast.Name):
            return node.id in ("fill_value", "fill")
        if isinstance(node, ast.Attribute):
            return node.attr == "fill_value"
        return False

    def visit_Compare(self, node: ast.Compare) -> None:
        # Exempt the canonical NaN-aware comparators — their bodies ARE
        # the intended home for raw equality probes.
        if self._function_stack and self._function_stack[-1] in _EXEMPT_FUNCTION_NAMES:
            self.generic_visit(node)
            return
        for i, op in enumerate(node.ops):
            if not isinstance(op, (ast.Eq, ast.NotEq)):
                continue
            left = node.left if i == 0 else node.comparators[i - 1]
            right = node.comparators[i]
            if self._is_fill_name(left) or self._is_fill_name(right):
                expr = ast.unparse(node)
                self.violations.append((node.lineno, expr))
                break
        self.generic_visit(node)


def test_no_naked_equality_against_fill_value() -> None:
    """Write-domain modules must not compare against ``fill_value`` / ``fill`` directly.

    Naked ``existing == fill_value`` returns False for NaN fills, mis-classifying
    fresh slots as "real data". Use ``_array_is_all_fill(arr, fill_value)`` from
    ``firecube.core.zarr.region_writer`` instead — it is NaN-aware.
    """
    all_violations: list[str] = []
    for scan_dir in SCAN_DIRS:
        for py_file in sorted(scan_dir.rglob("*.py")):
            rel_path = py_file.as_posix()
            if rel_path in PERMANENT_ALLOWLIST:
                continue
            source = py_file.read_text(encoding="utf-8")
            try:
                tree = ast.parse(source, filename=str(py_file))
            except SyntaxError:
                continue
            visitor = _FillValueComparisonVisitor()
            visitor.visit(tree)
            for lineno, expr in visitor.violations:
                all_violations.append(
                    f"{rel_path}:{lineno}: naked fill comparison `{expr}` — "
                    f"use `_array_is_all_fill(arr, fill_value)` from "
                    f"firecube.core.zarr.region_writer instead"
                )
    assert not all_violations, "Naked `== fill_value` / `== fill` comparisons found:\n" + "\n".join(
        all_violations
    )
