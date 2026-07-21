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

"""Architectural lint: ban unguarded ``(expected_time_count, *spec.shape[1:])`` substitutions.

Substituting the first axis of a Zarr array spec with ``expected_time_count``
is only correct for time-indexed arrays. Applying it unconditionally to
static (non-time-indexed) arrays — e.g. ``lat``/``lon`` coordinates — would
silently corrupt the spatial axis length when ``shape == (height, width)``
because the first dim is ``height``, not a time axis.

The canonical guarded pattern lives in
``src/firecube/ingestor/templates/direct_zarr.py``::

    if arr_spec.time_indexed:
        effective_shape = (expected_time_count, *arr_spec.shape[1:])
    else:
        effective_shape = arr_spec.shape

Scope: write-domain modules (zarr maintenance, ingestor runtime, ingestor
templates, and the CLI zarr command — the latter contains a direct allocator).
Detection: AST walk over each ``*.py`` file under the scan dirs. A tuple is
flagged when:

* it is an ``ast.Tuple`` whose first element names ``expected_time_count``
  (``ast.Name``), AND
* it contains at least one ``ast.Starred`` element (the ``*spec.shape[1:]``
  unpacking), AND
* it does NOT appear inside an enclosing ``ast.If`` statement whose test
  references ``.time_indexed`` (the canonical guard).

A ternary ``(X, *Y) if cond else Z`` is ``ast.IfExp``, not ``ast.If``, so
ternary-only "guards" do NOT count — only a statement-level ``if`` whose
test references ``time_indexed`` protects the substitution.
"""

from __future__ import annotations

import ast
from pathlib import Path

SCAN_DIRS: list[Path] = [
    Path("src/firecube/core/zarr"),
    Path("src/firecube/ingestor/runtime"),
    Path("src/firecube/ingestor/templates"),
    Path("src/firecube/cli"),
]

# No file-path exemptions: every call site must adopt the guarded pattern.
PERMANENT_ALLOWLIST: frozenset[str] = frozenset()

# No function-name exemptions: the three canonical sites must all be guarded.
_EXEMPT_FUNCTION_NAMES: frozenset[str] = frozenset()


def _if_test_references_time_indexed(test: ast.expr) -> bool:
    """Return True iff the given ``if`` test references ``.time_indexed``.

    Walks the test expression looking for any ``ast.Attribute`` whose
    ``.attr`` equals ``"time_indexed"`` (e.g. ``arr_spec.time_indexed``).
    """
    for sub in ast.walk(test):
        if isinstance(sub, ast.Attribute) and sub.attr == "time_indexed":
            return True
    return False


class _TimeCountSubstitutionVisitor(ast.NodeVisitor):
    """Detect unguarded ``(expected_time_count, *spec.shape[1:])`` tuples.

    Tracks two stacks:

    * ``_function_stack`` — current enclosing function name; used to skip
      flagging inside ``_EXEMPT_FUNCTION_NAMES`` (empty by default).
    * ``_if_guard_stack`` — list of booleans, one per enclosing ``ast.If``
      statement, where True means the ``if`` test references
      ``.time_indexed``. A tuple is PROTECTED iff ``any(self._if_guard_stack)``.
    """

    def __init__(self) -> None:
        self.violations: list[tuple[int, str, str]] = []
        self._function_stack: list[str] = []
        self._if_guard_stack: list[bool] = []

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        self._function_stack.append(node.name)
        self.generic_visit(node)
        self._function_stack.pop()

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
        self._function_stack.append(node.name)
        self.generic_visit(node)
        self._function_stack.pop()

    def visit_If(self, node: ast.If) -> None:
        guarded = _if_test_references_time_indexed(node.test)
        # The guard applies to the ``body`` branch only; the ``orelse`` branch
        # runs precisely when the guard is False, so substitutions inside it
        # are NOT protected.
        self._if_guard_stack.append(guarded)
        for stmt in node.body:
            self.visit(stmt)
        self._if_guard_stack.pop()
        # ``orelse`` is visited under the prior (outer) guard context only.
        for stmt in node.orelse:
            self.visit(stmt)

    @staticmethod
    def _is_candidate_substitution(node: ast.Tuple) -> bool:
        if len(node.elts) < 2:
            return False
        first = node.elts[0]
        if not (isinstance(first, ast.Name) and first.id == "expected_time_count"):
            return False
        return any(isinstance(elt, ast.Starred) for elt in node.elts[1:])

    def visit_Tuple(self, node: ast.Tuple) -> None:
        if self._is_candidate_substitution(node):
            current_func = self._function_stack[-1] if self._function_stack else "<module>"
            if current_func not in _EXEMPT_FUNCTION_NAMES and not any(self._if_guard_stack):
                snippet = ast.unparse(node)
                self.violations.append((node.lineno, current_func, snippet))
        self.generic_visit(node)


def test_no_unguarded_time_count_substitution() -> None:
    """Source must not contain unguarded ``(expected_time_count, *spec.shape[1:])`` constructions.

    Applying the substitution unconditionally to static (non-time-indexed)
    arrays would corrupt their first-axis length. Wrap the substitution in
    ``if arr_spec.time_indexed: ... else: effective_shape = arr_spec.shape``.
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
            visitor = _TimeCountSubstitutionVisitor()
            visitor.visit(tree)
            for lineno, func_name, snippet in visitor.violations:
                all_violations.append(
                    f"{rel_path}:{lineno}: unguarded substitution in `{func_name}`: {snippet}"
                )
    assert not all_violations, (
        "Unguarded (expected_time_count, *shape[1:]) substitutions found:\n"
        + "\n".join(all_violations)
        + "\n\nRemediation: Wrap the substitution in `if arr_spec.time_indexed: ... "
        + "else: effective_shape = arr_spec.shape` "
        + "(see src/firecube/ingestor/templates/direct_zarr.py:198-201 for the canonical pattern)."
    )
