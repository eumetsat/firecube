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

"""Architecture guard: pd.Timestamp(<numeric>) requires explicit unit= kwarg.

Prevents the 1970-epoch bug caused by ``pd.Timestamp(int_or_float)`` which
silently interprets the value as nanoseconds since epoch.

Scope: write-domain modules only (ingestor runtime + core zarr maintenance).
Detection: AST walk over each ``*.py`` file under the scan dirs. A call is
flagged when:

* the callee resolves to ``pd.Timestamp`` / ``pandas.Timestamp`` (matching the
  imported alias) or to a bare ``Timestamp`` name that was imported from
  ``pandas``, AND
* the first positional argument is a numeric literal (``ast.Constant`` whose
  ``.value`` is ``int`` or ``float``), AND
* the call does not pass ``unit=`` as a keyword argument.

The permanent allowlist below names modules whose ``pd.Timestamp`` usage is
known-safe (e.g. ``pd.Timestamp(datetime64)`` after CF-time decoding).
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

SCAN_DIRS: list[Path] = [
    Path("src/firecube/ingestor/runtime"),
    Path("src/firecube/core/zarr"),
]

PERMANENT_ALLOWLIST: frozenset[str] = frozenset(
    {
        # PERMANENT: time_decode calls pd.Timestamp(datetime64) after CF-time
        # decoding — input is a decoded datetime64, never a raw numeric literal.
        "src/firecube/core/zarr/time_decode.py",
    }
)


class _PdTimestampVisitor(ast.NodeVisitor):
    """Detect ``pd.Timestamp(<numeric literal>)`` calls without ``unit=`` kwarg."""

    def __init__(self) -> None:
        self.violations: list[tuple[int, str]] = []
        self._pandas_aliases: set[str] = set()
        self._timestamp_names: set[str] = set()

    def visit_Import(self, node: ast.Import) -> None:
        for alias in node.names:
            if alias.name in ("pandas", "pd"):
                self._pandas_aliases.add(alias.asname or alias.name)
        self.generic_visit(node)

    def visit_ImportFrom(self, node: ast.ImportFrom) -> None:
        if node.module in ("pandas", "pd"):
            for alias in node.names:
                if alias.name == "Timestamp":
                    self._timestamp_names.add(alias.asname or "Timestamp")
        self.generic_visit(node)

    def _is_pd_timestamp_call(self, node: ast.Call) -> bool:
        # ``pd.Timestamp(...)`` / ``pandas.Timestamp(...)``
        if (
            isinstance(node.func, ast.Attribute)
            and node.func.attr == "Timestamp"
            and isinstance(node.func.value, ast.Name)
        ):
            return node.func.value.id in self._pandas_aliases
        # Bare ``Timestamp(...)`` when imported from pandas
        if isinstance(node.func, ast.Name):
            return node.func.id in self._timestamp_names
        return False

    @staticmethod
    def _first_arg_is_numeric(node: ast.Call) -> bool:
        if not node.args:
            return False
        first = node.args[0]
        # ``bool`` is a subclass of ``int`` but ``True``/``False`` as a
        # timestamp source is exotic enough that we accept the false positive.
        return isinstance(first, ast.Constant) and isinstance(first.value, (int, float))

    @staticmethod
    def _has_unit_kwarg(node: ast.Call) -> bool:
        return any(kw.arg == "unit" for kw in node.keywords)

    def visit_Call(self, node: ast.Call) -> None:
        if (
            self._is_pd_timestamp_call(node)
            and self._first_arg_is_numeric(node)
            and not self._has_unit_kwarg(node)
        ):
            self.violations.append(
                (
                    node.lineno,
                    f"pd.Timestamp(<numeric literal>) without unit= at line {node.lineno}",
                )
            )
        self.generic_visit(node)


@pytest.mark.architecture
def test_no_raw_pd_timestamp_numeric() -> None:
    """Write-domain modules must not call ``pd.Timestamp(<numeric>)`` without ``unit=``."""
    all_violations: list[str] = []
    for scan_dir in SCAN_DIRS:
        for py_file in sorted(scan_dir.rglob("*.py")):
            if any(str(py_file).endswith(a) for a in PERMANENT_ALLOWLIST):
                continue
            source = py_file.read_text(encoding="utf-8")
            tree = ast.parse(source, filename=str(py_file))
            visitor = _PdTimestampVisitor()
            visitor.visit(tree)
            all_violations.extend(
                f"{py_file}:{lineno}: {msg}" for lineno, msg in visitor.violations
            )
    assert not all_violations, (
        "Write-domain code contains raw pd.Timestamp(<numeric>) without unit=:\n"
        + "\n".join(all_violations)
    )
