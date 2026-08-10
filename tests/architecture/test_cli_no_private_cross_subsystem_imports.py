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

"""RF-12: CLI must not import underscore-prefixed names from cross-subsystem modules."""

from __future__ import annotations

import ast
from pathlib import Path
from typing import NamedTuple

import pytest

_REPO_ROOT = Path(__file__).resolve().parents[2]
_CLI_ROOT = _REPO_ROOT / "src" / "firecube" / "cli"


class _Violation(NamedTuple):
    path: Path
    line: int
    module: str
    name: str


class _ImportCollector(ast.NodeVisitor):
    """Collect cross-subsystem imports of single-underscore-prefixed names."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self.violations: list[_Violation] = []

    def visit_ImportFrom(self, node: ast.ImportFrom) -> None:
        module = node.module or ""
        if not module.startswith("firecube."):
            return
        if module == "firecube.cli" or module.startswith("firecube.cli."):
            return

        for alias in node.names:
            if alias.name.startswith("_") and not alias.name.startswith("__"):
                self.violations.append(
                    _Violation(
                        path=self.path,
                        line=node.lineno,
                        module=module,
                        name=alias.name,
                    )
                )

        self.generic_visit(node)


def _find_violations(path: Path) -> list[_Violation]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    collector = _ImportCollector(path)
    collector.visit(tree)
    return collector.violations


@pytest.mark.architecture
def test_no_cli_private_cross_subsystem_imports() -> None:
    """RF-12: CLI modules must use public names for non-CLI imports."""
    assert _CLI_ROOT.is_dir(), f"expected CLI root at {_CLI_ROOT}"

    violations = [
        violation
        for py_file in sorted(_CLI_ROOT.rglob("*.py"))
        for violation in _find_violations(py_file)
    ]

    if violations:
        messages = []
        for violation in violations:
            rel_path = violation.path.relative_to(_REPO_ROOT).as_posix()
            display_path = rel_path.removeprefix("src/firecube/")
            messages.append(
                f"{display_path}:{violation.line} imports underscore-prefixed name "
                f"'{violation.name}' from cross-subsystem module '{violation.module}'. "
                "Use a public alias exported from firecube.ingestor.api or "
                "firecube.ingestor.runtime.zarr.write."
            )

        pytest.fail(
            "RF-12 violation: CLI modules cannot import underscore-prefixed names "
            "from cross-subsystem modules.\n" + "\n".join(messages)
        )
