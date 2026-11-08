# FIRECUBE_TEMPLATE_LICENSE_HEADER_BEGIN
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
# FIRECUBE_TEMPLATE_LICENSE_HEADER_END
{copyright_header}
"""Static test: plugin source may import Firecube only from ``firecube.sdk``.

The test parses every ``.py`` file under ``src/{import_name}/`` with :mod:`ast`.
It fails on any ``from firecube.X import ...`` or ``import firecube.X`` statement
that names a module other than ``firecube.sdk``.
"""

from __future__ import annotations

import ast
import pathlib

# Add a module here only when a name cannot be imported from firecube.sdk,
# and say why in a comment.
APPROVED_FIRECUBE_MODULES: frozenset[str] = frozenset({{"firecube.sdk"}})

SRC_ROOT: pathlib.Path = pathlib.Path(__file__).parent.parent / "src" / "{import_name}"


def _collect_firecube_imports(py_file: pathlib.Path) -> list[tuple[str, int]]:
    tree = ast.parse(py_file.read_text(encoding="utf-8"))
    found: list[tuple[str, int]] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module:
            if node.module == "firecube" or node.module.startswith("firecube."):
                found.append((node.module, node.lineno))
        elif isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name == "firecube" or alias.name.startswith("firecube."):
                    found.append((alias.name, node.lineno))
    return found


def test_only_firecube_sdk_is_imported() -> None:
    violations: list[str] = []
    py_files = sorted(SRC_ROOT.rglob("*.py"))
    assert py_files, f"No Python files found under {{SRC_ROOT}}"

    for py_file in py_files:
        if "__pycache__" in py_file.parts:
            continue
        rel = py_file.relative_to(SRC_ROOT.parent.parent)
        for module, lineno in _collect_firecube_imports(py_file):
            if module not in APPROVED_FIRECUBE_MODULES:
                violations.append(f"{{rel}}:{{lineno}}: import of '{{module}}'")

    assert not violations, (
        f"Plugins may only import Firecube from {{sorted(APPROVED_FIRECUBE_MODULES)}}:\n  "
        + "\n  ".join(violations)
    )
