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

"""Architecture constraint tests: Core Independence.

Enforces:
1. `firecube.core` must NOT import `firecube.ingestor` or `firecube.plugins`.
"""

import ast
from pathlib import Path

import pytest

# Adjust root relative to: tests/architecture/test_core_independence.py
# parent -> architecture, parent -> tests, parent -> root
PROJECT_ROOT = Path(__file__).parent.parent.parent


def get_imports(file_path: Path) -> list[str]:
    """Parse python file and return list of imported module names."""
    try:
        tree = ast.parse(file_path.read_text())
    except SyntaxError:
        return []

    imports = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imports.extend(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imports.append(node.module)
    return imports


def test_core_independence():
    """Ensure firecube.core does not depend on ingestor or plugins."""
    core_dir = PROJECT_ROOT / "src" / "firecube" / "core"

    for path in core_dir.rglob("*.py"):
        if "tests" in path.parts:
            continue

        imports = get_imports(path)
        for imp in imports:
            # Allow purely string imports (typing) if necessary, but ast parser usually catches actual imports
            if (
                imp.startswith("firecube.ingestor") or "ingestor" in imp.split(".")
            ) and imp.startswith("firecube.ingestor"):
                pytest.fail(f"Layer violation in {path.relative_to(PROJECT_ROOT)}: imports {imp}")

            if imp.startswith("firecube.plugins"):
                pytest.fail(f"Layer violation in {path.relative_to(PROJECT_ROOT)}: imports {imp}")

            # Strict API usage check
            if "register_ingestor" in imp and "firecube.ingestor.api" not in imp:
                pytest.fail(
                    f"API violation in {path.relative_to(PROJECT_ROOT)}: imports register_ingestor from {imp}. Must use firecube.ingestor.api."
                )
