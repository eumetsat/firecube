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

import ast
from pathlib import Path

import pytest

# Mirrors tests/unit/test_no_raw_fsspec_usage.py: anchor to repo root, not cwd.
_REPO_ROOT = Path(__file__).resolve().parents[2]

# Define the forbidden imports
FORBIDDEN_CALLS = {
    "get_global_storage_config",
}
FORBIDDEN_MODULES = {
    # We also banned runtime globals
    "firecube.core.runtime.get_global_storage_config",
}

# Directories to check
CHECK_DIRS = [
    "src/firecube/ingestor",
]


def get_python_files(root: Path):
    return sorted(root.rglob("*.py"))


class IsolationChecker(ast.NodeVisitor):
    def __init__(self, filename):
        self.filename = filename
        self.errors = []

    def visit_Import(self, node):
        for alias in node.names:
            if alias.name in FORBIDDEN_MODULES:
                self.errors.append(f"{self.filename}:{node.lineno} forbidden import '{alias.name}'")
        self.generic_visit(node)

    def visit_ImportFrom(self, node):
        if not node.module:
            return

        # Check forbidden modules
        if node.module in FORBIDDEN_MODULES:
            self.errors.append(
                f"{self.filename}:{node.lineno} forbidden import from '{node.module}'"
            )
            return

        # Check forbidden imports from runtime
        if node.module == "firecube.core.runtime":
            for alias in node.names:
                if alias.name in FORBIDDEN_CALLS:
                    self.errors.append(
                        f"{self.filename}:{node.lineno} forbidden import '{alias.name}' from '{node.module}'"
                    )

        self.generic_visit(node)


@pytest.mark.parametrize("directory", CHECK_DIRS)
def test_plugin_isolation(directory):
    root = _REPO_ROOT / directory
    if not root.exists():
        pytest.fail(f"expected directory missing: {root}")

    files = get_python_files(root)
    all_errors = []

    for py_file in files:
        # Skip tests directories within plugins/ingestor if checking source strictness?
        # Typically we want even tests to be isolated but maybe relax some?
        # For now, strict.

        try:
            tree = ast.parse(py_file.read_text(), filename=str(py_file))
            checker = IsolationChecker(str(py_file))
            checker.visit(tree)
            all_errors.extend(checker.errors)
        except Exception as e:
            all_errors.append(f"Failed to parse {py_file}: {e}")

    assert not all_errors, "\n".join(all_errors)
