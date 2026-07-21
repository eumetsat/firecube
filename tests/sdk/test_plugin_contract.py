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

pytestmark = pytest.mark.contract

# Mirrors tests/unit/test_no_raw_fsspec_usage.py: anchor to repo root, not cwd.
_REPO_ROOT = Path(__file__).resolve().parents[2]

# Policy definitions
FORBIDDEN_PREFIXES = [
    "firecube.ingestor.runtime",
    "firecube.ingestor.registry",
    "firecube.ingestor.types",
    "firecube.ingestor.utils",
]

ALLOWED_PREFIXES = [
    "firecube.core.api",
    "firecube.ingestor.api",
    "firecube.ingestor.extensions",
]

PLUGIN_ROOTS = (
    Path("src/firecube/plugins"),
    Path("examples"),
    *sorted(
        path.relative_to(_REPO_ROOT) for path in (_REPO_ROOT / "tests" / "fixtures").glob("*/src")
    ),
)


def get_python_files(root: Path):
    """Recursively yield python files."""
    yield from (path for path in root.rglob("*.py") if not path.name.startswith("test_"))


def _is_firecube_import(module_name: str) -> bool:
    return module_name == "firecube" or module_name.startswith("firecube.")


def _matches_prefix(module_name: str, prefixes: list[str]) -> bool:
    return any(module_name == prefix or module_name.startswith(prefix + ".") for prefix in prefixes)


def check_file_imports(file_path: Path):
    """Parse file and yield validation errors."""
    try:
        tree = ast.parse(file_path.read_text())
    except SyntaxError:
        return

    for node in ast.walk(tree):
        module_names: list[str] = []
        if isinstance(node, ast.Import):
            module_names.extend(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            module_names.append(node.module)

        for module_name in module_names:
            if _matches_prefix(module_name, FORBIDDEN_PREFIXES):
                yield (
                    f"{file_path}: Forbidden import '{module_name}' "
                    f"(internal API; FORBIDDEN_PREFIXES)"
                )
                continue
            if _is_firecube_import(module_name) and not _matches_prefix(
                module_name, ALLOWED_PREFIXES
            ):
                yield (
                    f"{file_path}: Disallowed firecube import '{module_name}' "
                    f"(not in ALLOWED_PREFIXES={ALLOWED_PREFIXES})"
                )


def test_plugin_contract_compliance():
    """Verify that plugins do not import internal modules."""
    scan_paths = [_REPO_ROOT / path for path in PLUGIN_ROOTS if (_REPO_ROOT / path).exists()]
    assert scan_paths, "No plugin directories found to scan"

    errors = []
    for path in scan_paths:
        for py_file in get_python_files(path):
            errors.extend(check_file_imports(py_file))

    assert not errors, "\n".join(errors)
