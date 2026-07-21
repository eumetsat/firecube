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

from __future__ import annotations

import ast
from pathlib import Path

import pytest

ALLOWED_TEMPLATES_FIRECUBE_PREFIXES = (
    "firecube.core.api",
    "firecube.core.errors",  # error classes needed for exception handling in templates
    "firecube.core.filesystem",
    "firecube.core.slot_index",  # direct import avoids circular-load via firecube.core.api
    "firecube.core.uris",
    "firecube.core.zarr.region_writer",
    "firecube.ingestor.api",
    "firecube.ingestor.config",
    "firecube.ingestor.extensions",
    "firecube.ingestor.runtime.zarr",
    "firecube.ingestor.templates",
    "firecube.ingestor.types",
    "firecube.ingestor.utils",
)

ALLOWED_PLUGINS_FIRECUBE_PREFIXES = (
    "firecube.core.api",
    "firecube.ingestor.api",
    "firecube.ingestor.extensions",
    "firecube.plugins",
)

CHECK_DIRS = (
    Path("src/firecube/ingestor/templates"),
    Path("src/firecube/plugins"),
)


def iter_python_files(root: Path):
    yield from root.rglob("*.py")


class ImportVisitor(ast.NodeVisitor):
    def __init__(self, *, allowed_prefixes: tuple[str, ...]) -> None:
        self.errors: list[str] = []
        self._in_type_checking = False
        self._allowed_prefixes = allowed_prefixes

    def _is_allowed_import(self, module_name: str | None) -> bool:
        if not module_name:
            return True

        if module_name == "firecube":
            return True

        if module_name.startswith("firecube."):
            return any(
                module_name == prefix or module_name.startswith(prefix + ".")
                for prefix in self._allowed_prefixes
            )

        return True

    def visit_If(self, node: ast.If) -> None:
        if isinstance(node.test, ast.Name) and node.test.id == "TYPE_CHECKING":
            self._in_type_checking = True
            for child in node.body:
                self.visit(child)
            self._in_type_checking = False
            return

        self.generic_visit(node)

    def visit_Import(self, node: ast.Import) -> None:
        if self._in_type_checking:
            return
        for alias in node.names:
            if not self._is_allowed_import(alias.name):
                self.errors.append(f"Import {alias.name} not allowed")

    def visit_ImportFrom(self, node: ast.ImportFrom) -> None:
        if self._in_type_checking:
            return
        if not self._is_allowed_import(node.module):
            self.errors.append(f"ImportFrom {node.module} not allowed")


@pytest.mark.parametrize(
    "file_path",
    [p for d in CHECK_DIRS for p in iter_python_files(d) if p.name != "__init__.py"],
)
def test_import_boundaries(file_path: Path) -> None:
    tree = ast.parse(file_path.read_text(encoding="utf-8"))
    allowed_prefixes = (
        ALLOWED_TEMPLATES_FIRECUBE_PREFIXES
        if "src/firecube/ingestor/templates" in str(file_path)
        else ALLOWED_PLUGINS_FIRECUBE_PREFIXES
    )
    visitor = ImportVisitor(allowed_prefixes=allowed_prefixes)
    visitor.visit(tree)
    if visitor.errors:
        pytest.fail(f"Architecture violation in {file_path}: {visitor.errors}")
