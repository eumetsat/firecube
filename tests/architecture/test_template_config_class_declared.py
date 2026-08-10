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

"""RF-11: template-config consumers must declare their template config class."""

from __future__ import annotations

import ast
from dataclasses import dataclass
from pathlib import Path

import pytest

pytestmark = pytest.mark.architecture

_REPO_ROOT = Path(__file__).resolve().parents[2]
_SRC_ROOT = _REPO_ROOT / "src" / "firecube" / "ingestor"


@dataclass(frozen=True)
class ClassInfo:
    node: ast.ClassDef
    path: Path
    bases: tuple[str, ...]


def _parse(path: Path) -> ast.AST:
    return ast.parse(path.read_text(encoding="utf-8"), filename=str(path))


def _name(node: ast.AST) -> str | None:
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        return node.attr
    if isinstance(node, ast.Subscript):
        return _name(node.value)
    if isinstance(node, ast.Call):
        return _name(node.func)
    return None


def _decorator_name(node: ast.AST) -> str | None:
    return _name(node.func) if isinstance(node, ast.Call) else _name(node)


class ClassCollector(ast.NodeVisitor):
    def __init__(self, path: Path) -> None:
        self.path = path
        self.classes: list[ClassInfo] = []

    def visit_ClassDef(self, node: ast.ClassDef) -> None:
        bases = tuple(base for base in (_name(base) for base in node.bases) if base is not None)
        self.classes.append(ClassInfo(node=node, path=self.path, bases=bases))
        self.generic_visit(node)


class SelfTemplateConfigFinder(ast.NodeVisitor):
    """Find method-body reads of the instance template configuration."""

    def __init__(self) -> None:
        self.lines: list[int] = []

    def visit_Attribute(self, node: ast.Attribute) -> None:
        if (
            isinstance(node.value, ast.Name)
            and node.value.id == "self"
            and node.attr == "template_config"
        ):
            self.lines.append(node.lineno)
        self.generic_visit(node)

    def visit_Call(self, node: ast.Call) -> None:
        if (
            isinstance(node.func, ast.Name)
            and node.func.id == "getattr"
            and len(node.args) >= 2
            and isinstance(node.args[0], ast.Name)
            and node.args[0].id == "self"
            and isinstance(node.args[1], ast.Constant)
            and node.args[1].value == "template_config"
        ):
            self.lines.append(node.lineno)
        self.generic_visit(node)


def _method_template_config_lines(node: ast.ClassDef) -> list[int]:
    lines: list[int] = []
    for stmt in node.body:
        if isinstance(stmt, ast.FunctionDef | ast.AsyncFunctionDef):
            finder = SelfTemplateConfigFinder()
            finder.visit(stmt)
            lines.extend(finder.lines)
    return sorted(set(lines))


def _declares_non_none_template_config_class(node: ast.ClassDef) -> bool:
    for stmt in node.body:
        value: ast.AST | None = None
        targets: list[ast.AST] = []
        if isinstance(stmt, ast.Assign):
            value = stmt.value
            targets = list(stmt.targets)
        elif isinstance(stmt, ast.AnnAssign):
            value = stmt.value
            targets = [stmt.target]

        if value is None:
            continue
        for target in targets:
            if isinstance(target, ast.Name) and target.id == "template_config_class":
                return not (isinstance(value, ast.Constant) and value.value is None)
    return False


def _defines_process_batch(node: ast.ClassDef) -> bool:
    for stmt in node.body:
        if (
            isinstance(stmt, ast.FunctionDef | ast.AsyncFunctionDef)
            and stmt.name == "_process_batch"
        ):
            decorators = {_decorator_name(decorator) for decorator in stmt.decorator_list}
            return "abstractmethod" not in decorators
    return False


def _has_abstract_methods(node: ast.ClassDef) -> bool:
    for stmt in node.body:
        if not isinstance(stmt, ast.FunctionDef | ast.AsyncFunctionDef):
            continue
        decorators = {_decorator_name(decorator) for decorator in stmt.decorator_list}
        if "abstractmethod" in decorators:
            return True
    return False


def _collect_classes() -> dict[str, ClassInfo]:
    classes: dict[str, ClassInfo] = {}
    for path in sorted(_SRC_ROOT.rglob("*.py")):
        collector = ClassCollector(path)
        collector.visit(_parse(path))
        for info in collector.classes:
            classes[info.node.name] = info
    return classes


def _inherits_from_baseingestor(
    class_name: str,
    classes: dict[str, ClassInfo],
    seen: frozenset[str] = frozenset(),
) -> bool:
    if class_name in seen:
        return False
    info = classes.get(class_name)
    if info is None:
        return False
    if "BaseIngestor" in info.bases:
        return True
    next_seen = seen | {class_name}
    return any(_inherits_from_baseingestor(base, classes, next_seen) for base in info.bases)


def _is_exempt_abstract(info: ClassInfo, classes: dict[str, ClassInfo]) -> bool:
    if info.node.name == "BaseIngestor":
        return True
    if any("ABC" in base for base in info.bases):
        return True
    return _has_abstract_methods(info.node) and not _defines_process_batch(info.node)


def test_template_config_consumers_declare_template_config_class(
    request: pytest.FixtureRequest,
) -> None:
    classes = _collect_classes()
    offenders: list[str] = []
    checked: list[str] = []
    skipped: list[str] = []

    for info in sorted(classes.values(), key=lambda item: (str(item.path), item.node.lineno)):
        if not _inherits_from_baseingestor(info.node.name, classes):
            continue

        refs = _method_template_config_lines(info.node)
        rel_path = info.path.relative_to(_REPO_ROOT).as_posix()
        if _is_exempt_abstract(info, classes):
            skipped.append(f"SKIP {info.node.name} at {rel_path}:{info.node.lineno}")
            continue
        if not refs:
            skipped.append(
                f"SKIP {info.node.name} at {rel_path}:{info.node.lineno} no template_config refs"
            )
            continue

        checked.append(f"CHECK {info.node.name} at {rel_path}:{info.node.lineno} refs={refs}")
        if not _declares_non_none_template_config_class(info.node):
            offenders.append(
                f"{info.node.name} at {rel_path}:{info.node.lineno} "
                "references self.template_config but does not declare template_config_class"
            )

    if int(getattr(request.config.option, "verbose", 0) or 0) > 0:
        print("RF-11 checked classes:")
        print("\n".join(checked) if checked else "<none>")
        print("RF-11 skipped classes:")
        print("\n".join(skipped) if skipped else "<none>")

    assert not offenders, "\n".join(offenders)
