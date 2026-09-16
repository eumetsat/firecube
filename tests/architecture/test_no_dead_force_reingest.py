# Copyright 2025-2026 EUMETSAT
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Invariants against dead ``force_reingest`` plumbing in the append path.

The resume service has no ``force_reingest`` parameter or write-only
private state for it. The tests inspect
signatures and the AST rather than searching for substrings
(TESTING_STANDARDS.md: structured inspection over substring matching).
"""

from __future__ import annotations

import ast
import inspect
from pathlib import Path
from types import ModuleType

from firecube.ingestor.runtime.zarr import append_services
from firecube.ingestor.runtime.zarr.strategies import append as append_strategy

SRC_ROOT = Path(__file__).parents[2] / "src"
TESTS_ROOT = Path(__file__).parents[1]

_APPEND_MODULES: tuple[ModuleType, ...] = (append_services, append_strategy)


def _self_attribute_name(node: ast.AST) -> str | None:
    if (
        isinstance(node, ast.Attribute)
        and isinstance(node.value, ast.Name)
        and node.value.id == "self"
    ):
        return node.attr
    return None


def _write_only_instance_attributes(module: ModuleType) -> dict[str, set[str]]:
    """Map class name -> private ``self.<name>`` set in ``__init__`` but never loaded.

    Public attributes are the class's interface and may legitimately be read
    only by callers; private ones are the class's own state, so a private
    attribute nobody in the class loads is dead.
    """
    assert module.__file__ is not None
    tree = ast.parse(Path(module.__file__).read_text())
    offenders: dict[str, set[str]] = {}
    for cls in (node for node in ast.walk(tree) if isinstance(node, ast.ClassDef)):
        inits = [
            node
            for node in cls.body
            if isinstance(node, ast.FunctionDef) and node.name == "__init__"
        ]
        if not inits:
            continue
        assigned = {
            name
            for node in ast.walk(inits[0])
            if isinstance(node, ast.Attribute)
            and isinstance(node.ctx, ast.Store)
            and (name := _self_attribute_name(node)) is not None
            and name.startswith("_")
        }
        loaded = {
            name
            for node in ast.walk(cls)
            if isinstance(node, ast.Attribute)
            and isinstance(node.ctx, ast.Load)
            and (name := _self_attribute_name(node)) is not None
        }
        dead = assigned - loaded
        if dead:
            offenders[cls.name] = dead
    return offenders


def test_no_write_only_private_attributes_in_append_classes() -> None:
    """every private attribute set in ``__init__`` is read somewhere in its class."""
    offenders = {
        module.__name__: dead
        for module in _APPEND_MODULES
        if (dead := _write_only_instance_attributes(module))
    }
    assert not offenders, f"write-only private instance attributes: {offenders}"


def test_force_reingest_not_a_resume_service_parameter() -> None:
    """``AppendResumeService`` takes no ``force_reingest`` at construction or write."""
    service = append_services.AppendResumeService
    for fn in (service.__init__, service.prepare_write):
        assert "force_reingest" not in inspect.signature(fn).parameters, (
            f"{fn.__qualname__} still accepts force_reingest"
        )


def test_state_by_slot_removed() -> None:
    """AppendClassification.state_by_slot removed."""
    for py in SRC_ROOT.rglob("*.py"):
        assert "state_by_slot" not in py.read_text(), f"state_by_slot found in {py}"


def test_locking_test_deleted() -> None:
    """test_append_strategy_signature.py deleted."""
    locking = TESTS_ROOT / "unit" / "test_append_strategy_signature.py"
    assert not locking.exists(), "test_append_strategy_signature.py still exists"
