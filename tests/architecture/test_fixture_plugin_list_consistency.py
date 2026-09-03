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

# tests/architecture/test_fixture_plugin_list_consistency.py
"""Architecture invariant: every fixture-plugin install list must
stay synchronized with the actual set of test-fixture plugin directories.

Prevents the drift class where a test file references a fixture whose
plugin package is not listed in the fail-fast conftest guard, in
plans/TEST.md, or in AGENTS.md - resulting in an ``Unknown plugin``
runtime error rather than a clean fail-fast message.
"""

from __future__ import annotations

import re
import tomllib
from pathlib import Path

import pytest

pytestmark = pytest.mark.architecture

REPO_ROOT = Path(__file__).resolve().parents[2]
FIXTURES_DIR = REPO_ROOT / "tests" / "fixtures"

# Fixture directories that register a ``firecube.plugins`` entry point but
# are NOT part of the pre-install required set. Reason: they are installed
# dynamically at test time (e.g., ``demo_plugin`` via
# ``tests/integration/test_plugin_lifecycle.py``) or serve as
# examples/documentation, not as fixtures the fail-fast guard should
# enforce. Filter by the ``_test_plugin`` suffix convention.
EXCLUDED_FIXTURE_DIRS: frozenset[str] = frozenset(
    {
        "demo_plugin",  # installed dynamically by test_plugin_lifecycle.py
    }
)


def _actual_fixture_plugins() -> set[str]:
    """Importable fixture modules shipped by tests/fixtures/ distributions.

    A fixture distribution is any directory under tests/fixtures/ (not in
    ``EXCLUDED_FIXTURE_DIRS``) whose pyproject.toml registers at least one
    ``firecube.plugins`` entry point. One distribution may ship several
    modules (the unified ``firecube_test_plugins`` package ships all of
    them); the guard in conftest imports MODULES, so the comparison set is
    the module names from each distribution's hatch ``packages`` list.
    """
    plugins: set[str] = set()
    for path in FIXTURES_DIR.iterdir():
        if not path.is_dir():
            continue
        if path.name in EXCLUDED_FIXTURE_DIRS:
            continue
        pyproject = path / "pyproject.toml"
        if not pyproject.exists():
            continue
        data = tomllib.loads(pyproject.read_text())
        entry_points = data.get("project", {}).get("entry-points", {}).get("firecube.plugins", {})
        if not entry_points:
            continue
        packages = (
            data.get("tool", {})
            .get("hatch", {})
            .get("build", {})
            .get("targets", {})
            .get("wheel", {})
            .get("packages", [f"src/{path.name}"])
        )
        for pkg in packages:
            plugins.add(pkg.rsplit("/", 1)[-1])
    return plugins


def _actual_fixture_distributions() -> set[str]:
    """Directories under tests/fixtures/ that must appear in install blocks:
    any non-excluded directory whose pyproject.toml registers at least one
    ``firecube.plugins`` entry point."""
    dists: set[str] = set()
    for path in FIXTURES_DIR.iterdir():
        if not path.is_dir() or path.name in EXCLUDED_FIXTURE_DIRS:
            continue
        pyproject = path / "pyproject.toml"
        if not pyproject.exists():
            continue
        data = tomllib.loads(pyproject.read_text())
        if data.get("project", {}).get("entry-points", {}).get("firecube.plugins", {}):
            dists.add(path.name)
    return dists


def _conftest_required_plugins() -> set[str]:
    """Module names in tests/conftest.py's required_plugins tuple."""
    conftest = (REPO_ROOT / "tests" / "conftest.py").read_text()
    match = re.search(r"required_plugins = \((.*?)\)", conftest, re.S)
    assert match is not None, "required_plugins tuple not found in tests/conftest.py"
    return set(re.findall(r'"([A-Za-z0-9_]+)"', match.group(1)))


def _install_block_plugins(path: Path) -> set[str]:
    """Names in a ``uv pip install -e tests/fixtures/<name>`` block."""
    text = path.read_text()
    matches = re.findall(r"uv pip install -e tests/fixtures/([a-zA-Z0-9_]+)", text)
    return set(matches)


def test_conftest_lists_match_actual_fixtures() -> None:
    actual = _actual_fixture_plugins()
    conftest = _conftest_required_plugins()
    missing = actual - conftest
    assert not missing, (
        f"tests/conftest.py required_plugins is missing fixtures: "
        f"{sorted(missing)}. Add them to keep the fail-fast guard "
        f"in sync with tests/fixtures/."
    )


def test_test_md_install_block_matches_actual_fixtures() -> None:
    actual = _actual_fixture_distributions()
    listed = _install_block_plugins(REPO_ROOT / "plans" / "TEST.md")
    missing = actual - listed
    assert not missing, (
        f"plans/TEST.md install block is missing fixtures: "
        f"{sorted(missing)}. Update the block to keep contributor "
        f"instructions in sync with tests/fixtures/."
    )


@pytest.mark.parametrize(
    "workflow",
    [".github/workflows/ci.yml", ".github/workflows/publish.yml"],
)
def test_workflow_install_blocks_match_actual_fixtures(workflow: str) -> None:
    actual = _actual_fixture_distributions()
    listed = _install_block_plugins(REPO_ROOT / workflow)
    missing = actual - listed
    assert not missing, (
        f"{workflow} fixture install block(s) are missing fixtures: "
        f"{sorted(missing)}. Add a 'uv pip install -e tests/fixtures/<name>' "
        f"line to every install block so CI stays in sync with tests/fixtures/."
    )


def test_agents_md_install_block_matches_actual_fixtures() -> None:
    actual = _actual_fixture_distributions()
    listed = _install_block_plugins(REPO_ROOT / "AGENTS.md")
    missing = actual - listed
    assert not missing, (
        f"AGENTS.md install block is missing fixtures: "
        f"{sorted(missing)}. Update the block to keep the AI-agent "
        f"install instructions in sync with tests/fixtures/."
    )
