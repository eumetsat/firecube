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
    """Directories under tests/fixtures/ whose pyproject.toml registers
    at least one ``firecube.plugins`` entry point AND whose name is not
    in ``EXCLUDED_FIXTURE_DIRS``.

    The naming convention is that pre-install required fixtures end in
    ``_test_plugin``; the exclusion list catches any historical outliers.
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
        if entry_points:
            plugins.add(path.name)
    return plugins


def _conftest_required_plugins() -> set[str]:
    """Names in tests/conftest.py's required_plugins tuple (first element
    of each tuple pair)."""
    conftest = (REPO_ROOT / "tests" / "conftest.py").read_text()
    matches = re.findall(r'\(\s*"([^"]+)"\s*,\s*"[^"]+"\s*\)', conftest)
    return set(matches)


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
    actual = _actual_fixture_plugins()
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
    actual = _actual_fixture_plugins()
    listed = _install_block_plugins(REPO_ROOT / workflow)
    missing = actual - listed
    assert not missing, (
        f"{workflow} fixture install block(s) are missing fixtures: "
        f"{sorted(missing)}. Add a 'uv pip install -e tests/fixtures/<name>' "
        f"line to every install block so CI stays in sync with tests/fixtures/."
    )


def test_agents_md_install_block_matches_actual_fixtures() -> None:
    actual = _actual_fixture_plugins()
    listed = _install_block_plugins(REPO_ROOT / "AGENTS.md")
    missing = actual - listed
    assert not missing, (
        f"AGENTS.md install block is missing fixtures: "
        f"{sorted(missing)}. Update the block to keep the AI-agent "
        f"install instructions in sync with tests/fixtures/."
    )
