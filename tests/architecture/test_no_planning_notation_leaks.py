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

"""Architecture lint: reject planning-notation leaks in src/firecube."""

from __future__ import annotations

import re
from pathlib import Path

import pytest

pytestmark = pytest.mark.architecture

REPO_ROOT = Path(__file__).resolve().parents[2]
SRC_ROOT = REPO_ROOT / "src" / "firecube"

LEAK_PATTERNS = [
    r"Anchor plan",
    r"plan task \d",
    r"task \d\.\d",
    r"§\d{2,}",
    r"per PR\d",
    r"PR#\d+",
    r"see plans/",
    r"from \.sisyphus",
    r"see \.armagan",
]


def _collect_hits(paths: list[Path]) -> list[str]:
    compiled = [re.compile(pattern) for pattern in LEAK_PATTERNS]
    hits: list[str] = []

    for path in paths:
        text = path.read_text(encoding="utf-8")
        for line_number, line in enumerate(text.splitlines(), start=1):
            hits.extend(
                f"{path.relative_to(REPO_ROOT)}:{line_number}: {line.strip()}"
                for pattern in compiled
                if pattern.search(line)
            )

    return hits


def _python_sources(root: Path) -> list[Path]:
    return sorted(path for path in root.rglob("*.py") if "__pycache__" not in path.parts)


def test_no_planning_notation_leaks_in_src_firecube() -> None:
    """No shipped source under ``src/firecube`` should carry planning notation."""
    hits = _collect_hits(_python_sources(SRC_ROOT))

    assert hits == [], "planning-notation leaks still present:\n" + "\n".join(hits)


def test_false_positive_sources_are_not_flagged() -> None:
    """CF-1.8 citations and Prometheus mentions are not planning notation."""
    hits = _collect_hits(
        [
            SRC_ROOT / "core" / "cf" / "check_ids.py",
            SRC_ROOT / "core" / "observability" / "metrics.py",
        ]
    )

    assert hits == [], "false positives were flagged:\n" + "\n".join(hits)
