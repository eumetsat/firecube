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

"""Reference pages must render from docstrings, not carry static API prose.

Invariant: a page under ``docs/reference/`` consists of mkdocstrings ``:::``
directives, short connective glue, and links. The behavior, constraints, and
examples of a public symbol live in that symbol's docstring, where they render
automatically and cannot drift from the code. A Python code fence on a
reference page is hand-maintained API prose by definition: it duplicates what
a docstring should say and goes stale silently (observed twice before this
guard existed: a failure-modes table naming the wrong exception class, and a
callable-payload rule contradicting the docstring).

To document an example, put it in the docstring's ``Examples:`` section and
let the existing ``:::`` directive render it. A page that genuinely needs an
inline fence must be listed in ``INTENTIONALLY_STATIC`` with a reason; the
list starts empty and every addition is a conscious exception.
"""

from __future__ import annotations

from pathlib import Path

import pytest

pytestmark = pytest.mark.docs_static

_REPO_ROOT = Path(__file__).resolve().parents[2]
REFERENCE_DIR = _REPO_ROOT / "docs" / "reference"

# Reference pages allowed to carry Python code fences. Each entry needs a
# reason; remove the entry when the page's examples move into docstrings.
INTENTIONALLY_STATIC: dict[str, str] = {}


def test_reference_pages_carry_no_python_fences() -> None:
    offenders: list[str] = []
    for page in sorted(REFERENCE_DIR.rglob("*.md")):
        relative = page.relative_to(_REPO_ROOT).as_posix()
        if page.name in INTENTIONALLY_STATIC:
            continue
        for line_number, line in enumerate(page.read_text().splitlines(), start=1):
            if line.lstrip().startswith(("```python", "```py")):
                offenders.append(f"{relative}:{line_number}")
    assert not offenders, (
        "Python code fences on reference pages are hand-maintained API prose "
        "and drift from the code. Move the example into the documented "
        "symbol's docstring (Examples: section) so the existing ':::' "
        "directive renders it, or add the page to INTENTIONALLY_STATIC with "
        f"a reason. Offenders: {offenders}"
    )


def test_static_allowlist_stays_current() -> None:
    stale = [name for name in INTENTIONALLY_STATIC if not (REFERENCE_DIR / name).exists()]
    assert not stale, f"INTENTIONALLY_STATIC lists pages that no longer exist: {stale}"
