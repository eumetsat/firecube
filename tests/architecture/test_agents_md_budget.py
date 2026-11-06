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

"""Budget gate for the root ``AGENTS.md``, which is onboarding and a router, not a ledger.

The file stays at or under 200 lines and 12 KiB, no bullet exceeds 60 words, and no
line carries a ``path:NNN`` reference. Fenced code blocks are ignored by the bullet
and reference checks. Each failure lists the offending line numbers so the maintainer
can act without re-measuring.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

pytestmark = pytest.mark.architecture

_AGENTS_MD = Path(__file__).resolve().parents[2] / "AGENTS.md"
MAX_LINES = 200
MAX_BYTES = 12 * 1024
MAX_BULLET_WORDS = 60

_BULLET = re.compile(r"^(\s*)- ")
_FENCE = re.compile(r"^\s*(```|~~~)")
_URL = re.compile(r"\b[a-zA-Z][a-zA-Z0-9+.-]*://\S+")
_LINE_REF = re.compile(r"([\w./-]+):(\d+)")


def _prose_lines(text: str) -> list[tuple[int, str]]:
    """Return ``(1-based line number, line)`` for every line outside a fenced block."""
    lines: list[tuple[int, str]] = []
    in_fence = False
    for number, line in enumerate(text.splitlines(), start=1):
        if _FENCE.match(line):
            in_fence = not in_fence
        elif not in_fence:
            lines.append((number, line))
    return lines


def _long_bullets(text: str) -> list[tuple[int, str, int]]:
    """Return ``(start line, first 60 characters, word count)`` of each bullet over the cap."""
    bullets: list[tuple[int, str, list[str]]] = []
    indent = -1
    previous = 0
    for number, line in _prose_lines(text):
        marker = _BULLET.match(line)
        continues = (
            indent >= 0
            and number == previous + 1
            and line.strip()
            and not line.lstrip().startswith("#")
            and len(line) - len(line.lstrip()) > indent
        )
        if marker:
            indent = len(marker.group(1))
            bullets.append((number, line.strip(), line.split()[1:]))
        elif continues:
            bullets[-1][2].extend(line.split())
        else:
            indent = -1
        previous = number
    return [
        (start, first[:60], len(words))
        for start, first, words in bullets
        if len(words) > MAX_BULLET_WORDS
    ]


def _line_references(text: str) -> list[tuple[int, str]]:
    """Return ``(line number, token)`` for each ``path:NNN`` outside fences and URLs."""
    found: list[tuple[int, str]] = []
    for number, line in _prose_lines(text):
        for match in _LINE_REF.finditer(_URL.sub("", line)):
            path = match.group(1)
            if "/" in path or path.endswith((".py", ".md")):
                found.append((number, match.group(0)))
    return found


def _line_count_failure(path: Path) -> str:
    count = path.read_bytes().count(b"\n")
    return f"{path.name} has {count} lines; the budget is {MAX_LINES}." if count > MAX_LINES else ""


def _size_failure(path: Path) -> str:
    size = len(path.read_bytes())
    return f"{path.name} is {size} bytes; the budget is {MAX_BYTES}." if size > MAX_BYTES else ""


def _bullet_failure(path: Path) -> str:
    rows = [
        f"  line {n}: {w} words: {first!r}"
        for n, first, w in _long_bullets(path.read_text(encoding="utf-8"))
    ]
    if not rows:
        return ""
    return f"{len(rows)} bullets exceed {MAX_BULLET_WORDS} words:\n" + "\n".join(rows)


def _reference_failure(path: Path) -> str:
    rows = [f"  line {n}: {token}" for n, token in _line_references(path.read_text("utf-8"))]
    if not rows:
        return ""
    return f"{len(rows)} path:NNN references; name the symbol instead:\n" + "\n".join(rows)


def test_agents_md_has_at_most_200_lines() -> None:
    assert not (failure := _line_count_failure(_AGENTS_MD)), failure


def test_agents_md_is_at_most_12_kib() -> None:
    assert not (failure := _size_failure(_AGENTS_MD)), failure


def test_agents_md_has_no_bullet_over_60_words() -> None:
    assert not (failure := _bullet_failure(_AGENTS_MD)), failure


def test_agents_md_has_no_path_line_references() -> None:
    assert not (failure := _reference_failure(_AGENTS_MD)), failure


_FENCED = "- fenced src/x/y.py:42 " + "many " * 70
_COMPLIANT = f"""# Title

- short bullet one
  with a continuation line
- bullet naming `src/x/y.py` and https://host.example:8080/a/b:42 only
  - nested bullet

```
{_FENCED}
```
"""


def test_budget_checks_pass_a_compliant_document(tmp_path: Path) -> None:
    doc = tmp_path / "AGENTS.md"
    doc.write_text(_COMPLIANT, encoding="utf-8")
    checks = (_line_count_failure, _size_failure, _bullet_failure, _reference_failure)
    assert [check(doc) for check in checks] == ["", "", "", ""]


def test_budget_checks_flag_each_violation_with_its_line_numbers(tmp_path: Path) -> None:
    doc = tmp_path / "AGENTS.md"
    doc.write_text(
        "- " + "word " * 61 + "\n"
        "- short\n"
        "  " + "word " * 30 + "\n"
        "  " + "word " * 31 + "\n"
        "\n"
        "See `claims.py:105-124` and src/x/y.py:42.\n" + "filler\n" * MAX_LINES + "x" * MAX_BYTES,
        encoding="utf-8",
    )
    assert "has 206 lines" in _line_count_failure(doc)
    assert "bytes; the budget is 12288" in _size_failure(doc)
    bullets = _bullet_failure(doc)
    assert "line 1: 61 words: '- word word" in bullets
    assert "line 2: 62 words: '- short'" in bullets
    refs = _reference_failure(doc)
    assert "line 6: claims.py:105" in refs
    assert "line 6: src/x/y.py:42" in refs
