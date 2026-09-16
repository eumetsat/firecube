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

from __future__ import annotations

import re

import pytest

from firecube.ingestor.config.coercion import coerce_cli_value
from firecube.ingestor.errors import ConfigurationError

pytestmark = pytest.mark.unit

_HINT_PATTERN = re.compile(r"Try: --option 'example_list=(?P<value>.*)'$")


def _hinted_value(message: str) -> str:
    match = _HINT_PATTERN.search(message)
    assert match is not None, message
    return match.group("value")


def test_bare_glob_accepted() -> None:
    """bare glob *.csv accepted as ["*.csv"] via coerce_cli_value."""
    result = coerce_cli_value("*.csv", list[str] | None, "example_list")
    assert result == ["*.csv"]


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("[*.csv]", ["*.csv"]),
        ("[*.csv,*.txt]", ["*.csv", "*.txt"]),
        ("[ *.csv , '*.txt' ]", ["*.csv", "*.txt"]),
    ],
)
def test_unquoted_list_hint_round_trips(raw: str, expected: list[str]) -> None:
    """the quoting hint is a value coerce_cli_value accepts as the intended list."""
    with pytest.raises(ConfigurationError, match="quoting") as excinfo:
        coerce_cli_value(raw, list[str] | None, "example_list")

    hinted = _hinted_value(str(excinfo.value))
    assert coerce_cli_value(hinted, list[str] | None, "example_list") == expected


def test_unquoted_list_without_elements_hints_placeholder() -> None:
    """a bracket value with no elements renders a generic placeholder, not an empty list."""
    with pytest.raises(ConfigurationError, match="quoting") as excinfo:
        coerce_cli_value("[,]", list[str] | None, "example_list")

    assert _hinted_value(str(excinfo.value)) == '["<value>", ...]'


def test_valid_json_list_unchanged() -> None:
    """Verified-correct: JSON list still accepted."""
    result = coerce_cli_value('["*.csv"]', list[str] | None, "example_list")
    assert result == ["*.csv"]
