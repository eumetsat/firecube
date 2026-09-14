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

"""Tests for --time-range option scaffold on `chunks delete`."""

from __future__ import annotations

import click
import pytest
from click.testing import CliRunner

from firecube.cli.chunks._common import parse_time_range
from firecube.cli.chunks._delete import delete_cmd


def test_time_range_appears_in_help() -> None:
    """--time-range must be listed in `chunks delete --help` output."""
    runner = CliRunner()
    result = runner.invoke(delete_cmd, ["--help"])
    assert result.exit_code == 0
    assert "time-range" in result.output


def test_time_range_parses_valid_date_range() -> None:
    """parse_time_range returns a 2-tuple for a valid START:END string."""
    result = parse_time_range("2024-01-01:2024-01-31")
    assert result is not None
    assert isinstance(result, tuple)
    assert len(result) == 2
    start, end = result
    assert start == "2024-01-01"
    assert end == "2024-01-31"


def test_time_range_returns_none_for_none_input() -> None:
    """parse_time_range returns None when given None."""
    assert parse_time_range(None) is None


def test_time_range_returns_none_for_empty_string() -> None:
    """parse_time_range returns None when given an empty string."""
    assert parse_time_range("") is None


def test_time_range_rejects_bad_input() -> None:
    """parse_time_range raises click.BadParameter for malformed input."""
    with pytest.raises(click.BadParameter):
        parse_time_range("bad")


def test_time_range_rejects_missing_end() -> None:
    """parse_time_range raises click.BadParameter when END is missing."""
    with pytest.raises(click.BadParameter):
        parse_time_range("2024-01-01:")
