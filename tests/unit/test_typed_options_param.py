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

import click
import pytest

from firecube.cli._typed_options import TypedOptionsParam


def test_valid_string_option() -> None:
    assert TypedOptionsParam().convert("key=value", None, None) == ("key", "value")


def test_valid_int_option() -> None:
    assert TypedOptionsParam("cli_test_plugin").convert("test_int=42", None, None) == (
        "test_int",
        42,
    )


def test_empty_key_rejected() -> None:
    with pytest.raises(click.BadParameter, match="Option key cannot be empty"):
        TypedOptionsParam().convert("=v", None, None)


def test_empty_value_rejected() -> None:
    with pytest.raises(click.BadParameter, match="Option value for 'k' cannot be empty"):
        TypedOptionsParam().convert("k=", None, None)


def test_no_equals_rejected() -> None:
    with pytest.raises(click.BadParameter, match="Use key=value syntax"):
        TypedOptionsParam().convert("k", None, None)


def test_unknown_key_rejected_with_context() -> None:
    with pytest.raises(click.BadParameter) as exc_info:
        TypedOptionsParam("cli_test_plugin").convert("unknown=value", None, None)

    message = str(exc_info.value)
    assert "Unknown option 'unknown' for plugin 'cli_test_plugin'" in message
    assert "test_int" in message
    assert "test_str" in message
    assert "test_bool" in message


def test_invalid_value_type_rejected_with_expected_type() -> None:
    with pytest.raises(click.BadParameter, match="Invalid integer for test_int"):
        TypedOptionsParam("cli_test_plugin").convert("test_int=not-an-int", None, None)


@pytest.mark.parametrize(
    ("key", "flag"),
    [
        ("write_mode", "--write-mode"),
        ("slot_start", "--slot-start"),
        ("slot_end", "--slot-end"),
        ("slot_size", "--slot-size"),
        ("slot_group", "--slot-group"),
    ],
)
def test_typed_flag_owned_keys_rejected_via_option(key: str, flag: str) -> None:
    """Keys owned by dedicated `firecube ingest` flags must not pass through
    `--option`: the free-form merge happens after typed-flag resolution, so
    they would silently override the explicit flag (plans/STYLE.md,
    free-form option overload)."""
    with pytest.raises(click.BadParameter) as exc_info:
        TypedOptionsParam("cli_test_plugin").convert(f"{key}=1", None, None)
    message = str(exc_info.value)
    assert flag in message, f"remediation should point at {flag}: {message}"


def test_engine_only_options_still_accepted() -> None:
    """Engine options without a dedicated flag (the documented --option
    surface, e.g. force_reingest) must keep working."""
    assert TypedOptionsParam("cli_test_plugin").convert("force_reingest=true", None, None) == (
        "force_reingest",
        True,
    )
