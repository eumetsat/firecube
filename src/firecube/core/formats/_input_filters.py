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

"""Validation and splitting for explicit input-file filters."""

from __future__ import annotations

from collections.abc import Iterable, Mapping

LEGACY_INPUT_PATTERNS_MESSAGE = (
    "include_patterns has been removed; use --input-filters '[\"*.csv\"]' "
    'or input_filters = ["*.csv"] in configuration.'
)


def reject_legacy_input_patterns(options: Mapping[str, object]) -> None:
    """Reject the removed option before unknown-key checks or CLI overrides."""
    if "include_patterns" in options:
        raise ValueError(LEGACY_INPUT_PATTERNS_MESSAGE)


def validate_input_filters(value: object) -> list[str] | None:
    """Validate a configured filter list without coercing strings or members."""
    if value is None:
        return None
    if not isinstance(value, list):
        raise ValueError(
            'input_filters must be a list of strings, for example ["*.csv", "!draft_*"]'
        )
    for pattern in value:
        if not isinstance(pattern, str) or not pattern or pattern == "!":
            raise ValueError(
                "input_filters entries must be non-empty strings; '!' needs a filename glob"
            )
    return list(value)


def split_input_filters(patterns: Iterable[str] | None) -> tuple[tuple[str, ...], tuple[str, ...]]:
    """Separate inclusion and exclusion globs, interpreting each prefix once."""
    filters = validate_input_filters(list(patterns) if patterns is not None else None)
    included: list[str] = []
    excluded: list[str] = []
    for pattern in filters or ():
        if pattern.startswith("\\!"):
            included.append(pattern[1:])
        elif pattern.startswith("!"):
            excluded.append(pattern[1:])
        else:
            included.append(pattern)
    return tuple(included), tuple(excluded)
