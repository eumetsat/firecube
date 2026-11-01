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

"""Validation and splitting of input-file filters and STAC search filters."""

from __future__ import annotations

import fnmatch
from collections.abc import Iterable, Mapping
from typing import Any

from firecube.core.discovery._types import SourceItem
from firecube.core.discovery.errors import STAC_SEARCH_KEYS, StacSearchError
from firecube.core.errors import Message, Param
from firecube.core.uris import href_basename

LEGACY_INPUT_PATTERNS_MESSAGE = Message(
    "include_patterns has been removed; use {input_filters} "
    'or input_filters = ["*.csv"] in configuration.',
    input_filters=Param("input_filters", '["*.csv"]'),
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


def narrow_by_basename(
    items: Iterable[SourceItem], patterns: Iterable[str] | None
) -> list[SourceItem]:
    """Narrow STAC items on the basename of their href.

    With a positive pattern an item is kept only when one positive pattern matches; a
    ``!`` pattern excludes, whatever the order. Matching is case-sensitive ``fnmatch``,
    as for file trees. ``None`` or no patterns keeps every item.
    """
    included, excluded = split_input_filters(patterns)
    kept: list[SourceItem] = []
    for item in items:
        name = href_basename(item.href)
        if any(fnmatch.fnmatchcase(name, pattern) for pattern in excluded):
            continue
        if included and not any(fnmatch.fnmatchcase(name, pattern) for pattern in included):
            continue
        kept.append(item)
    return kept


def validate_stac_search(value: object) -> dict[str, Any]:
    """Validate a STAC search filter and return it as a new dict.

    The filter is a mapping whose keys are any of ``datetime``, ``bbox``,
    ``intersects`` and ``ids``. Values are passed to the STAC API as given.

    Raises:
        StacSearchError: If ``value`` is not a mapping, or holds another key;
            ``collections`` included, since the collection comes from the source URI.
    """
    if not isinstance(value, Mapping):
        raise StacSearchError()
    refused = [str(key) for key in value if key not in STAC_SEARCH_KEYS]
    if refused:
        raise StacSearchError(refused)
    return {str(key): item for key, item in value.items()}
