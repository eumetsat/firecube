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

"""Optional generic string-pattern parsing."""

from __future__ import annotations

from typing import Any


def parse_pattern(pattern: str, text: str) -> dict[str, Any]:
    """Parse a complete string with a Trollsift format pattern.

    Loads Trollsift only when called. No filesystem access, basename extraction,
    field renaming, source filtering, or timestamp interpretation is performed.
    Datetimes without timezone information remain timezone-naive.

    Args:
        pattern: Trollsift pattern using named format fields.
        text: Complete string to match, including any caller-supplied path.

    Returns:
        Parsed fields with Trollsift's original names, values, and types.

    Raises:
        ImportError: If Trollsift is missing; install ``firecube[patterns]``.
            Other import failures propagate unchanged.
        ValueError: If the string does not fully match or a field is invalid.
            Malformed patterns propagate Trollsift's parsing errors unchanged.

    Examples:
        Parse a generic identifier and integer field:

            >>> parse_pattern("invoice_{customer}_{number:04d}.txt", "invoice_acme_0042.txt")
            {'customer': 'acme', 'number': 42}
    """
    try:
        from trollsift import parse
    except ModuleNotFoundError as exc:
        if exc.name != "trollsift":
            raise
        raise ImportError(
            "Pattern parsing requires Trollsift; install firecube[patterns]."
        ) from exc
    return parse(pattern, text, full_match=True)
