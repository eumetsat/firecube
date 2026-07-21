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

"""Strict UTC ISO-8601 → numpy.datetime64 helpers. Package-private."""

from __future__ import annotations

from collections.abc import Iterable
from datetime import datetime, timedelta

import numpy as np


def _parse_iso_utc(s: str) -> datetime:
    """Parse a strict UTC ISO-8601 string to a naive UTC datetime.

    Accepts Z suffix or explicit +00:00 offset. Rejects non-UTC offsets
    with NotImplementedError. Rejects timezone-naive, malformed, or empty strings
    with ValueError.
    """
    if not isinstance(s, str):
        raise ValueError(f"expected str, got {type(s).__name__!r}")
    if not s:
        raise ValueError("empty ISO string")
    if _fractional_second_digits(s) > 6:
        raise ValueError(f"unparseable ISO string: {s!r}")

    normalized = s[:-1] + "+00:00" if s.endswith("Z") else s
    try:
        dt = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise ValueError(f"unparseable ISO string: {s!r}") from exc

    if dt.tzinfo is None:
        raise ValueError(
            f"timezone-naive ISO string: {s!r}; expected UTC with 'Z' or '+00:00' suffix"
        )
    if dt.utcoffset() != timedelta(0):
        raise NotImplementedError(f"non-UTC ISO string: {s!r} (offset={dt.utcoffset()})")
    dt = dt.replace(tzinfo=None)

    return dt


def _fractional_second_digits(s: str) -> int:
    if "." not in s:
        return 0

    fraction = s.rsplit(".", maxsplit=1)[1]
    for separator in ("Z", "+", "-"):
        fraction = fraction.split(separator, maxsplit=1)[0]
    return len(fraction)


def _iso_strs_to_datetime64(strs: Iterable[str]) -> np.ndarray:
    """Convert an iterable of strict UTC ISO-8601 strings to datetime64.

    Precision is auto-detected from the input: datetime64[s] when no string
    contains a fractional-second component, datetime64[us] otherwise.
    The whole array uses a single unit (upgraded to [us] if any element has
    fractional-second syntax).

    Raises ValueError for malformed or non-UTC strings, and for strings with
    more than 6 fractional digits (nanosecond precision; stdlib limit).
    """
    strs_list = list(strs)
    if not strs_list:
        return np.array([], dtype="datetime64[s]")

    has_frac = any("." in s for s in strs_list)
    unit = "us" if has_frac else "s"
    parsed = [_parse_iso_utc(s) for s in strs_list]
    return np.asarray(parsed, dtype=f"datetime64[{unit}]")
