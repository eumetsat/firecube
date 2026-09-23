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

"""Shared guard against silently mislabeling a non-Gregorian calendar value.

The direct-Zarr write path (`firecube.core.zarr.coord_materialization.coord_to_datetime64`,
`firecube.core.zarr.region_writer.RegionZarrWriter._normalize_timestamp_value`, and its
``_ns`` sibling) historically accepted any value with an ``isoformat()`` method and stamped
it into a Gregorian ``datetime64`` slot -- including a `cftime`-shaped object whose real
calendar is, for example, ``"360_day"``. The conversion went through the value's ISO text,
so a date that exists in both calendars (a ``360_day`` ``1850-03-01``, say) was stamped as the
same year-month-day in Gregorian time: a different instant, with no error and nothing on disk
to say the calendar was ever different. Only dates with no Gregorian counterpart (``02-29`` in
a non-leap year, ``02-30``) raised. Without this guard that mislabeling is silent data
corruption a reader cannot detect (see `plans/DESIGN.md` "fail loudly").

This module gives the three write-time call sites one shared check.
"""

from __future__ import annotations

from typing import Any

from firecube.core.encoded_time import is_calendar_valued, is_gregorian_like

__all__ = ["reject_non_gregorian_calendar_value"]


def reject_non_gregorian_calendar_value(value: Any) -> None:
    """Raise ``ValueError`` when *value* is calendar-valued on a non-Gregorian calendar.

    A Gregorian-like calendar-valued object (calendar normalising to
    ``"standard"``, ``"gregorian"``, or ``"proleptic_gregorian"``) is
    unaffected -- it keeps converting to ``datetime64`` exactly as before.
    Any other calendar-valued object (for example a `cftime.Datetime360Day`)
    is refused: silently accepting it would store its day/month/year fields
    as if they were Gregorian, which is simply wrong for calendars where
    those fields have no Gregorian equivalent (a ``360_day`` ``02-30`` does
    not exist in the Gregorian calendar).

    Values that are not calendar-valued at all (plain strings, ``datetime``,
    ``numpy.datetime64``, ...) are out of scope for this guard and are never
    rejected here.

    Args:
        value: Any candidate coordinate value.

    Raises:
        ValueError: If *value* is calendar-valued (see
            `firecube.core.encoded_time.is_calendar_valued`) and its
            calendar does not normalise to a Gregorian-like name.
    """
    if not is_calendar_valued(value):
        return
    if is_gregorian_like(value.calendar):
        return
    raise ValueError(
        f"value {value!r} has calendar={value.calendar!r}, which is not Gregorian-like; "
        "writing it into a Gregorian datetime64 coordinate would silently mislabel it. "
        f"Declare calendar={value.calendar!r} on the plugin's time axis "
        "(RegularTimeAxis/IrregularTimeAxis, or TimeAxis.grid/.explicit/.discovered) "
        "instead."
    )
