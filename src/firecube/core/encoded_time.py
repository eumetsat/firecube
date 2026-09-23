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

"""Domain-neutral helpers for calendar-declared time axes.

This module is the single arithmetic entry point for every calendar a time
axis may declare, Gregorian included. A CF calendar only changes how days
group into months and years; every CF calendar has 86400-second days, so a
fixed-cadence axis is exact linear arithmetic in the encoded
(``units``/``calendar``) domain.

`encode_coordinate` fast-paths Gregorian-like calendars (``"standard"``,
``"gregorian"``, ``"proleptic_gregorian"``): a ``str``, ``datetime.datetime``,
``numpy.datetime64``, ``pandas.Timestamp``, or calendar-valued value on a
Gregorian-like axis is converted straight to epoch seconds and offset
against the axis's ``units``, importing neither ``xarray`` nor ``cftime``.
A non-Gregorian calendar, or an axis ``units`` string that is not
``"seconds since "``-prefixed, falls through to xarray's CF time coder,
which brings in ``cftime`` transitively through the hard ``netcdf4``
dependency.

`coerce_to_epoch_s` is the general-purpose epoch-seconds converter for
``str``/``datetime``/``numpy.datetime64``/``pandas.Timestamp`` input. It has
no xarray/cftime import on its call path and performs no I/O.
`is_gregorian_axis` and `is_gregorian_like` let a caller branch on an axis's
calendar before ever calling `encode_coordinate`.

Nothing here is exported from a public ``api.py`` facade. It is internal
plumbing consumed by ``firecube.core.index_spec`` and
``firecube.core.index_resolve``.
"""

from __future__ import annotations

import datetime as dt
import math
import numbers
from collections.abc import Callable
from typing import Any, Literal

import numpy as np

from firecube.core.slot_index import iso_to_epoch_s

__all__ = [
    "coerce_to_epoch_s",
    "derive_regular_axis_units",
    "encode_coordinate",
    "is_calendar_valued",
    "is_gregorian_axis",
    "is_gregorian_like",
    "normalise_calendar",
    "validate_calendar_units",
]

_GREGORIAN_LIKE_CALENDARS = frozenset({"standard", "gregorian", "proleptic_gregorian"})
_CALENDAR_ALIASES = {
    "365_day": "noleap",
    "366_day": "all_leap",
    # Firecube stores Gregorian-like time as ``datetime64``, which is
    # proleptic Gregorian; the three CF spellings name the same encoding.
    "standard": "proleptic_gregorian",
    "gregorian": "proleptic_gregorian",
}
_UTC_EXPLICIT_SUFFIXES = ("Z", "+00:00", "-00:00")
_SECONDS_SINCE_PREFIX = "seconds since "


def _xarray_time_codecs() -> tuple[Callable[..., Any], Callable[..., Any]]:
    """Lazily import xarray's CF time coders.

    Mirrors ``firecube.core.zarr.time_decode._xarray_time_codecs`` so this
    module stays cheap to import for callers that never touch a
    non-Gregorian calendar axis.
    """

    from xarray.coding.times import decode_cf_datetime, encode_cf_datetime

    return decode_cf_datetime, encode_cf_datetime


def normalise_calendar(calendar: str) -> str:
    """Normalise a CF calendar name for comparison and storage.

    Lowercases the name and translates aliases to one canonical spelling:
    ``"365_day"`` -> ``"noleap"``, ``"366_day"`` -> ``"all_leap"``, and
    ``"standard"`` / ``"gregorian"`` -> ``"proleptic_gregorian"`` (firecube
    stores Gregorian-like time as ``datetime64``, which is proleptic
    Gregorian; dates before 1582-10-15 follow that rule, not the CF mixed
    calendar). Every other name is returned lowercased and otherwise
    unchanged.

    Args:
        calendar: A CF calendar name, e.g. ``"360_Day"`` or ``"NoLeap"``.

    Returns:
        The normalised calendar name.

    Raises:
        ValueError: If *calendar* is not a non-empty string.
    """

    if not isinstance(calendar, str) or not calendar.strip():
        raise ValueError(
            f"calendar must be a CF calendar name such as 'proleptic_gregorian' or "
            f"'360_day'; got {calendar!r} (omit calendar for Gregorian time)"
        )
    text = calendar.strip().lower()
    return _CALENDAR_ALIASES.get(text, text)


def is_gregorian_like(calendar: str) -> bool:
    """Return whether *calendar* denotes a Gregorian-shaped calendar.

    Args:
        calendar: A CF calendar name; normalised internally before the
            comparison, so callers may pass a raw or already-normalised name.

    Returns:
        ``True`` for ``"standard"``, ``"gregorian"``, or
        ``"proleptic_gregorian"`` (after normalisation); ``False`` otherwise.
    """

    return normalise_calendar(calendar) in _GREGORIAN_LIKE_CALENDARS


def is_gregorian_axis(axis: Any) -> bool:
    """Return whether *axis* addresses Gregorian (datetime64) time.

    Args:
        axis: An axis-like object; only its optional ``calendar`` attribute
            is inspected.

    Returns:
        ``True`` when *axis* has no ``calendar`` attribute, ``axis.calendar``
        is ``None``, or ``axis.calendar`` normalises to a Gregorian-like
        name; ``False`` otherwise.
    """

    calendar = getattr(axis, "calendar", None)
    return calendar is None or is_gregorian_like(calendar)


def is_calendar_valued(value: Any) -> bool:
    """Return whether *value* duck-types as a calendar-valued object.

    A calendar-valued object has a string ``calendar`` attribute and a
    callable ``isoformat`` method, the shape every ``cftime`` datetime class
    has. This module never imports ``cftime`` to perform the check.

    Args:
        value: Any candidate coordinate value.

    Returns:
        ``True`` if *value* has both a string ``calendar`` attribute and a
        callable ``isoformat`` attribute.
    """

    calendar = getattr(value, "calendar", None)
    isoformat = getattr(value, "isoformat", None)
    return isinstance(calendar, str) and callable(isoformat)


def derive_regular_axis_units(epoch: str) -> str:
    """Derive CF ``units`` for a regular axis from its UTC-explicit epoch.

    Turns ``"2049-01-01T12:00:00Z"`` into
    ``"seconds since 2049-01-01 12:00:00"``. The epoch is never parsed as a
    date (not with ``numpy``, not with ``datetime``): it is only sliced as
    text, so an epoch that is invalid in the Gregorian calendar (e.g.
    ``"1850-02-30T00:00:00Z"``) still derives units cleanly -- validity is
    the calendar's own coder's concern, checked separately by
    `validate_calendar_units`.

    Args:
        epoch: UTC-explicit ISO 8601 epoch string; must end with ``"Z"``,
            ``"+00:00"``, or ``"-00:00"``.

    Returns:
        The derived CF ``units`` string, ``"seconds since <epoch body>"``,
        with the ``"T"`` separator rewritten to a space.

    Raises:
        ValueError: If *epoch* is not a non-empty, UTC-explicit string.
    """

    if not isinstance(epoch, str) or not epoch.strip():
        raise ValueError(f"epoch must be a non-empty UTC ISO string; got {epoch!r}")
    text = epoch.strip()
    for suffix in _UTC_EXPLICIT_SUFFIXES:
        if text.endswith(suffix):
            body = text[: -len(suffix)]
            break
    else:
        raise ValueError(
            f"epoch must be UTC-explicit ('Z', '+00:00', or '-00:00' offset); got {epoch!r}"
        )
    return f"seconds since {body.replace('T', ' ')}"


def validate_calendar_units(*, units: str, calendar: str) -> None:
    """Validate that *units* and *calendar* are jointly usable.

    Asks xarray's CF decoder to decode the value ``0`` against *units* and
    *calendar*. An unknown calendar name or a reference date in *units* that
    does not exist in *calendar* (e.g. ``2049-02-31`` in ``360_day``) both
    surface here as a decode failure, which this function re-raises as a
    single, clear `ValueError`.

    Args:
        units: A CF ``units`` string, e.g.
            ``"seconds since 2049-01-01 12:00:00"``.
        calendar: A CF calendar name.

    Raises:
        ValueError: If *units* and *calendar* cannot be decoded together.
    """

    decode_cf_datetime, _ = _xarray_time_codecs()
    try:
        decode_cf_datetime(np.array([0]), units=units, calendar=calendar)
    except Exception as exc:
        raise ValueError(f"units={units!r} is not valid for calendar={calendar!r}: {exc}") from exc


def _canonicalise_encoded_number(value: Any) -> int | float:
    """Coerce a numeric value to the canonical `encode_coordinate` result.

    Returns a Python ``int`` when the value is integral, else a Python
    ``float``. Raises `TypeError` for ``bool``/``np.bool_`` (a boolean is
    not a coordinate). Raises `ValueError` for a non-finite ``float``
    (``NaN`` or infinity).

    ``np.bool_`` must be rejected *before* the ``np.generic`` unboxing:
    ``np.bool_(True).item()`` returns Python ``True``, and ``int(True) == 1``
    would otherwise silently coerce a boolean into a valid coordinate.
    """

    if isinstance(value, (bool, np.bool_)):
        raise TypeError(f"encoded coordinate must not be bool; got {value!r}")
    if isinstance(value, np.generic):
        value = value.item()
    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            raise ValueError(f"encoded coordinate must be finite; got {value!r}")
        if value.is_integer():
            return int(value)
        return value
    return int(value)


def _apply_mode(result: int | float, *, mode: str) -> int | float:
    """Apply ``mode="floor"`` truncation to a non-Gregorian encode result.

    Args:
        result: The canonicalised `encode_coordinate` result (xarray-encoded
            or already-encoded numeric passthrough).
        mode: ``"exact"`` returns *result* unchanged. ``"floor"`` floors a
            non-integral ``float`` result to an ``int``.

    Returns:
        *result* unchanged (``mode="exact"``, or *result* is already
        integral), or its floor as an ``int`` (``mode="floor"`` and *result*
        is a non-integral ``float``).
    """

    if mode == "floor" and isinstance(result, float):
        return math.floor(result)
    return result


def _seconds_since_epoch_s(units: str) -> int | None:
    """Return the Gregorian axis epoch in seconds, or ``None`` if unfit.

    Only a ``"seconds since "``-prefixed *units* string is fast-pathable;
    any other CF ``units`` string returns ``None`` so the caller falls back
    to the xarray-based path.

    Args:
        units: A CF ``units`` string.

    Returns:
        The epoch in seconds since the Unix epoch, or ``None`` if *units*
        does not start with ``"seconds since "``.
    """

    if not units.startswith(_SECONDS_SINCE_PREFIX):
        return None
    epoch_body = units[len(_SECONDS_SINCE_PREFIX) :]
    epoch_iso = f"{epoch_body.replace(' ', 'T', 1)}Z"
    return iso_to_epoch_s(epoch_iso)


def _as_utc_explicit(text: str) -> str:
    """Append ``"Z"`` to *text* unless it already ends with a UTC suffix."""

    if text.endswith(_UTC_EXPLICIT_SUFFIXES):
        return text
    return f"{text}Z"


def _gregorian_epoch_s(value: Any, *, mode: str) -> int:
    """Compute epoch seconds for a value addressing a Gregorian-like axis.

    Delegates to `coerce_to_epoch_s`, so every accepted type keeps its
    established semantics: a ``str`` and a ``numpy.datetime64`` are truncated
    to whole seconds in both modes, while a ``datetime.datetime`` or
    ``pandas.Timestamp`` with a fractional second raises in ``"exact"`` mode.

    Args:
        value: A ``str``, ``datetime.datetime``, ``numpy.datetime64``, or
            ``pandas.Timestamp`` coordinate value.
        mode: ``"exact"`` or ``"floor"``.

    Returns:
        Whole seconds since the Unix epoch.

    Raises:
        TypeError: If *value* is not one of the accepted types.
        ValueError: In ``"exact"`` mode, if a ``datetime.datetime`` or
            ``pandas.Timestamp`` value has a fractional second.
    """

    return coerce_to_epoch_s(value, mode=mode)


def coerce_to_epoch_s(value: Any, *, mode: str = "floor") -> int:
    """Coerce a coordinate value to seconds since the Unix epoch (UTC).

    Accepts the following types:

    - ``str``: UTC-explicit ISO 8601 string (via ``iso_to_epoch_s``).
    - ``datetime.datetime``: naive treated as UTC (FCI pattern);
      aware converted to UTC.
    - ``numpy.datetime64``: any unit, converted to seconds.
    - ``pandas.Timestamp``: naive treated as UTC; aware converted to UTC.

    Args:
        value: The coordinate value to coerce.
        mode: ``"floor"`` (default) or ``"exact"``. In ``"exact"`` mode,
            fractional seconds raise ``ValueError``.

    Returns:
        Integer seconds since the Unix epoch (UTC).

    Raises:
        TypeError: If ``value`` is not one of the accepted types.
        ValueError: In ``"exact"`` mode, if the value has sub-second precision.
    """
    import pandas as pd  # lazy import: keep at function scope to defer import cost

    if isinstance(value, str):
        return iso_to_epoch_s(value)

    if isinstance(value, pd.Timestamp):
        if value.tz is None:
            value = value.tz_localize("UTC")
        else:
            value = value.tz_convert("UTC")
        ts = value.timestamp()
        if mode == "exact" and ts != int(ts):
            raise ValueError(
                f"coordinate {value!r} has sub-second precision; "
                "use mode='floor' or provide a whole-second value"
            )
        return int(ts)

    if isinstance(value, dt.datetime):
        if value.tzinfo is None:
            # Default: naive datetime treated as UTC (FCI pattern)
            value = value.replace(tzinfo=dt.UTC)
        else:
            value = value.astimezone(dt.UTC)
        ts = value.timestamp()
        if mode == "exact" and ts != int(ts):
            raise ValueError(
                f"coordinate {value!r} has sub-second precision; "
                "use mode='floor' or provide a whole-second value"
            )
        return int(ts)

    if isinstance(value, np.datetime64):
        return int(value.astype("datetime64[s]").astype("int64"))

    if is_calendar_valued(value):
        raise TypeError(
            "coordinate must be str, datetime, numpy.datetime64, or pandas.Timestamp; "
            f"got {type(value).__name__!r}; the value is on calendar {value.calendar!r}; "
            "declare calendar= on the time axis"
        )

    raise TypeError(
        "coordinate must be str, datetime, numpy.datetime64, or pandas.Timestamp; "
        f"got {type(value).__name__!r}"
    )


def encode_coordinate(
    value: Any,
    *,
    units: str,
    calendar: str,
    mode: Literal["exact", "floor"] = "exact",
) -> int | float:
    """Encode a plugin-supplied coordinate value onto a calendar time axis.

    Implements the acceptance table for values addressing a calendar axis:

    * A calendar-valued object (see `is_calendar_valued`) whose own
      calendar normalises to the same value as *calendar* is encoded. When
      *calendar* is Gregorian-like (see `is_gregorian_like`) and *units* is
      a ``"seconds since "`` string, this goes through the no-xarray,
      no-cftime fast path; otherwise it is encoded via xarray's CF coder
      against *units* and *calendar*.
    * A calendar-valued object on a *different* calendar raises
      `ValueError` naming both calendars -- xarray would otherwise encode
      it silently, mislabeling the value.
    * ``bool`` raises `TypeError`.
    * Any other ``int``/``float``/numpy number is taken as already encoded
      in the axis units and returned canonicalised.
    * A ``str``, ``datetime.datetime``, ``numpy.datetime64``, or
      ``pandas.Timestamp`` value on a Gregorian-like axis declared with
      ``"seconds since "`` *units* is encoded through the same no-xarray,
      no-cftime fast path.
    * Any other Gregorian-shaped value (or a Gregorian-shaped value against
      a non-Gregorian axis, or non-``"seconds since "`` *units*) raises
      `TypeError`.

    Args:
        value: The coordinate value supplied by a plugin.
        units: The axis's CF ``units`` string.
        calendar: The axis's (already normalised) CF calendar name.
        mode: ``"exact"`` (default) or ``"floor"``. On the Gregorian fast
            path, ``"exact"`` raises for a sub-second-precision ``str``,
            ``datetime.datetime``, or ``pandas.Timestamp`` value (a
            ``numpy.datetime64`` value always floors); ``"floor"`` truncates
            instead. Off the fast path, ``"floor"`` floors a non-integral
            xarray-encoded or already-encoded numeric result to an ``int``;
            ``"exact"`` leaves it as a non-integral ``float``.

    Returns:
        The canonical encoded value: a Python ``int`` when the encoded
        number is integral, else a Python ``float``.

    Raises:
        TypeError: *value* is ``bool``, or is a Gregorian-shaped value that
            cannot address *calendar*/*units* as given.
        ValueError: *value* is a calendar-valued object on a different
            calendar than *calendar*, the encoded result is not finite, or
            (``mode="exact"`` on the Gregorian fast path) *value* has
            sub-second precision.
    """

    if isinstance(value, bool):
        raise TypeError(f"encode_coordinate does not accept bool; got {value!r}")

    # ``np.bool_`` is not an instance of ``bool``, so it skips the guard
    # above; a boolean is not a coordinate on any calendar, so it gets the
    # same refusal as ``bool`` (naming the numpy type), not the
    # "Gregorian-typed value" message meant for datetime-like inputs.
    if isinstance(value, np.bool_):
        raise TypeError(f"encode_coordinate does not accept bool (np.bool_); got {value!r}")

    axis_calendar_norm = normalise_calendar(calendar)
    axis_is_gregorian = is_gregorian_like(axis_calendar_norm)

    if is_calendar_valued(value):
        value_calendar_norm = normalise_calendar(value.calendar)
        # Exact match is always accepted; a Gregorian-like value against a
        # Gregorian-like axis is also accepted even when the two spellings
        # differ (e.g. a `cftime.DatetimeGregorian`, calendar="standard",
        # addressing an axis declared calendar="proleptic_gregorian") --
        # `_GREGORIAN_LIKE_CALENDARS` already treats the three spellings as
        # the same calendar; only a genuine cross-calendar mismatch (e.g.
        # noleap on a 360_day axis) is refused.
        calendars_compatible = value_calendar_norm == axis_calendar_norm or (
            axis_is_gregorian and is_gregorian_like(value_calendar_norm)
        )
        if not calendars_compatible:
            guidance = (
                f"; declare calendar={value.calendar!r} on the time axis "
                "(RegularTimeAxis/IrregularTimeAxis, or TimeAxis.grid/.explicit/.discovered)"
                if axis_is_gregorian
                else ""
            )
            raise ValueError(
                f"coordinate calendar {value.calendar!r} does not match axis calendar "
                f"{calendar!r}{guidance}"
            )
        if axis_is_gregorian:
            axis_epoch_s = _seconds_since_epoch_s(units)
            if axis_epoch_s is not None:
                iso_text = _as_utc_explicit(value.isoformat())
                value_epoch_s = _gregorian_epoch_s(iso_text, mode=mode)
                return _canonicalise_encoded_number(value_epoch_s - axis_epoch_s)
        _, encode_cf_datetime = _xarray_time_codecs()
        encoded, _, _ = encode_cf_datetime(
            np.array([value], dtype=object), units=units, calendar=calendar
        )
        return _apply_mode(_canonicalise_encoded_number(encoded.reshape(-1)[0]), mode=mode)

    if isinstance(value, (numbers.Integral, numbers.Real)):
        return _apply_mode(_canonicalise_encoded_number(value), mode=mode)

    if axis_is_gregorian:
        axis_epoch_s = _seconds_since_epoch_s(units)
        if axis_epoch_s is not None:
            value_epoch_s = _gregorian_epoch_s(value, mode=mode)
            return _canonicalise_encoded_number(value_epoch_s - axis_epoch_s)

    raise TypeError(
        f"a Gregorian-typed value ({type(value).__name__}) cannot address a calendar axis "
        f"(calendar={calendar!r}, units={units!r}); pass a calendar-valued object on "
        "that calendar, a recognized Gregorian-shaped value against a Gregorian axis "
        "declared with 'seconds since ' units, or an already-encoded int/float"
    )
