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

"""Float-precision fuzz test for irregular calendar axes with fractional units.

Encoding a ``cftime`` coordinate against a *fractional* CF ``units`` string
(e.g. ``"days since 1850-01-01"`` combined with a sub-day resolution
timestamp) yields a ``float64`` value from xarray's CF coder. ``float64``
carries only ~15-17 significant decimal digits, so at large day-offsets the
encoded value's unit-in-last-place (ULP) grows past the resolution the
caller assumed. The failure mode the store must never exhibit is *silent*
corruption: two distinct calendar-valued coordinates encoding to the same
float and landing at different slots.

This fuzz test locks in three properties of the
``_canonicalise_encoded_number`` + ``IrregularTimeResolver.position()`` path
across ``Datetime360Day`` and ``DatetimeNoLeap``:

1. In the operational range 1850-2100, spacings above ~1 ms are far above
   the float ULP (~2 microseconds at the far end); pairs > 1 ms apart
   round-trip through the resolver as distinct slots.
2. When two coordinates are so close that they round to the same encoded
   float (the "sub-microsecond" regime), the encoding is symmetric:
   ``encode(a) == encode(b)`` implies ``encode(b) == encode(a)`` and both
   canonicalise to the same value. Encoding is a pure function of the
   coordinate, never of call order.
3. At the extreme model year 280,000 with sub-millisecond spacing (well
   below the ~1.9 ms ULP at that offset), collisions become common; the
   store's response is always *loud*: either the collision is dedup'd at
   ``IrregularTimeAxis`` construction (``ValueError`` naming duplicates),
   or the two coordinates land at distinct slots after ``position()``.
   The forbidden outcome -- two coordinates that encoded to the same float
   silently landing at different slot indices -- is asserted not to occur.

Runs deterministically with a fixed seed (``_FUZZ_SEED``); no external
fuzz-testing dependency (``hypothesis``) is required. Total runtime is
under 2 seconds so this test lives on the fast lane.
"""

from __future__ import annotations

import datetime as dt
import random
from typing import Any

import cftime
import pytest

from firecube.core.encoded_time import _canonicalise_encoded_number, encode_coordinate
from firecube.core.index_resolve import IrregularTimeResolver
from firecube.core.index_spec import IrregularTimeAxis

pytestmark = pytest.mark.unit


_FUZZ_SEED = 20260921
_FUZZ_ITERATIONS = 50
_UNITS = "days since 1850-01-01 00:00:00"

_CALENDARS: tuple[tuple[str, type[Any]], ...] = (
    ("360_day", cftime.Datetime360Day),
    ("noleap", cftime.DatetimeNoLeap),
)


def _random_cftime_1850_2100(rng: random.Random, cls: type[Any]) -> Any:
    # day <= 28 keeps the generator safe for every CF calendar (360_day has
    # no day 29-30 outside its "February"; noleap has no Feb 29).
    return cls(
        rng.randint(1850, 2098),
        rng.randint(1, 12),
        rng.randint(1, 28),
        rng.randint(0, 22),
        rng.randint(0, 59),
        rng.randint(0, 59),
        rng.randint(0, 999_999),
    )


def _random_cftime_year_280000(rng: random.Random, cls: type[Any]) -> Any:
    return cls(
        rng.randint(279_990, 280_010),
        rng.randint(1, 12),
        rng.randint(1, 28),
        rng.randint(0, 22),
        rng.randint(0, 59),
        rng.randint(0, 58),
        rng.randint(0, 999_499),
    )


def _shift(value: Any, microseconds: int) -> Any:
    return value + dt.timedelta(microseconds=microseconds)


@pytest.mark.parametrize(("calendar", "cls"), _CALENDARS)
def test_pairs_over_1ms_apart_round_trip_distinctly(calendar: str, cls: type[Any]) -> None:
    """> 1 ms spacing in [1850, 2100) never produces a float collision.

    Rationale: at 2099-12-31 the encoded value is ~91,250 days; the ULP is
    ~2 microseconds. A 2 ms lower bound on spacing sits three orders of
    magnitude above that ULP, so no false-duplicate is possible. Any
    collision here signals a regression in the encoder or in
    ``_canonicalise_encoded_number``'s float handling.
    """

    rng = random.Random(_FUZZ_SEED)
    for _ in range(_FUZZ_ITERATIONS):
        a = _random_cftime_1850_2100(rng, cls)
        # 2 ms to 1 hour, chosen to span both millisecond and second scales.
        spacing_us = rng.randint(2_000, 3_600_000_000)
        b = _shift(a, spacing_us)

        encoded_a = encode_coordinate(a, units=_UNITS, calendar=calendar)
        encoded_b = encode_coordinate(b, units=_UNITS, calendar=calendar)

        canon_a = _canonicalise_encoded_number(encoded_a)
        canon_b = _canonicalise_encoded_number(encoded_b)

        assert canon_a != canon_b, (
            f"unexpected encoded collision at {a!r} / {b!r} "
            f"(spacing={spacing_us} us, canon={canon_a!r})"
        )

        # Round-trip through the full resolver: IrregularTimeAxis encodes
        # values at construction, and IrregularTimeResolver.position()
        # re-encodes the query and looks it up by equality. Both sides use
        # _canonicalise_encoded_number, so this exercises the whole path.
        axis = IrregularTimeAxis(
            coordinate="time",
            values=[a, b],
            calendar=calendar,
            units=_UNITS,
        )
        resolver = IrregularTimeResolver(axis=axis)
        assert resolver.position(a) == 0
        assert resolver.position(b) == 1


@pytest.mark.parametrize(("calendar", "cls"), _CALENDARS)
def test_sub_microsecond_pairs_encode_symmetrically(calendar: str, cls: type[Any]) -> None:
    """Sub-microsecond distinct coordinates encode symmetrically.

    ``cftime``'s wall-clock resolution is one microsecond, so "less than
    one microsecond apart" in the operational range means the same
    coordinate value. The invariant this exercises is that encoding is a
    pure function: ``encode(a)`` and ``encode(b)`` are byte-identical when
    ``a == b``, regardless of independent object identity. Any drift here
    -- e.g. call-order-dependent state -- would corrupt dedup logic in the
    axis and the append cursor.
    """

    rng = random.Random(_FUZZ_SEED + 1)
    for _ in range(_FUZZ_ITERATIONS):
        a = _random_cftime_1850_2100(rng, cls)
        b = cls(a.year, a.month, a.day, a.hour, a.minute, a.second, a.microsecond)

        encoded_a = encode_coordinate(a, units=_UNITS, calendar=calendar)
        encoded_b = encode_coordinate(b, units=_UNITS, calendar=calendar)

        assert encoded_a == encoded_b, (
            f"encoding is not a pure function of the coordinate: "
            f"{a!r} -> {encoded_a!r}, {b!r} -> {encoded_b!r}"
        )

        canon_a = _canonicalise_encoded_number(encoded_a)
        canon_b = _canonicalise_encoded_number(encoded_b)
        assert canon_a == canon_b
        # Canonical form is deterministic per coordinate; the reverse
        # direction must produce the same canonical value.
        assert _canonicalise_encoded_number(encoded_b) == canon_a


@pytest.mark.parametrize(("calendar", "cls"), _CALENDARS)
def test_year_280000_submillisecond_spacing_never_silently_corrupts(
    calendar: str, cls: type[Any]
) -> None:
    """Regression baseline: no silent corruption at year 280,000.

    At ~278,150 model years past the 1850 epoch the encoded ``float64`` is
    ~1e8 days and the ULP is ~1.9 ms, so a randomly-chosen sub-millisecond
    spacing collides on a substantial fraction of examples. The store must
    respond loudly in either direction:

    * If the two coordinates encode to the same float,
      ``IrregularTimeAxis`` construction refuses them as duplicates.
    * If they encode to distinct floats, both are accepted and
      ``resolver.position()`` returns distinct slot indices.

    The forbidden outcome -- construction succeeds with two coordinates
    that encoded to the same float but ``position()`` returns different
    slots -- would silently split incoming granules across two rows and
    is checked against explicitly.
    """

    rng = random.Random(_FUZZ_SEED + 2)
    collisions_seen = 0
    for _ in range(_FUZZ_ITERATIONS):
        a = _random_cftime_year_280000(rng, cls)
        # 1 to 500 microseconds -- deliberately below the ~1.9 ms ULP
        # at year 280,000 so collisions are common.
        spacing_us = rng.randint(1, 500)
        b = _shift(a, spacing_us)

        encoded_a = encode_coordinate(a, units=_UNITS, calendar=calendar)
        encoded_b = encode_coordinate(b, units=_UNITS, calendar=calendar)

        if encoded_a == encoded_b:
            # Collision path: the axis must refuse loudly.
            collisions_seen += 1
            with pytest.raises(ValueError, match="duplicates"):
                IrregularTimeAxis(
                    coordinate="time",
                    values=[a, b],
                    calendar=calendar,
                    units=_UNITS,
                )
        else:
            # Distinct-encoding path: construction accepts, positions are
            # distinct. This is the "no silent corruption" leg -- if the
            # resolver ever returned the same slot for two distinct floats,
            # that would be a bug regardless of how close the values are.
            axis = IrregularTimeAxis(
                coordinate="time",
                values=[a, b],
                calendar=calendar,
                units=_UNITS,
            )
            resolver = IrregularTimeResolver(axis=axis)
            pos_a = resolver.position(a)
            pos_b = resolver.position(b)
            assert pos_a == 0
            assert pos_b == 1
            assert pos_a != pos_b

    # Sanity: the year-280,000 regime must actually exercise collisions on
    # this seed and iteration budget; otherwise the "no silent corruption"
    # guarantee is not being tested. Guarding this avoids the test silently
    # degrading into a no-op if a future change accidentally moves the
    # collision boundary outside the fuzz window.
    assert collisions_seen > 0, (
        f"expected at least one float collision at year 280,000 with "
        f"sub-millisecond spacing over {_FUZZ_ITERATIONS} examples "
        f"(calendar={calendar}); the fuzz window may need widening"
    )
