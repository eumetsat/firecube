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

"""Contracts for the ``TimeAxis`` facade.

Two invariants matter more than any feature here:

* **The facade is sugar, not a dialect.** Each ``TimeAxis`` constructor
  returns the same plain dataclass a raw declaration would, so a cube
  declared through the facade is indistinguishable from one declared raw.
"""

from __future__ import annotations

import datetime as dt

import pytest

from firecube.core.api import (
    AUTO,
    AxisSpec,
    IndexSpec,
    IrregularTimeAxis,
    RegularTimeAxis,
    TimeAxis,
    resolve_index_spec,
)

_EPOCH = "2024-01-01T00:00:00Z"
_CADENCE_S = 600
_SLOTS = 144


def _identity(axis: AxisSpec) -> tuple[str, dict]:
    spec = IndexSpec(name="decl_v1", groups={"data": axis})
    resolved = resolve_index_spec(spec, time_dim_name="time")
    return resolved.identity_hash, resolved.canonical_index_payload()


def _regular(**overrides) -> RegularTimeAxis:
    kwargs = {
        "coordinate": "time",
        "epoch": _EPOCH,
        "cadence_s": _CADENCE_S,
        "slot_count": _SLOTS,
    }
    kwargs.update(overrides)
    return RegularTimeAxis(**kwargs)


class TestTimeAxisFacade:
    def test_grid_matches_raw_exact_declaration(self) -> None:
        via_facade = TimeAxis.grid(
            coordinate="time", epoch=_EPOCH, cadence_s=_CADENCE_S, slot_count=_SLOTS
        )
        raw = _regular(mode="exact")
        assert via_facade == raw
        assert _identity(via_facade) == _identity(raw)

    def test_observed_matches_raw_floor_declaration(self) -> None:
        via_facade = TimeAxis.observed(
            coordinate="time", epoch=_EPOCH, cadence_s=_CADENCE_S, slot_count=_SLOTS
        )
        raw = _regular(mode="floor")
        assert via_facade == raw
        assert _identity(via_facade) == _identity(raw)

    def test_grid_with_floor_placement_is_rejected(self) -> None:
        with pytest.raises(ValueError, match="cannot pass drift verification"):
            TimeAxis.grid(
                coordinate="time",
                epoch=_EPOCH,
                cadence_s=_CADENCE_S,
                slot_count=_SLOTS,
                placement="floor",
            )

    def test_explicit_matches_raw_irregular_declaration(self) -> None:
        values = ("2024-01-01T00:00:02Z", "2024-01-01T00:10:07Z")
        assert TimeAxis.explicit(coordinate="time", values=values) == IrregularTimeAxis(
            coordinate="time", values=values
        )

    def test_discovered_matches_raw_auto_declaration(self) -> None:
        axis = TimeAxis.discovered(coordinate="time")
        assert isinstance(axis, IrregularTimeAxis)
        assert axis.values is AUTO

    def test_explicit_rejects_string_values(self) -> None:
        with pytest.raises(TypeError, match="not a string or bytes"):
            TimeAxis.explicit(coordinate="time", values="abc")

    def test_explicit_rejects_bytes_values(self) -> None:
        with pytest.raises(TypeError, match="not a string or bytes"):
            TimeAxis.explicit(coordinate="time", values=b"abc")

    def test_explicit_rejects_duplicate_values(self) -> None:
        with pytest.raises(ValueError, match="must not contain duplicates"):
            TimeAxis.explicit(
                coordinate="time",
                values=("2024-01-01T00:00:00Z", "2024-01-01T00:00:00Z"),
            )

    def test_discovered_equals_and_hashes_raw_auto(self) -> None:
        via_facade = TimeAxis.discovered(coordinate="time")
        raw = IrregularTimeAxis(coordinate="time", values=AUTO)
        assert via_facade == raw
        assert hash(via_facade) == hash(raw)

    def test_all_constructors_python_hash_identical_to_raw(self) -> None:
        # ``_identity`` asserts the content-addressed resolved-index hash;
        # Python ``hash()`` is a separate contract (frozen-dataclass invariant).
        assert hash(
            TimeAxis.grid(coordinate="time", epoch=_EPOCH, cadence_s=_CADENCE_S, slot_count=_SLOTS)
        ) == hash(_regular(mode="exact"))
        assert hash(
            TimeAxis.observed(
                coordinate="time", epoch=_EPOCH, cadence_s=_CADENCE_S, slot_count=_SLOTS
            )
        ) == hash(_regular(mode="floor"))
        values = ("2024-01-01T00:00:02Z", "2024-01-01T00:10:07Z")
        assert hash(TimeAxis.explicit(coordinate="time", values=values)) == hash(
            IrregularTimeAxis(coordinate="time", values=values)
        )

    def test_explicit_canonicalizes_tzaware_datetimes_at_resolve_time(self) -> None:
        # The facade stores caller-supplied values verbatim; the resolver
        # normalizes them into UTC-form ISO only when it emits the canonical
        # payload. Verifies the deferred-canonicalization invariant end-to-end.
        ts_utc = dt.datetime(2024, 1, 1, 0, 0, 2, tzinfo=dt.UTC)
        ts_offset = dt.datetime(2024, 1, 1, 5, 10, 7, tzinfo=dt.timezone(dt.timedelta(hours=5)))
        axis = TimeAxis.explicit(coordinate="time", values=[ts_utc, ts_offset])
        assert axis.values == (ts_utc, ts_offset)

        spec = IndexSpec(name="tz_v1", groups={"data": axis})
        resolved = resolve_index_spec(spec, time_dim_name="time")
        stored = resolved.canonical_index_payload()["groups"]["data"]["params"]["values"]
        assert all(isinstance(s, str) and s.endswith("Z") for s in stored), stored
        # 05:10:07+05:00 -> 00:10:07Z; UTC input survives verbatim.
        assert set(stored) == {"2024-01-01T00:00:02Z", "2024-01-01T00:10:07Z"}

    def test_serial_mode_declaration_needs_no_extent(self) -> None:
        axis = TimeAxis.observed(coordinate="time", epoch=_EPOCH, cadence_s=_CADENCE_S)
        assert axis.slot_count is None and axis.end_date is None
