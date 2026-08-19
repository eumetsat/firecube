"""Golden byte-parity tests for RegularTimeResolver against SlotIndexModel.

These tests verify that ``resolve_index_spec`` produces a ``ResolvedIndex``
whose ``as_legacy_slot_index_model()`` is byte-identical to the legacy
``SlotIndexModel`` for the FCI and OPERA production declarations.

Both ``canonical_bytes()`` AND ``identity_hash`` are asserted (per Oracle:
"compare bytes-to-bytes AND hash-to-hash").
"""

from __future__ import annotations

import pytest

from firecube.core.api import SlotAxis, SlotIndexModel, normalize_epoch_iso
from firecube.core.index_resolve import resolve_index_spec

# Concrete-module imports for the new symbols; core.api re-exports land in task 1.10.
# SlotIndexModel/SlotAxis/normalize_epoch_iso ALREADY on core.api (from prior work); OK to import from core.api.
from firecube.core.index_spec import IndexSpec, RegularTimeAxis

# ---------------------------------------------------------------------------
# FCI L1C declaration (eumetsat_repeat_cycle_v1)
# 7 days x 144 slots/day = 1008 slots; cadence 600s; mode floor
# ---------------------------------------------------------------------------
_FCI_GROUPS = ("data_1km", "data_2km")
_FCI_SPEC = IndexSpec(
    name="eumetsat_repeat_cycle_v1",
    groups={
        g: RegularTimeAxis(
            coordinate="time",
            epoch="2024-09-24T00:00:00Z",
            cadence_s=600,
            mode="floor",
            size=1008,
        )
        for g in _FCI_GROUPS
    },
)
_FCI_LEGACY = SlotIndexModel(
    name="eumetsat_repeat_cycle_v1",
    epoch=normalize_epoch_iso("2024-09-24T00:00:00Z"),
    groups={g: SlotAxis(cadence_s=600, mode="floor") for g in _FCI_GROUPS},
    time_unit=None,
)

# ---------------------------------------------------------------------------
# OPERA/SEVIRI/NORDLIS declaration (opera_seviri_nordlis_v2)
# Two cadences: 300s (exact) for data_300 and data_300b;
#               900s (exact) for data_900, data_900b, data_900c
# ---------------------------------------------------------------------------
_OPERA_GROUPS_300 = ("data_300", "data_300b")
_OPERA_GROUPS_900 = ("data_900", "data_900b", "data_900c")
_OPERA_SPEC = IndexSpec(
    name="opera_seviri_nordlis_v2",
    groups={
        **{
            g: RegularTimeAxis(
                coordinate="time",
                epoch="2024-01-01T00:00:00Z",
                cadence_s=300,
                mode="exact",
                size=288,  # 1 day x 288 slots at 300s
            )
            for g in _OPERA_GROUPS_300
        },
        **{
            g: RegularTimeAxis(
                coordinate="time",
                epoch="2024-01-01T00:00:00Z",
                cadence_s=900,
                mode="exact",
                size=96,  # 1 day x 96 slots at 900s
            )
            for g in _OPERA_GROUPS_900
        },
    },
)
_OPERA_LEGACY = SlotIndexModel(
    name="opera_seviri_nordlis_v2",
    epoch=normalize_epoch_iso("2024-01-01T00:00:00Z"),
    groups={
        **{g: SlotAxis(cadence_s=300, mode="exact") for g in _OPERA_GROUPS_300},
        **{g: SlotAxis(cadence_s=900, mode="exact") for g in _OPERA_GROUPS_900},
    },
    time_unit=None,
)


@pytest.mark.parametrize(
    "spec,legacy,label",
    [
        (_FCI_SPEC, _FCI_LEGACY, "fci"),
        (_OPERA_SPEC, _OPERA_LEGACY, "opera"),
    ],
    ids=["fci", "opera"],
)
def test_byte_parity(spec: IndexSpec, legacy: SlotIndexModel, label: str) -> None:
    """Resolved index produces byte-identical SlotIndexModel for production declarations.

    Both canonical_bytes() AND identity_hash are asserted (per Oracle directive:
    compare bytes-to-bytes AND hash-to-hash).
    """
    resolved = resolve_index_spec(spec, time_dim_name="time")
    resolved_legacy = resolved.as_legacy_slot_index_model()
    assert resolved_legacy is not None, f"{label}: as_legacy_slot_index_model() returned None"
    assert resolved_legacy.canonical_bytes() == legacy.canonical_bytes(), (
        f"{label}: BYTE PARITY BROKEN"
    )
    assert resolved_legacy.identity_hash == legacy.identity_hash, f"{label}: HASH PARITY BROKEN"


def test_regular_time_axis_accepts_minus_zero_zero_epoch() -> None:
    """RegularTimeAxis accepts '-00:00' as a valid UTC epoch string."""

    spec_minus = IndexSpec(
        name="test_minus_zero_zero",
        groups={
            "data": RegularTimeAxis(
                coordinate="time",
                epoch="2024-01-01T00:00:00-00:00",
                cadence_s=600,
                mode="exact",
                size=10,
            )
        },
    )
    spec_z = IndexSpec(
        name="test_minus_zero_zero",
        groups={
            "data": RegularTimeAxis(
                coordinate="time",
                epoch="2024-01-01T00:00:00Z",
                cadence_s=600,
                mode="exact",
                size=10,
            )
        },
    )

    resolved_minus = resolve_index_spec(spec_minus, time_dim_name="time")
    resolved_z = resolve_index_spec(spec_z, time_dim_name="time")

    legacy_minus = resolved_minus.as_legacy_slot_index_model()
    legacy_z = resolved_z.as_legacy_slot_index_model()
    assert legacy_minus is not None
    assert legacy_z is not None
    assert legacy_minus.canonical_bytes() == legacy_z.canonical_bytes(), (
        "'-00:00' and 'Z' epochs must produce byte-identical SlotIndexModels"
    )
    assert legacy_minus.identity_hash == legacy_z.identity_hash
