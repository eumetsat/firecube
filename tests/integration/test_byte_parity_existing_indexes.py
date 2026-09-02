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

"""Byte-parity gate for existing recorded index shapes.

This gate pins the canonical bytes and identity hashes of the FCI-shaped and
OPERA-shaped index fixtures. Any change to the serialization logic, key
ordering, or regular-axis emission policy is caught here as a byte diff.

Two invariants the gate enforces:

* **Legacy ``SlotIndexModel`` bytes are unchanged.** OPERA- and FCI-shaped
  models produce the exact byte string and identity hash they always have.
* **Resolved-index payload bytes are unchanged.** Exact/grid, floor/observed,
  integer-only, and mixed axes keep their canonical bytes stable.
"""

from __future__ import annotations

import hashlib

import pytest

from firecube.core.controlplane.types import canonical_index_bytes
from firecube.core.index_resolve import ResolvedIndex, resolve_index_spec
from firecube.core.index_spec import IndexSpec, IntegerAxis, RegularTimeAxis
from firecube.core.slot_index import SlotAxis, SlotIndexModel

pytestmark = [pytest.mark.integration, pytest.mark.snapshot, pytest.mark.contract]


# Golden byte strings and hex hashes below are the exact canonical output
# produced by the firecube serialization stack for the associated fixture.
# Regenerating any of them requires evidence that the previous bytes were
# wrong; a routine refactor of serialization code is not sufficient
# justification.

OPERA_LEGACY_BYTES = (
    b'{"epoch":"2026-01-01T00:00:00Z","groups":{"g1":{"cadence_s":300,'
    b'"mode":"exact"}},"name":"opera_v1","schema_version":"v1","time_unit":null}'
)
OPERA_LEGACY_HASH = "bc441fbb3eb848c54db2c4673e75271d3b98dbb2b7b49081b521b7ef3923a894"

FCI_LEGACY_BYTES = (
    b'{"epoch":"2024-08-01T00:00:00Z","groups":{"g1":{"cadence_s":600,'
    b'"mode":"floor"}},"name":"fci_l1c_v1","schema_version":"v1","time_unit":null}'
)
FCI_LEGACY_HASH = "f189c758fbbf83ec112389610dacad91e1bc35fc35689b107924509a815470b6"

OPERA_RESOLVED_BYTES = (
    b'{"groups":{"data":{"kind":"regular_time","params":{"cadence_s":300,'
    b'"epoch":"2026-01-01T00:00:00Z","mode":"exact"},"size":288}},'
    b'"name":"opera_v1_resolved","schema_version":"v1"}'
)
OPERA_RESOLVED_HASH = "8fb909e5e1f6661829398471fd8d425c4a0acf333c9373544a19ca70ad6d5bed"

FCI_RESOLVED_BYTES = (
    b'{"groups":{"data":{"kind":"regular_time","params":{"cadence_s":600,'
    b'"epoch":"2024-08-01T00:00:00Z","mode":"floor"},"size":144}},'
    b'"name":"fci_l1c_v1_resolved","schema_version":"v1"}'
)
FCI_RESOLVED_HASH = "e5a5126fa51b288dd4f0657100618723c499b93aad417a5b956acd915698b2a4"

INTEGER_RESOLVED_BYTES = (
    b'{"groups":{"data":{"kind":"integer","params":{},"size":12}},'
    b'"name":"integer_only_v1","schema_version":"v1"}'
)
INTEGER_RESOLVED_HASH = "ac6579db66b817cf4c5e35fe54745d57dcf27a70070d4eca8c7da4d078360a1c"


def _opera_legacy_model() -> SlotIndexModel:
    return SlotIndexModel(
        name="opera_v1",
        epoch="2026-01-01T00:00:00Z",
        groups={"g1": SlotAxis(cadence_s=300, mode="exact")},
    )


def _fci_legacy_model() -> SlotIndexModel:
    return SlotIndexModel(
        name="fci_l1c_v1",
        epoch="2024-08-01T00:00:00Z",
        groups={"g1": SlotAxis(cadence_s=600, mode="floor")},
    )


def _opera_resolved_index() -> ResolvedIndex:
    axis = RegularTimeAxis(
        coordinate="time",
        epoch="2026-01-01T00:00:00Z",
        cadence_s=300,
        mode="exact",
        slot_count=288,
    )
    return resolve_index_spec(
        IndexSpec(name="opera_v1_resolved", groups={"data": axis}),
        time_dim_name="time",
    )


def _fci_resolved_index() -> ResolvedIndex:
    axis = RegularTimeAxis(
        coordinate="time",
        epoch="2024-08-01T00:00:00Z",
        cadence_s=600,
        mode="floor",
        slot_count=144,
    )
    return resolve_index_spec(
        IndexSpec(name="fci_l1c_v1_resolved", groups={"data": axis}),
        time_dim_name="time",
    )


def _integer_resolved_index() -> ResolvedIndex:
    return resolve_index_spec(
        IndexSpec(name="integer_only_v1", groups={"data": IntegerAxis(slot_count=12)}),
        time_dim_name="time",
    )


class TestLegacySlotIndexBytesUnchanged:
    def test_opera_legacy_bytes_match_golden(self) -> None:
        assert _opera_legacy_model().canonical_bytes() == OPERA_LEGACY_BYTES

    def test_opera_legacy_hash_matches_golden(self) -> None:
        assert _opera_legacy_model().identity_hash == OPERA_LEGACY_HASH

    def test_fci_legacy_bytes_match_golden(self) -> None:
        assert _fci_legacy_model().canonical_bytes() == FCI_LEGACY_BYTES

    def test_fci_legacy_hash_matches_golden(self) -> None:
        assert _fci_legacy_model().identity_hash == FCI_LEGACY_HASH

    def test_legacy_hashes_are_sha256_of_canonical_bytes(self) -> None:
        for model, expected_hash in (
            (_opera_legacy_model(), OPERA_LEGACY_HASH),
            (_fci_legacy_model(), FCI_LEGACY_HASH),
        ):
            assert hashlib.sha256(model.canonical_bytes()).hexdigest() == expected_hash


class TestResolvedIndexBytesUnchanged:
    def test_opera_resolved_bytes_match_golden(self) -> None:
        resolved = _opera_resolved_index()
        assert canonical_index_bytes(resolved.canonical_index_payload()) == OPERA_RESOLVED_BYTES

    def test_opera_resolved_hash_matches_golden(self) -> None:
        assert _opera_resolved_index().identity_hash == OPERA_RESOLVED_HASH

    def test_fci_resolved_bytes_match_golden(self) -> None:
        resolved = _fci_resolved_index()
        assert canonical_index_bytes(resolved.canonical_index_payload()) == FCI_RESOLVED_BYTES

    def test_fci_resolved_hash_matches_golden(self) -> None:
        assert _fci_resolved_index().identity_hash == FCI_RESOLVED_HASH

    def test_integer_resolved_bytes_match_golden(self) -> None:
        resolved = _integer_resolved_index()
        assert canonical_index_bytes(resolved.canonical_index_payload()) == INTEGER_RESOLVED_BYTES

    def test_integer_resolved_hash_matches_golden(self) -> None:
        assert _integer_resolved_index().identity_hash == INTEGER_RESOLVED_HASH

    def test_resolved_hashes_are_sha256_of_canonical_bytes(self) -> None:
        for resolved, expected_hash in (
            (_opera_resolved_index(), OPERA_RESOLVED_HASH),
            (_fci_resolved_index(), FCI_RESOLVED_HASH),
            (_integer_resolved_index(), INTEGER_RESOLVED_HASH),
        ):
            payload = resolved.canonical_index_payload()
            assert hashlib.sha256(canonical_index_bytes(payload)).hexdigest() == expected_hash
