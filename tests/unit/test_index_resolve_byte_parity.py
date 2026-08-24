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

"""Identity contract tests for ``ResolvedIndex`` canonical hashing."""

from __future__ import annotations

import hashlib

from firecube.core.controlplane.types import canonical_index_bytes
from firecube.core.index_resolve import resolve_index_spec
from firecube.core.index_spec import IndexSpec, IntegerAxis, RegularTimeAxis


def _integer_spec() -> IndexSpec:
    return IndexSpec(name="integer_only", groups={"data": IntegerAxis(slot_count=144)})


def _mixed_spec() -> IndexSpec:
    return IndexSpec(
        name="mixed_kind",
        groups={
            "integer": IntegerAxis(slot_count=12),
            "regular": RegularTimeAxis(
                coordinate="timestamp",
                epoch="2024-01-01T00:00:00Z",
                cadence_s=600,
                mode="exact",
                slot_count=24,
            ),
        },
    )


def _regular_spec() -> IndexSpec:
    return IndexSpec(
        name="regular_only",
        groups={
            "data": RegularTimeAxis(
                coordinate="timestamp",
                epoch="2024-01-01T00:00:00Z",
                cadence_s=300,
                mode="exact",
                slot_count=4,
            )
        },
    )


class TestResolvedIndexRecordByteParity:
    def test_integer_only_hash_deterministic(self) -> None:
        resolved = resolve_index_spec(_integer_spec(), time_dim_name="timestamp")

        assert (
            resolved.identity_hash
            == resolve_index_spec(_integer_spec(), time_dim_name="timestamp").identity_hash
        )

    def test_mixed_kind_hash_deterministic(self) -> None:
        resolved = resolve_index_spec(_mixed_spec(), time_dim_name="timestamp")

        assert (
            resolved.identity_hash
            == resolve_index_spec(_mixed_spec(), time_dim_name="timestamp").identity_hash
        )

    def test_regular_only_hash_diverges_from_legacy(self) -> None:
        resolved = resolve_index_spec(_regular_spec(), time_dim_name="timestamp")

        legacy = resolved.as_legacy_slot_index_model()

        assert legacy is not None
        assert resolved.identity_hash != legacy.identity_hash

    def test_identity_hash_equals_sha256_of_canonical_payload(self) -> None:
        resolved = resolve_index_spec(_mixed_spec(), time_dim_name="timestamp")

        assert (
            resolved.identity_hash
            == hashlib.sha256(canonical_index_bytes(resolved.canonical_index_payload())).hexdigest()
        )

    def test_as_resolved_index_record_hash_matches_resolved_hash(self) -> None:
        resolved = resolve_index_spec(_mixed_spec(), time_dim_name="timestamp")

        assert resolved.as_resolved_index_record(run_id="r").identity_hash == resolved.identity_hash
