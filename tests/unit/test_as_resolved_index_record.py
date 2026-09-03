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

"""Unit tests for ResolvedIndex canonical payloads and records."""

from __future__ import annotations

import hashlib

from firecube.core.controlplane.types import ResolvedIndexRecord, canonical_index_bytes
from firecube.core.index_resolve import resolve_index_spec
from firecube.core.index_spec import IndexSpec, IntegerAxis, RegularTimeAxis


def _regular_spec(*, name: str = "regular_only") -> IndexSpec:
    return IndexSpec(
        name=name,
        groups={
            "data_a": RegularTimeAxis(
                coordinate="time",
                epoch="2024-01-01T00:00:00Z",
                cadence_s=300,
                mode="exact",
                slot_count=4,
            ),
            "data_b": RegularTimeAxis(
                coordinate="time",
                epoch="2024-01-01T00:00:00Z",
                cadence_s=900,
                mode="floor",
                end_date="2024-01-01T01:00:00Z",
            ),
        },
    )


def _integer_spec(*, name: str = "integer_only") -> IndexSpec:
    return IndexSpec(
        name=name,
        groups={
            "data": IntegerAxis(slot_count=3),
            "quality": IntegerAxis(slot_count=7),
        },
    )


def _mixed_spec(*, name: str = "mixed") -> IndexSpec:
    return IndexSpec(
        name=name,
        groups={
            "integer": IntegerAxis(slot_count=5),
            "regular": RegularTimeAxis(
                coordinate="time",
                epoch="2024-01-01T00:00:00Z",
                cadence_s=600,
                mode="exact",
                slot_count=2,
            ),
        },
    )


def _unbounded_regular_spec(*, name: str = "unbounded_regular") -> IndexSpec:
    return IndexSpec(
        name=name,
        groups={
            "data": RegularTimeAxis(
                coordinate="time",
                epoch="2024-01-01T00:00:00Z",
                cadence_s=600,
                mode="floor",
            ),
        },
    )


def test_canonical_index_payload_is_deterministic_for_regular_axes() -> None:
    resolved = resolve_index_spec(_regular_spec(), time_dim_name="time")

    payload_a = resolved.canonical_index_payload()
    payload_b = resolved.canonical_index_payload()

    assert payload_a == payload_b
    assert payload_a["schema_version"] == "v1"
    assert payload_a["name"] == "regular_only"
    assert payload_a["groups"]["data_a"] == {
        "kind": "regular_time",
        "size": 4,
        "params": {
            "epoch": "2024-01-01T00:00:00Z",
            "cadence_s": 300,
            "mode": "exact",
        },
    }
    assert payload_a["groups"]["data_b"] == {
        "kind": "regular_time",
        "size": 4,
        "params": {
            "epoch": "2024-01-01T00:00:00Z",
            "cadence_s": 900,
            "mode": "floor",
            "end_date": "2024-01-01T01:00:00Z",
        },
    }


def test_unbounded_regular_axis_payload_uses_null_size_and_roundtrips() -> None:
    resolved = resolve_index_spec(_unbounded_regular_spec(), time_dim_name="time")

    assert resolved.position("data", "2024-01-01T00:00:00Z") == 0
    assert resolved.position("data", "2024-01-01T00:19:59Z") == 1
    assert resolved.position("data", "2024-01-02T00:00:00Z") == 144

    payload = resolved.canonical_index_payload()
    assert payload["groups"]["data"] == {
        "kind": "regular_time",
        "size": None,
        "params": {
            "epoch": "2024-01-01T00:00:00Z",
            "cadence_s": 600,
            "mode": "floor",
        },
    }

    record = resolved.as_resolved_index_record(run_id="r1", recorded_at="2026-08-20T00:00:00Z")
    assert ResolvedIndexRecord.from_json_bytes(record.to_json_bytes()) == record


def test_canonical_index_payload_is_deterministic_for_integer_axes() -> None:
    resolved = resolve_index_spec(_integer_spec(), time_dim_name="time")

    payload = resolved.canonical_index_payload()

    assert payload == resolved.canonical_index_payload()
    assert payload["groups"] == {
        "data": {"kind": "integer", "size": 3, "params": {}},
        "quality": {"kind": "integer", "size": 7, "params": {}},
    }


def test_canonical_index_payload_handles_mixed_axis_kinds() -> None:
    resolved = resolve_index_spec(_mixed_spec(), time_dim_name="time")

    payload = resolved.canonical_index_payload()

    assert payload["groups"]["integer"] == {"kind": "integer", "size": 5, "params": {}}
    assert payload["groups"]["regular"]["kind"] == "regular_time"
    assert payload["groups"]["regular"]["params"] == {
        "epoch": "2024-01-01T00:00:00Z",
        "cadence_s": 600,
        "mode": "exact",
    }


def test_identity_hash_uses_canonical_index_bytes_contract() -> None:
    resolved = resolve_index_spec(_mixed_spec(), time_dim_name="time")

    expected = hashlib.sha256(canonical_index_bytes(resolved.canonical_index_payload())).hexdigest()

    assert resolved.identity_hash == expected


def test_as_resolved_index_record_roundtrips_through_json_bytes() -> None:
    resolved = resolve_index_spec(_mixed_spec(), time_dim_name="time")

    record = resolved.as_resolved_index_record(run_id="r1", recorded_at="2026-08-20T00:00:00Z")
    recovered = ResolvedIndexRecord.from_json_bytes(record.to_json_bytes())

    assert recovered == record
    assert recovered.index == resolved.canonical_index_payload()
    assert recovered.recorded_by_run_id == "r1"


def test_as_resolved_index_record_uses_provided_run_metadata_only_in_record_fields() -> None:
    resolved = resolve_index_spec(_regular_spec(), time_dim_name="time")

    record = resolved.as_resolved_index_record(run_id="r1", recorded_at="2026-08-20T00:00:00Z")

    assert record.recorded_by_run_id == "r1"
    assert record.recorded_at == "2026-08-20T00:00:00Z"
    assert record.index == resolved.canonical_index_payload()
    assert record.identity_hash == resolved.identity_hash


def test_regular_only_identity_hash_diverges_from_legacy_slot_index_model() -> None:
    resolved = resolve_index_spec(_regular_spec(), time_dim_name="time")
    legacy = resolved.as_legacy_slot_index_model()

    assert legacy is not None
    assert resolved.identity_hash != legacy.identity_hash
