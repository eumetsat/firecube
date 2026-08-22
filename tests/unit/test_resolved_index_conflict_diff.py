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

"""Unit tests for resolved-index conflict diff formatting."""

from __future__ import annotations

import hashlib
from typing import Any

import pytest

from firecube.core.controlplane.manager import _format_resolved_index_diff
from firecube.core.controlplane.types import canonical_index_bytes

pytestmark = pytest.mark.unit


def _index(
    *,
    name: str = "idx",
    groups: dict[str, dict[str, Any]] | None = None,
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "schema_version": "v1",
        "name": name,
        "groups": groups
        if groups is not None
        else {"data": {"kind": "integer", "size": 4, "params": {}}},
    }
    if extra:
        payload.update(extra)
    return payload


def test_diff_reports_groups_symmetric_difference() -> None:
    diff = _format_resolved_index_diff(
        _index(groups={"data_2km": {"kind": "integer", "size": 2, "params": {}}}),
        _index(groups={"data_500m": {"kind": "integer", "size": 5, "params": {}}}),
    )

    assert "only_in_stored" in diff
    assert "data_2km" in diff
    assert "only_in_incoming" in diff
    assert "data_500m" in diff


def test_diff_reports_per_group_cadence_change() -> None:
    diff = _format_resolved_index_diff(
        _index(groups={"data": {"kind": "regular_time", "size": 4, "params": {"cadence_s": 300}}}),
        _index(groups={"data": {"kind": "regular_time", "size": 4, "params": {"cadence_s": 600}}}),
    )

    assert "group 'data'" in diff
    assert "params" in diff
    assert "cadence_s" in diff
    assert "300" in diff
    assert "600" in diff


def test_diff_reports_per_group_axis_kind_change() -> None:
    diff = _format_resolved_index_diff(
        _index(groups={"data": {"kind": "regular_time", "size": 4, "params": {}}}),
        _index(groups={"data": {"kind": "integer", "size": 4, "params": {}}}),
    )

    assert "kind" in diff
    assert "regular_time" in diff
    assert "integer" in diff


def test_diff_reports_top_level_name_change() -> None:
    diff = _format_resolved_index_diff(_index(name="alpha"), _index(name="beta"))

    assert "name" in diff
    assert "alpha" in diff
    assert "beta" in diff


def test_diff_ends_with_both_truncated_hashes() -> None:
    stored = _index(name="alpha")
    incoming = _index(name="beta")
    stored_hash = hashlib.sha256(canonical_index_bytes(stored)).hexdigest()[:16]
    incoming_hash = hashlib.sha256(canonical_index_bytes(incoming)).hexdigest()[:16]

    diff = _format_resolved_index_diff(stored, incoming)

    lines = diff.splitlines()
    assert lines[-2] == f"stored_hash={stored_hash}"
    assert lines[-1] == f"incoming_hash={incoming_hash}"


def test_diff_omits_run_metadata_noise() -> None:
    diff = _format_resolved_index_diff(
        _index(extra={"recorded_at": "2026-08-19T00:00:00Z", "recorded_by_run_id": "fdhsi"}),
        _index(
            name="idx2", extra={"recorded_at": "2026-08-20T00:00:00Z", "recorded_by_run_id": "hrfi"}
        ),
    )

    assert "recorded_at" not in diff
    assert "recorded_by_run_id" not in diff
    assert "2026-08-19" not in diff
    assert "fdhsi" not in diff
    assert "hrfi" not in diff
