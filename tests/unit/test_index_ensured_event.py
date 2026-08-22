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

"""Unit tests for IndexEnsuredEvent WAL persistence."""

from __future__ import annotations

import pytest

from firecube.core.controlplane.types import IndexEnsuredEvent


def test_index_ensured_event_constructs_and_serializes() -> None:
    event = IndexEnsuredEvent(
        run_id="r1",
        product="p1",
        identity_hash="abc",
        axis_kinds=("regular", "integer"),
        groups=("data", "time"),
        outcome="created",
        timestamp="2026-08-19T12:00:00Z",
    )

    assert event.run_id == "r1"
    assert event.axis_kinds == ("integer", "regular")
    assert event.groups == ("data", "time")
    assert event.to_dict() == {
        "schema_version": "v2",
        "run_id": "r1",
        "product": "p1",
        "identity_hash": "abc",
        "axis_kinds": ["integer", "regular"],
        "groups": ["data", "time"],
        "outcome": "created",
        "timestamp": "2026-08-19T12:00:00Z",
    }


def test_index_ensured_event_rejects_unknown_outcome() -> None:
    with pytest.raises(ValueError, match="outcome"):
        IndexEnsuredEvent(
            run_id="r1",
            product="p1",
            identity_hash="abc",
            axis_kinds=("integer",),
            groups=("data",),
            outcome="badval",  # type: ignore[arg-type]
            timestamp="2026-08-19T12:00:00Z",
        )


def test_index_ensured_event_rejects_axis_kinds_list() -> None:
    with pytest.raises(TypeError, match="axis_kinds"):
        IndexEnsuredEvent(
            run_id="r1",
            product="p1",
            identity_hash="abc",
            axis_kinds=["integer"],  # type: ignore[arg-type]
            groups=("data",),
            outcome="created",
            timestamp="2026-08-19T12:00:00Z",
        )


def test_index_ensured_event_rejects_groups_list() -> None:
    with pytest.raises(TypeError, match="groups"):
        IndexEnsuredEvent(
            run_id="r1",
            product="p1",
            identity_hash="abc",
            axis_kinds=("integer",),
            groups=["data"],  # type: ignore[arg-type]
            outcome="created",
            timestamp="2026-08-19T12:00:00Z",
        )


def test_index_ensured_event_requires_all_fields() -> None:
    with pytest.raises(TypeError):
        IndexEnsuredEvent(  # type: ignore[call-arg]
            run_id="r1",
            product="p1",
            identity_hash="abc",
            axis_kinds=("integer",),
            groups=("data",),
            outcome="created",
        )
