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

import pytest

from firecube.core.errors import SchemaDriftError
from firecube.core.zarr._coord_lifecycle import (
    CoordLifecycleState,
    raise_if_invalid,
    resolve_coord_lifecycle,
)
from firecube.core.zarr._sealing_markers import ATTR_COORD_MANAGED, ATTR_PREALLOCATED


@pytest.mark.parametrize(
    ("attrs", "expected"),
    [
        ({}, CoordLifecycleState.LEGACY),
        ({ATTR_PREALLOCATED: True}, CoordLifecycleState.PREALLOCATED),
        ({ATTR_COORD_MANAGED: True}, CoordLifecycleState.COORD_MANAGED),
        (
            {ATTR_PREALLOCATED: True, ATTR_COORD_MANAGED: True},
            CoordLifecycleState.INVALID_BOTH_MARKERS,
        ),
    ],
)
def test_resolve_coord_lifecycle_states(attrs, expected):
    assert resolve_coord_lifecycle(attrs) is expected


def test_coord_lifecycle_state_has_exact_members():
    assert [state.name for state in CoordLifecycleState] == [
        "LEGACY",
        "PREALLOCATED",
        "COORD_MANAGED",
        "INVALID_BOTH_MARKERS",
    ]


@pytest.mark.parametrize(
    "state",
    [
        CoordLifecycleState.LEGACY,
        CoordLifecycleState.PREALLOCATED,
        CoordLifecycleState.COORD_MANAGED,
    ],
)
def test_raise_if_invalid_noops_for_valid_states(state):
    raise_if_invalid(state, "group/time")


def test_raise_if_invalid_rejects_both_markers():
    with pytest.raises(SchemaDriftError, match="mutually exclusive") as exc_info:
        raise_if_invalid(CoordLifecycleState.INVALID_BOTH_MARKERS, "group/time")

    assert str(exc_info.value) == (
        "coordinate array group/time has both firecube_preallocated and "
        "firecube_coord_managed markers; states are mutually exclusive; a run "
        "may have crashed mid-migration or the store was manually edited"
    )
