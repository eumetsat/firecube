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

"""Internal coordinate-array marker lifecycle helpers."""

from __future__ import annotations

from enum import Enum, auto

from firecube.core.errors import SchemaDriftError
from firecube.core.zarr._sealing_markers import ATTR_COORD_MANAGED, ATTR_PREALLOCATED

__all__ = [
    "CoordLifecycleState",
    "assert_coord_markers_consistent",
    "raise_if_invalid",
    "resolve_coord_lifecycle",
]


def assert_coord_markers_consistent(attrs: dict, coord_path: str) -> None:
    """Raise ``SchemaDriftError`` when a coordinate carries both sealing markers.

    ``firecube_preallocated`` and ``firecube_coord_managed`` are mutually
    exclusive lifecycles for a time-coordinate array; their combined presence
    means a run crashed mid-transition or the store was edited out of band.
    Tooling that is about to stamp or trust either marker calls this first
    with the array's attribute dict and its store path (used in the error
    message). A consistent attribute set returns ``None``.
    """
    raise_if_invalid(resolve_coord_lifecycle(attrs), coord_path)


class CoordLifecycleState(Enum):
    """Internal lifecycle state derived from coordinate marker attributes."""

    LEGACY = auto()
    PREALLOCATED = auto()
    COORD_MANAGED = auto()
    INVALID_BOTH_MARKERS = auto()


def resolve_coord_lifecycle(attrs: dict) -> CoordLifecycleState:
    """Resolve coordinate-array lifecycle state from marker attributes."""
    has_preallocated = bool(attrs.get(ATTR_PREALLOCATED, False))
    has_coord_managed = bool(attrs.get(ATTR_COORD_MANAGED, False))

    if has_preallocated and has_coord_managed:
        return CoordLifecycleState.INVALID_BOTH_MARKERS
    if has_preallocated:
        return CoordLifecycleState.PREALLOCATED
    if has_coord_managed:
        return CoordLifecycleState.COORD_MANAGED
    return CoordLifecycleState.LEGACY


def raise_if_invalid(state: CoordLifecycleState, coord_path: str) -> None:
    """Raise when the resolved coordinate lifecycle is internally inconsistent."""
    if state is not CoordLifecycleState.INVALID_BOTH_MARKERS:
        return

    raise SchemaDriftError(
        f"coordinate array {coord_path} has both {ATTR_PREALLOCATED} and "
        f"{ATTR_COORD_MANAGED} markers; states are mutually exclusive; a run "
        "may have crashed mid-migration or the store was manually edited"
    )
