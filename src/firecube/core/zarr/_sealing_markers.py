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

"""Sealing marker attribute name constants for coordinate-array lifecycle management.

These constants name the reserved Zarr array attributes that firecube stamps
during preallocate and consolidate operations. They are re-exported from
``firecube.core.api`` for use by tooling that inspects cube state.
"""

from __future__ import annotations

from typing import Final

__all__ = ["ATTR_CONSOLIDATED_AT", "ATTR_COORD_MANAGED", "ATTR_PREALLOCATED"]

ATTR_PREALLOCATED: Final[str] = "firecube_preallocated"
"""Zarr array attr stamped on a time coord array after dense preallocate materialization.

Presence of this attr (with value ``True``) means the coord array was written
dense during ``firecube zarr preallocate``. Subsequent ``write_timestamp``
calls perform an equality drift check instead of creating new chunk files.
"""

ATTR_COORD_MANAGED: Final[str] = "firecube_coord_managed"
"""Zarr array attr for an engine-managed unsealed coordinate.

Presence of this attr (with value ``True``) means the materializer may fill
NaT holes during staged seeding, while pods verify-or-error on writes. It is
mutually exclusive with ``ATTR_PREALLOCATED``.
"""

ATTR_CONSOLIDATED_AT: Final[str] = "firecube_consolidated_at"
"""Zarr array attr stamped on a time coord array after ``consolidate-time-coord``.

Value is an ISO 8601 UTC timestamp string recording when consolidation ran.
A ``ConsolidatedTimeCoord`` WAL event accompanies this attr; ``ResumeGuard``
reads that event to block further ingest on the sealed cube.
"""
