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

"""Canonical CF check identifiers and descriptions."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Final

from .report import CFSeverity


@dataclass(frozen=True, slots=True)
class CheckDescription:
    """Static metadata describing a CF check."""

    id: str
    severity: CFSeverity
    summary: str
    cf_section: str


CF_CHECK_IDS: Final[dict[str, CheckDescription]] = {
    "CF001": CheckDescription(
        id="CF001",
        severity=CFSeverity.error,
        summary="Global `Conventions` attr present and contains a CF version token",
        cf_section="§2.6.1",
    ),
    "CF002": CheckDescription(
        id="CF002",
        severity=CFSeverity.warning,
        summary="Global `title` attr present and is a non-empty string",
        cf_section="§2.6.2",
    ),
    "CF003": CheckDescription(
        id="CF003",
        severity=CFSeverity.warning,
        summary="Global `institution`, `source`, `history` recommended",
        cf_section="§2.6.2",
    ),
    "CF004": CheckDescription(
        id="CF004",
        severity=CFSeverity.error,
        summary="At least one dim has a coordinate variable identifiable as time (by attrs)",
        cf_section="§4.4",
    ),
    "CF005": CheckDescription(
        id="CF005",
        severity=CFSeverity.error,
        summary="Time coord `units` matches `<unit> since <reference>` regex",
        cf_section="§4.4.1",
    ),
    "CF006": CheckDescription(
        id="CF006",
        severity=CFSeverity.warning,
        summary="Time coord has `calendar` attribute",
        cf_section="§4.4.1",
    ),
    "CF007": CheckDescription(
        id="CF007",
        severity=CFSeverity.info,
        summary='Time coord has `axis="T"` for explicit identification',
        cf_section="§4.4",
    ),
    "CF008": CheckDescription(
        id="CF008",
        severity=CFSeverity.warning,
        summary="Latitude coord identifiable",
        cf_section="§4.1",
    ),
    "CF009": CheckDescription(
        id="CF009",
        severity=CFSeverity.warning,
        summary="Longitude coord identifiable",
        cf_section="§4.2",
    ),
    "CF010": CheckDescription(
        id="CF010",
        severity=CFSeverity.error,
        summary="Every dimensional data var has `units` attr",
        cf_section="§3.1",
    ),
    "CF011": CheckDescription(
        id="CF011",
        severity=CFSeverity.warning,
        summary="Every data var has `long_name` OR `standard_name`",
        cf_section="§3.2/§3.3",
    ),
    "CF012": CheckDescription(
        id="CF012",
        severity=CFSeverity.error,
        summary="All reference attrs resolve to existing variables",
        cf_section="§5/§7.1",
    ),
    "CF013": CheckDescription(
        id="CF013",
        severity=CFSeverity.error,
        summary="If `bounds` attr exists, shape is `(N, 2)`",
        cf_section="§7.1",
    ),
    "CF014": CheckDescription(
        id="CF014",
        severity=CFSeverity.warning,
        summary="Variable names match CF naming pattern",
        cf_section="§3.1",
    ),
    "CF015": CheckDescription(
        id="CF015",
        severity=CFSeverity.warning,
        summary="Coordinate variables are monotonic",
        cf_section="§5",
    ),
}
