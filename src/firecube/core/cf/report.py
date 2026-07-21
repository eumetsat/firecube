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

"""Typed CF report data structures."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum


class CFSeverity(StrEnum):
    error = "error"
    warning = "warning"
    info = "info"


@dataclass(frozen=True, slots=True)
class CFFinding:
    id: str
    severity: CFSeverity
    target: str
    message: str
    suggested_fix: str | None = None


@dataclass(slots=True)
class _CFSummary:
    errors: int
    warnings: int
    info: int


@dataclass(slots=True)
class CFReport:
    product: str
    group: str
    findings: list[CFFinding] = field(default_factory=list)

    @property
    def summary(self) -> _CFSummary:
        return _CFSummary(
            errors=sum(1 for f in self.findings if f.severity == CFSeverity.error),
            warnings=sum(1 for f in self.findings if f.severity == CFSeverity.warning),
            info=sum(1 for f in self.findings if f.severity == CFSeverity.info),
        )
