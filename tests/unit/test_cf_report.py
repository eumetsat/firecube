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

"""Tests for CF report data structures."""

# pyright: reportMissingImports=false

from __future__ import annotations

from dataclasses import FrozenInstanceError

import pytest

from firecube.core.cf import CF_CHECK_IDS, CFFinding, CFReport, CFSeverity


def test_all_15_check_ids_present() -> None:
    assert len(CF_CHECK_IDS) == 15
    assert CF_CHECK_IDS["CF001"].severity is CFSeverity.error
    assert CF_CHECK_IDS["CF002"].severity is CFSeverity.warning
    assert CF_CHECK_IDS["CF007"].severity is CFSeverity.info


def test_cf_report_summary_counts() -> None:
    report = CFReport(
        product="p",
        group="g",
        findings=[
            CFFinding(id="CF001", severity=CFSeverity.error, target="/", message="e"),
            CFFinding(id="CF002", severity=CFSeverity.warning, target="/", message="w"),
            CFFinding(id="CF007", severity=CFSeverity.info, target="/", message="i"),
        ],
    )

    assert report.summary.errors == 1
    assert report.summary.warnings == 1
    assert report.summary.info == 1


def test_cf_finding_is_frozen() -> None:
    finding = CFFinding(id="CF001", severity=CFSeverity.error, target="/", message="m")

    with pytest.raises(FrozenInstanceError):
        finding.message = "n"  # type: ignore[misc]
