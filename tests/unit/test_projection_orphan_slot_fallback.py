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

"""Tests that _run_info_from_entry heals missing slot fields from run_id suffix."""

from __future__ import annotations

import logging

from firecube.core.controlplane._projection import ManifestProjection


def _make_projection() -> ManifestProjection:
    """Return a ManifestProjection with a minimal stub repo."""

    class _StubRepo:
        run_stale_threshold_s = 3600
        log = logging.getLogger("test")

    proj = ManifestProjection.__new__(ManifestProjection)
    proj._repo = _StubRepo()
    return proj


def test_orphan_payload_heals_slot_range_from_run_id() -> None:
    proj = _make_projection()
    payload = {
        "run_id": "prod-host-abc__group=SEVIRI_L15__slot=526080-526128",
        "status": "orphaned",
        "run_dir": "run-dir",
        "run_uri": "run-uri",
    }

    run_info = proj._run_info_from_entry("prod", payload)

    assert run_info.slot_range == (526080, 526128)
    assert run_info.slot_group == "SEVIRI_L15"


def test_explicit_payload_wins_over_parser() -> None:
    proj = _make_projection()
    payload = {
        "run_id": "prod-host-abc__group=SEVIRI_L15__slot=526080-526128",
        "status": "orphaned",
        "run_dir": "run-dir",
        "run_uri": "run-uri",
        "slot_range": [1, 2],
        "slot_group": "EXPLICIT",
    }

    run_info = proj._run_info_from_entry("prod", payload)

    assert run_info.slot_range == (1, 2)
    assert run_info.slot_group == "EXPLICIT"


def test_no_suffix_no_heal() -> None:
    proj = _make_projection()
    payload = {
        "run_id": "plain-run-id",
        "status": "orphaned",
        "run_dir": "run-dir",
        "run_uri": "run-uri",
    }

    run_info = proj._run_info_from_entry("prod", payload)

    assert run_info.slot_range is None
    assert run_info.slot_group is None
