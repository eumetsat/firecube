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

"""Unit tests for ``firecube zarr preallocate --dry-run`` output formatting.

Covers the ``_emit_preallocate_dry_run`` grid-policy branch: calendar
regular axes render their ``first``/``last`` slot as an ISO 8601 date on
the axis's own calendar (with the calendar name in parentheses), while
Gregorian regular axes stay on today's raw ``resolved_index.coordinate``
form (``numpy.datetime64`` rendered by the f-string). Both fixtures are
statically registered by ``tests/fixtures/firecube_test_plugins`` and are
exercised through the real Click CLI (no monkeypatching of internals).
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import calendar_axis_test_plugin  # noqa: F401  (fixture package must be installed)
import pytest
import regular_axis_test_plugin  # noqa: F401  (fixture package must be installed)
from click.testing import CliRunner

from firecube.cli.main import cli

pytestmark = pytest.mark.unit


_CALENDAR_SUFFIX_RE = re.compile(r"\((?:360_day|noleap|gregorian|standard|julian|all_leap)\)")


def _dry_run(plugin: str, target: Path, *, options: dict[str, Any] | None = None) -> Any:
    args = [
        "zarr",
        "preallocate",
        plugin,
        "--target",
        f"file://{target}",
        "--product-name",
        plugin,
        "--storage-type",
        "local",
        "--storage-driver",
        "fsspec",
        "--write-mode",
        "direct",
        "--dry-run",
        "--option",
        "no_progress=true",
    ]
    for key, value in (options or {}).items():
        args.extend(["--option", f"{key}={value}"])
    return CliRunner().invoke(cli, args)


def test_calendar_axis_dry_run_prints_calendar_dates(tmp_path: Path) -> None:
    """Regular calendar axis dry-run renders first/last as calendar dates.

    Uses the ``calendar_axis_regular`` fixture plugin (defaults: epoch
    ``2049-01-01T00:00:00Z``, ``cadence_s=86400``, ``slot_count=90``,
    ``calendar="360_day"``). Post-fix, the dry-run must show the first date
    (``2049-01-01T00:00:00``) suffixed with ``(360_day)`` and NOT print the
    raw encoded integer form ``first=0; last=...``.
    """
    target = tmp_path / "cube.zarr"
    options = {"calendar": "360_day", "slot_count": 90}

    result = _dry_run("calendar_axis_regular", target, options=options)

    assert result.exit_code == 0, result.output
    # Calendar-date rendering (positive space): the first slot's ISO 8601
    # form on the axis's own calendar, with the calendar name in parens.
    assert "first=2049-01-01T00:00:00 (360_day);" in result.output, result.output
    # 90 slots at daily cadence on a 360-day calendar span 3 months of 30
    # days each: slot 89 lands on 2049-03-30.
    assert "last=2049-03-30T00:00:00 (360_day);" in result.output, result.output
    # Negative space: the pre-fix raw-integer form must be gone. Pre-fix,
    # a calendar axis printed ``first=0; last=7689600`` (encoded seconds
    # since epoch) which was unreadable without the operator doing the
    # cadence math themselves.
    assert "first=0;" not in result.output, result.output
    # The dry-run must NOT mutate the target; the store dir stays absent
    # (``--dry-run`` is documented as "makes zero filesystem mutations").
    assert not target.exists(), f"dry-run must not create {target!r}"


def test_gregorian_axis_dry_run_unchanged(tmp_path: Path) -> None:
    """Non-calendar (Gregorian) regular axis dry-run stays unchanged.

    Uses the ``regular_axis_dense_coord`` fixture plugin (no ``calendar``
    field on its ``RegularTimeAxis``, so the coord is ``datetime64[ns]``).
    Its dry-run report must NOT carry any ``(<calendar-name>)`` suffix on
    ``first``/``last``: those values are already ``numpy.datetime64``
    renderings and the calendar-date formatter is strictly gated on the
    axis's calendar being non-Gregorian-like.
    """
    target = tmp_path / "cube.zarr"

    result = _dry_run("regular_axis_dense_coord", target)

    assert result.exit_code == 0, result.output
    match = _CALENDAR_SUFFIX_RE.search(result.output)
    assert match is None, (
        "Gregorian axis dry-run must not carry a calendar-name suffix on "
        f"first/last; saw {match.group(0)!r} in output:\n{result.output}"
    )
    # Positive space: today's shape survives — ``first=``/``last=`` are
    # still emitted, just without a calendar suffix. datetime64 rendering
    # goes through ``str(np.datetime64(...))``.
    assert "policy=grid;" in result.output, result.output
    assert "first=2024-01-01T00:00:00;" in result.output, result.output
    assert not target.exists(), f"dry-run must not create {target!r}"


def test_explicit_gregorian_calendar_dry_run_matches_unset(tmp_path: Path) -> None:
    """An explicitly declared ``calendar="proleptic_gregorian"`` dry-runs like an unset one.

    Uses the ``calendar_axis_regular`` fixture plugin with its ``calendar``
    option set to the Gregorian-like name ``"proleptic_gregorian"`` instead
    of the default ``"360_day"``. The report must render the same shape as
    an axis with no calendar declared at all: no ``(<calendar>)`` suffix on
    ``first``/``last``, and no preallocation gate implied by ``policy=grid``
    alone (a calendar-name suffix is what signals the gate applies).
    """
    target = tmp_path / "cube.zarr"
    options = {"calendar": "proleptic_gregorian", "slot_count": 10}

    result = _dry_run("calendar_axis_regular", target, options=options)

    assert result.exit_code == 0, result.output
    match = _CALENDAR_SUFFIX_RE.search(result.output)
    assert match is None, (
        "explicit proleptic_gregorian dry-run must not carry a calendar-name "
        f"suffix on first/last; saw {match.group(0)!r} in output:\n{result.output}"
    )
    assert "(proleptic_gregorian)" not in result.output, result.output
    assert "policy=grid;" in result.output, result.output
    assert "first=2049-01-01T00:00:00;" in result.output, result.output
    assert "last=2049-01-10T00:00:00;" in result.output, result.output
    assert not target.exists(), f"dry-run must not create {target!r}"
