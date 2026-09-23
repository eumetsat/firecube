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

"""End-to-end staged append with a non-Gregorian CF calendar time axis.

The bug this protects: staged mode (``GenericZarrIngestor``, xarray append)
with a time coordinate on a non-Gregorian calendar (e.g. ``360_day``) wrote
correctly on the first ingest, but (a) coverage ``time_min``/``time_max``
silently reported ``null`` because ``AppendCoverageBuilder.record_batch`` only
updated bounds for a ``datetime64``-decoded array, and (b) a SECOND ingest
(``--option resume_existing=true``) into the same store always crashed with
``TypeError: ufunc 'isnan' not supported for the input types`` from
``AppendOrder._maximum``, because a non-standard calendar decodes to an
object array of ``cftime`` scalars, not ``datetime64``.

Uses the ``calendar_time_test_plugin`` fixture (a ``GenericZarrIngestor``
whose time axis is built with ``xr.date_range(..., calendar=..., use_cftime=True)``)
through the real CLI (``click.testing.CliRunner``) against a real local Zarr
store under ``tmp_path``. ``calendar=None`` is the Gregorian control case in
the same harness.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, cast

import pytest
import xarray as xr
import zarr
from click.testing import CliRunner, Result

from firecube.cli.main import cli

pytestmark = pytest.mark.integration

_PLUGIN = "calendar_time_test_plugin"


def _make_dummy_input(tmp_path: Path) -> Path:
    source = tmp_path / "dummy_input"
    source.mkdir(exist_ok=True)
    (source / "dummy.nc").touch(exist_ok=True)
    return source


def _ingest_args(
    source: Path,
    target: Path,
    *,
    product: str,
    start: str,
    count: int,
    calendar: str | None,
    resume: bool = False,
) -> list[str]:
    args = [
        "ingest",
        _PLUGIN,
        "--input-data",
        str(source),
        "--target",
        f"file://{target}",
        "--product-name",
        product,
        "--storage-type",
        "local",
        "--storage-driver",
        "fsspec",
        "--output-format",
        "zarr",
        "--write-mode",
        "staged",
        "--option",
        "no_progress=true",
        "--option",
        "pipeline_batch_size=100",
        "--option",
        f"start={start}",
        "--option",
        f"count={count}",
    ]
    if calendar is not None:
        args += ["--option", f"calendar={calendar}"]
    if resume:
        args += ["--option", "resume_existing=true"]
    return args


def _run(args: list[str]) -> Result:
    return CliRunner().invoke(cli, args)


def _manifest(result: Result) -> dict[str, Any]:
    text = result.output
    idx = text.rfind('{\n  "plugin"')
    assert idx >= 0, text
    payload = json.loads(text[idx:])
    assert isinstance(payload, dict), payload
    return payload


def _coverage_entries(manifest: dict[str, Any]) -> list[dict[str, Any]]:
    return list(manifest["metrics"]["zarr"]["coverage"])


def _open_time_decoded(target: Path) -> xr.DataArray:
    ds = xr.open_zarr(str(target), group="default", consolidated=False, zarr_format=3)
    try:
        return ds["time"].load()
    finally:
        ds.close()


def _raw_time_attrs(target: Path) -> dict[str, Any]:
    root = zarr.open_group(store=str(target), mode="r", zarr_format=3, use_consolidated=False)
    array = cast(Any, root["default"])["time"]
    return {"dtype": str(array.dtype), **dict(array.attrs)}


@pytest.mark.parametrize(
    ("calendar", "run1_start", "run1_count", "run2_start", "run2_count"),
    [
        pytest.param("360_day", "2049-01-01", 30, "2049-02-01", 30, id="360_day"),
        pytest.param("noleap", "2001-01-01", 31, "2001-02-01", 28, id="noleap"),
        pytest.param(None, "2024-01-01", 10, "2024-01-11", 10, id="gregorian_control"),
    ],
)
def test_first_ingest_then_append_resume_round_trips(
    tmp_path: Path,
    calendar: str | None,
    run1_start: str,
    run1_count: int,
    run2_start: str,
    run2_count: int,
) -> None:
    """First ingest + a second ``resume_existing`` append both exit 0, in order.

    Covers: exit codes, step count and monotonic order, on-disk ``units`` +
    ``calendar`` attrs preserved, and (for the calendar cases) that
    ``xr.open_zarr`` decodes the coordinate into calendar-valued (cftime)
    objects rather than crashing or silently mislabeling it as Gregorian.
    """
    source = _make_dummy_input(tmp_path)
    target = tmp_path / f"store_{calendar or 'gregorian'}.zarr"

    first = _run(
        _ingest_args(
            source, target, product="p", start=run1_start, count=run1_count, calendar=calendar
        )
    )
    assert first.exit_code == 0, first.output

    second = _run(
        _ingest_args(
            source,
            target,
            product="p",
            start=run2_start,
            count=run2_count,
            calendar=calendar,
            resume=True,
        )
    )
    assert second.exit_code == 0, (
        f"second (resume_existing) ingest failed for calendar={calendar!r}:\n{second.output}"
    )

    total = run1_count + run2_count
    time_values = _open_time_decoded(target).values
    assert time_values.size == total

    if calendar is not None:
        assert time_values.dtype.kind == "O", (
            f"calendar={calendar!r} coordinate must decode to an object array of "
            f"calendar-valued scalars, got dtype={time_values.dtype!r}"
        )
        assert all(str(getattr(v, "calendar", None)) in {calendar, "standard"} for v in time_values)
        # strictly increasing, using the calendar objects' own ordering
        assert all(time_values[i] < time_values[i + 1] for i in range(total - 1))

        raw_attrs = _raw_time_attrs(target)
        assert raw_attrs["calendar"] == calendar
        assert "units" in raw_attrs
        assert raw_attrs["dtype"] == "int64"
    else:
        assert time_values.dtype.kind == "M"


def test_360_day_calendar_keeps_february_30(tmp_path: Path) -> None:
    """The defining 360_day behavior: 02-30 is a real, present date after a resumed append."""
    source = _make_dummy_input(tmp_path)
    target = tmp_path / "store.zarr"

    first = _run(
        _ingest_args(source, target, product="p", start="2049-01-01", count=30, calendar="360_day")
    )
    assert first.exit_code == 0, first.output
    second = _run(
        _ingest_args(
            source,
            target,
            product="p",
            start="2049-02-01",
            count=30,
            calendar="360_day",
            resume=True,
        )
    )
    assert second.exit_code == 0, second.output

    time_values = _open_time_decoded(target).values
    has_feb_30 = any(
        getattr(value, "month", None) == 2 and getattr(value, "day", None) == 30
        for value in time_values
    )
    assert has_feb_30, [str(v) for v in time_values]


@pytest.mark.parametrize(
    ("calendar", "start", "count"),
    [
        pytest.param("360_day", "2049-01-01", 10, id="360_day"),
        pytest.param("noleap", "2001-01-01", 10, id="noleap"),
        pytest.param(None, "2024-01-01", 10, id="gregorian_control"),
    ],
)
def test_coverage_bounds_non_null_each_run(
    tmp_path: Path, calendar: str | None, start: str, count: int
) -> None:
    """Coverage ``time_min``/``time_max`` are real, non-null ISO strings on BOTH runs.

    Before the fix, a calendar-valued batch's bounds were silently dropped
    (``time_min: null, time_max: null``); the Gregorian control proves the
    calendar case now matches the always-worked datetime64 behavior.
    """
    source = _make_dummy_input(tmp_path)
    target = tmp_path / f"store_{calendar or 'gregorian'}.zarr"

    first = _run(
        _ingest_args(source, target, product="p", start=start, count=count, calendar=calendar)
    )
    assert first.exit_code == 0, first.output
    first_coverage = _coverage_entries(_manifest(first))
    assert first_coverage, "first run produced no coverage entries"
    for entry in first_coverage:
        assert entry["time_min"] is not None, entry
        assert entry["time_max"] is not None, entry
        assert isinstance(entry["time_min"], str)
        assert isinstance(entry["time_max"], str)

    later_start = (
        "2049-06-01" if calendar == "360_day" else "2001-06-01" if calendar else "2024-06-01"
    )
    second = _run(
        _ingest_args(
            source,
            target,
            product="p",
            start=later_start,
            count=count,
            calendar=calendar,
            resume=True,
        )
    )
    assert second.exit_code == 0, second.output
    second_coverage = _coverage_entries(_manifest(second))
    assert second_coverage, "second (resume) run produced no coverage entries"
    for entry in second_coverage:
        assert entry["time_min"] is not None, entry
        assert entry["time_max"] is not None, entry


@pytest.mark.parametrize(
    "calendar",
    [pytest.param("360_day", id="360_day"), pytest.param(None, id="gregorian_control")],
)
def test_overlapping_reingest_matches_gregorian_outcome(
    tmp_path: Path, calendar: str | None
) -> None:
    """Re-ingesting the exact same range is refused without resume, deduped with resume.

    Asserts the calendar case gets the SAME outcome the Gregorian control
    gets from this harness: exit 1 (refused) without ``resume_existing``, and
    exit 0 with an unchanged store size when re-ingested WITH
    ``resume_existing=true`` (full-batch dedup, not a duplicate write).
    """
    source = _make_dummy_input(tmp_path)
    target = tmp_path / f"store_{calendar or 'gregorian'}.zarr"
    start = "2049-01-01" if calendar == "360_day" else "2024-01-01"

    first = _run(
        _ingest_args(source, target, product="p", start=start, count=10, calendar=calendar)
    )
    assert first.exit_code == 0, first.output
    length_after_first = _open_time_decoded(target).sizes["time"]

    without_resume = _run(
        _ingest_args(source, target, product="p", start=start, count=10, calendar=calendar)
    )
    assert without_resume.exit_code == 1, (
        f"non-resumed overlapping re-ingest should be refused for calendar={calendar!r}:\n"
        f"{without_resume.output}"
    )
    assert "resume_existing" in without_resume.output

    with_resume = _run(
        _ingest_args(
            source, target, product="p", start=start, count=10, calendar=calendar, resume=True
        )
    )
    assert with_resume.exit_code == 0, (
        f"resumed overlapping re-ingest should dedup, not fail, for calendar={calendar!r}:\n"
        f"{with_resume.output}"
    )
    assert _open_time_decoded(target).sizes["time"] == length_after_first, (
        "a fully-overlapping resumed re-ingest must not duplicate or grow the store"
    )
