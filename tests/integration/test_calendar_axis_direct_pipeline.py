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

"""End-to-end CLI contracts for calendar-declared time axes on the direct path.

Drives ``calendar_axis_test_plugin`` fixtures (installed by
``tests/fixtures/firecube_test_plugins``) through the real ``firecube``
CLI against real local Zarr stores under ``tmp_path``. Covers preallocate +
ingest for regular and irregular (explicit and AUTO) calendar axes, the
already-encoded-number input path, idempotency/resume, the preallocate-
required gate, wrong-calendar/Gregorian rejection at write time, the A1/A2
uncalendared-axis guards surfaced through the CLI, resolved-index identity,
``zarr index show --derived``, and the interaction with ``zarr validate``,
``zarr slots``, and ``zarr consolidate-time-coord``.

No mocks of firecube internals; assertions read the store (Zarr arrays,
``.firecube/index/current.json``) and CLI output only.

Deliberately does NOT snapshot/clear/restore the process-global
``firecube.ingestor.registry.loader.AVAILABLE_INGESTORS`` registry around
each test (the pattern some sibling fixture files use for dynamically
re-registered, per-test plugin classes): this file's plugins are all
statically ``@register_ingestor``-decorated, so the registry's normal
lazy, cached discovery already finds them once and keeps them for the rest
of the session. Clearing the registry and relying on re-import to refill it
is unsafe once *any* entry-point plugin module has already been imported in
the process, because ``importlib``/``entry_points().load()`` treat an
already-imported module as a cache hit and never re-run its
``@register_ingestor`` decorators -- so a clear there is never fully
undone, and a sibling test file that runs later in the same session (e.g.
one right after this one, alphabetically) can find its own plugin missing.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, cast

import calendar_axis_test_plugin  # noqa: F401  (fixture package must be installed)
import numpy as np
import pytest
import regular_axis_test_plugin  # noqa: F401  (fixture package must be installed)
import xarray as xr
import zarr
from click.testing import CliRunner

from firecube.cli.main import cli

pytestmark = pytest.mark.integration


_GROUP = "data"
_COORD = "time"


def _base_args(
    plugin: str,
    product: str,
    target: Path,
    *,
    options: dict[str, Any] | None = None,
) -> list[str]:
    args = [
        "--target",
        f"file://{target}",
        "--product-name",
        product,
        "--storage-type",
        "local",
        "--storage-driver",
        "fsspec",
        "--write-mode",
        "direct",
        "--option",
        "no_progress=true",
    ]
    for key, value in (options or {}).items():
        args.extend(["--option", f"{key}={value}"])
    return args


def _preallocate(
    plugin: str, target: Path, *, product: str | None = None, options: dict[str, Any] | None = None
) -> Any:
    return CliRunner().invoke(
        cli,
        [
            "zarr",
            "preallocate",
            plugin,
            *_base_args(plugin, product or plugin, target, options=options),
        ],
    )


def _ingest(
    plugin: str, target: Path, *, product: str | None = None, options: dict[str, Any] | None = None
) -> Any:
    return CliRunner().invoke(
        cli, ["ingest", plugin, *_base_args(plugin, product or plugin, target, options=options)]
    )


def _root(target: Path) -> Any:
    return zarr.open_group(store=str(target), mode="r", zarr_format=3)


def _open_xr(target: Path, *, group: str = _GROUP) -> xr.Dataset:
    return xr.open_zarr(str(target), group=group, consolidated=False, zarr_format=3)


def _error_text(result: Any) -> str:
    """Concatenate CLI stdout and the propagated exception's message.

    Some failures (uncaught ``ValueError`` from AUTO discovery, not wrapped
    into a ``click.ClickException``) never reach ``result.output`` under
    ``CliRunner`` but ARE what a real terminal invocation prints to stderr
    (via Python's default excepthook) before exiting non-zero. Checking both
    keeps the assertion honest to real CLI behaviour either way.
    """
    exc_text = "" if result.exception is None else str(result.exception)
    return f"{result.output}\n{exc_text}"


def _last_pretty_json_object(text: str) -> Any:
    """Parse the last pretty-printed ``{...}`` block in *text*.

    Some CLI commands interleave single-line structured JSON log records
    with a final pretty-printed (multi-line, indented) JSON report on
    stdout. The report is always the last such block, and its opening line
    is exactly ``"{"`` (indent 0), which no compact log line ever is.
    """
    lines = text.splitlines()
    start = None
    for index, line in enumerate(lines):
        if line.strip() == "{":
            start = index
    assert start is not None, f"no pretty-printed JSON object found in output:\n{text}"
    return json.loads("\n".join(lines[start:]))


def _snapshot(target: Path) -> dict[str, bytes]:
    return {
        p.relative_to(target).as_posix(): p.read_bytes() for p in target.rglob("*") if p.is_file()
    }


# ---------------------------------------------------------------------------
# 1. Regular axis: preallocate + ingest, 360_day and noleap.
# ---------------------------------------------------------------------------


@pytest.mark.contract
@pytest.mark.parametrize("calendar", ["360_day", "noleap"])
def test_regular_axis_preallocate_and_ingest(tmp_path: Path, calendar: str) -> None:
    target = tmp_path / "cube.zarr"
    options = {"calendar": calendar, "slot_count": 90}

    pre = _preallocate("calendar_axis_regular", target, options=options)
    assert pre.exit_code == 0, pre.output
    ing = _ingest("calendar_axis_regular", target, options=options)
    assert ing.exit_code == 0, ing.output

    coord = cast(Any, _root(target)[f"{_GROUP}/{_COORD}"])
    assert coord.dtype == np.dtype("int64")
    assert coord.attrs["units"] == "seconds since 2049-01-01 00:00:00"
    assert coord.attrs["calendar"] == calendar
    assert coord.attrs["firecube_preallocated"] is True

    ds = _open_xr(target)
    isoformats = [value.isoformat() for value in ds[_COORD].values]
    if calendar == "360_day":
        assert any(iso.startswith("2049-02-30") for iso in isoformats)
    else:
        assert not any(iso.startswith("2049-02-29") for iso in isoformats)

    # Every model date matches the expected cftime date for that slot.
    import cftime

    axis_units = "seconds since 2049-01-01 00:00:00"
    expected = cftime.num2date(np.arange(90) * 86400, units=axis_units, calendar=calendar)
    expected_iso = [value.isoformat() for value in np.asarray(expected).reshape(-1)]
    assert isoformats == expected_iso

    values = np.asarray(cast(Any, _root(target)[f"{_GROUP}/values"])[:]).reshape(-1)
    assert list(values) == list(range(90))


# ---------------------------------------------------------------------------
# 2. Irregular explicit and AUTO axes, with gaps.
# ---------------------------------------------------------------------------


@pytest.mark.contract
@pytest.mark.parametrize("calendar", ["360_day", "noleap"])
@pytest.mark.parametrize(
    "plugin", ["calendar_axis_irregular_explicit", "calendar_axis_irregular_auto"]
)
def test_irregular_axis_preallocate_and_ingest(tmp_path: Path, plugin: str, calendar: str) -> None:
    target = tmp_path / "cube.zarr"
    options = {"calendar": calendar}

    pre = _preallocate(plugin, target, options=options)
    assert pre.exit_code == 0, pre.output
    ing = _ingest(plugin, target, options=options)
    assert ing.exit_code == 0, ing.output

    coord = cast(Any, _root(target)[f"{_GROUP}/{_COORD}"])
    # The irregular axis's units are "days since ...", not seconds, so the
    # encoded values are the day offsets themselves.
    expected_days = np.array([0, 2, 5, 9, 14], dtype=np.int64)
    assert np.array_equal(np.asarray(coord[:]), expected_days)
    assert coord.attrs["calendar"] == calendar
    assert coord.attrs["units"] == "days since 2049-01-01 00:00:00"

    values = np.asarray(cast(Any, _root(target)[f"{_GROUP}/values"])[:]).reshape(-1)
    assert list(values) == [0.0, 1.0, 2.0, 3.0, 4.0]

    ds = _open_xr(target)
    isoformats = [value.isoformat() for value in ds[_COORD].values]
    assert isoformats == [
        "2049-01-01T00:00:00",
        "2049-01-03T00:00:00",
        "2049-01-06T00:00:00",
        "2049-01-10T00:00:00",
        "2049-01-15T00:00:00",
    ]


# ---------------------------------------------------------------------------
# 3. Already-encoded numbers resolve to the same slots as cftime objects.
# ---------------------------------------------------------------------------


def test_encoded_numbers_match_cftime_placement(tmp_path: Path) -> None:
    cftime_target = tmp_path / "cftime.zarr"
    encoded_target = tmp_path / "encoded.zarr"
    options = {"calendar": "360_day", "slot_count": 30}

    assert _preallocate("calendar_axis_regular", cftime_target, options=options).exit_code == 0
    assert _ingest("calendar_axis_regular", cftime_target, options=options).exit_code == 0

    assert (
        _preallocate("calendar_axis_regular_encoded", encoded_target, options=options).exit_code
        == 0
    )
    assert _ingest("calendar_axis_regular_encoded", encoded_target, options=options).exit_code == 0

    cftime_coord = np.asarray(cast(Any, _root(cftime_target)[f"{_GROUP}/{_COORD}"])[:])
    encoded_coord = np.asarray(cast(Any, _root(encoded_target)[f"{_GROUP}/{_COORD}"])[:])
    assert np.array_equal(cftime_coord, encoded_coord)

    cftime_values = np.asarray(cast(Any, _root(cftime_target)[f"{_GROUP}/values"])[:])
    encoded_values = np.asarray(cast(Any, _root(encoded_target)[f"{_GROUP}/values"])[:])
    assert np.array_equal(cftime_values, encoded_values)


# ---------------------------------------------------------------------------
# 4. Idempotency / resume.
# ---------------------------------------------------------------------------


def test_regular_axis_preallocate_is_idempotent_and_resume_is_a_noop(tmp_path: Path) -> None:
    target = tmp_path / "cube.zarr"
    options = {"calendar": "360_day", "slot_count": 10}

    assert _preallocate("calendar_axis_regular", target, options=options).exit_code == 0
    coord_before = np.asarray(cast(Any, _root(target)[f"{_GROUP}/{_COORD}"])[:])

    second_pre = _preallocate("calendar_axis_regular", target, options=options)
    assert second_pre.exit_code == 0, second_pre.output
    coord_after_reprealloc = np.asarray(cast(Any, _root(target)[f"{_GROUP}/{_COORD}"])[:])
    assert np.array_equal(coord_before, coord_after_reprealloc)

    assert _ingest("calendar_axis_regular", target, options=options).exit_code == 0
    values_before = np.asarray(cast(Any, _root(target)[f"{_GROUP}/values"])[:]).reshape(-1)

    resume_options = dict(options)
    resume_options["resume_existing"] = "true"
    resume = _ingest("calendar_axis_regular", target, options=resume_options)
    assert resume.exit_code == 0, resume.output

    coord_after_resume = np.asarray(cast(Any, _root(target)[f"{_GROUP}/{_COORD}"])[:])
    values_after_resume = np.asarray(cast(Any, _root(target)[f"{_GROUP}/values"])[:]).reshape(-1)
    assert np.array_equal(coord_before, coord_after_resume)
    assert np.array_equal(values_before, values_after_resume)
    assert list(values_after_resume) == list(range(10))


# ---------------------------------------------------------------------------
# 5. Ingest without preallocate on a calendar axis.
# ---------------------------------------------------------------------------


def test_ingest_without_preallocate_refuses_and_creates_no_coord(tmp_path: Path) -> None:
    target = tmp_path / "cube.zarr"
    options = {"calendar": "360_day", "slot_count": 10}

    result = _ingest("calendar_axis_regular", target, options=options)
    assert result.exit_code != 0
    text = _error_text(result)
    assert "firecube zarr preallocate" in text
    assert "360_day" in text
    assert "'data'" in text or "group 'data'" in text

    assert not target.joinpath(_GROUP, _COORD).exists(), (
        "a calendar coordinate array must not be created by ingest without preallocate"
    )


# ---------------------------------------------------------------------------
# 6. Wrong-calendar value and Gregorian datetime handed to a calendar axis.
# ---------------------------------------------------------------------------


def test_wrong_calendar_value_rejected_at_write_time(tmp_path: Path) -> None:
    target = tmp_path / "cube.zarr"
    assert _preallocate("calendar_axis_wrong_calendar", target).exit_code == 0

    result = _ingest("calendar_axis_wrong_calendar", target)
    assert result.exit_code != 0
    text = _error_text(result)
    assert "360_day" in text
    assert "noleap" in text

    values = np.asarray(cast(Any, _root(target)[f"{_GROUP}/values"])[:])
    assert bool(np.all(values == -1.0)), "nothing should have been written for the bad item"


def test_gregorian_value_rejected_on_calendar_axis(tmp_path: Path) -> None:
    target = tmp_path / "cube.zarr"
    assert _preallocate("calendar_axis_gregorian_value", target).exit_code == 0

    result = _ingest("calendar_axis_gregorian_value", target)
    assert result.exit_code != 0
    text = _error_text(result)
    assert "cannot address a calendar axis" in text

    values = np.asarray(cast(Any, _root(target)[f"{_GROUP}/values"])[:])
    assert bool(np.all(values == -1.0)), "nothing should have been written for the bad item"


# ---------------------------------------------------------------------------
# 7. Calendar-valued object handed to an axis WITHOUT calendar (A1/A2).
# ---------------------------------------------------------------------------


def test_uncalendared_regular_axis_error_mentions_calendar(tmp_path: Path) -> None:
    target = tmp_path / "cube.zarr"
    # This plugin declares NO calendar; no preallocate gate applies.
    result = _ingest("calendar_axis_uncalendared_regular", target)
    assert result.exit_code != 0
    text = _error_text(result)
    assert "declare calendar=" in text
    assert "360_day" in text


def test_uncalendared_auto_axis_error_mentions_calendar(tmp_path: Path) -> None:
    target = tmp_path / "cube.zarr"
    result = _ingest("calendar_axis_uncalendared_auto", target)
    assert result.exit_code != 0
    text = _error_text(result)
    assert "calendar=" in text
    assert "units=" in text
    assert "360_day" in text


# ---------------------------------------------------------------------------
# 8. Identity: calendar in the resolved-index record; different calendars
#    diverge; re-preallocate with a different calendar is refused.
# ---------------------------------------------------------------------------


def test_resolved_index_record_carries_calendar_and_identity_diverges(tmp_path: Path) -> None:
    target_a = tmp_path / "a.zarr"
    target_b = tmp_path / "b.zarr"
    options_a = {"calendar": "360_day", "slot_count": 5}
    options_b = {"calendar": "noleap", "slot_count": 5}

    assert _preallocate("calendar_axis_regular", target_a, options=options_a).exit_code == 0
    assert _preallocate("calendar_axis_regular", target_b, options=options_b).exit_code == 0

    record_a = json.loads((target_a / ".firecube" / "index" / "current.json").read_text())
    record_b = json.loads((target_b / ".firecube" / "index" / "current.json").read_text())

    assert record_a["index"]["groups"][_GROUP]["params"]["calendar"] == "360_day"
    assert record_b["index"]["groups"][_GROUP]["params"]["calendar"] == "noleap"
    assert record_a["identity_hash"] != record_b["identity_hash"]


def test_repreallocate_with_different_calendar_is_refused(tmp_path: Path) -> None:
    target = tmp_path / "cube.zarr"
    options_360 = {"calendar": "360_day", "slot_count": 5}
    options_noleap = {"calendar": "noleap", "slot_count": 5}

    assert _preallocate("calendar_axis_regular", target, options=options_360).exit_code == 0

    conflict = _preallocate("calendar_axis_regular", target, options=options_noleap)
    assert conflict.exit_code != 0
    text = _error_text(conflict)
    assert "resolved index" in text
    assert "360_day" in text
    assert "noleap" in text


# ---------------------------------------------------------------------------
# 9. `firecube zarr index show --derived` prints calendar dates incl. 02-30.
# ---------------------------------------------------------------------------


def test_index_show_derived_prints_calendar_dates(tmp_path: Path) -> None:
    target = tmp_path / "cube.zarr"
    options = {"calendar": "360_day", "slot_count": 90}
    assert _preallocate("calendar_axis_regular", target, options=options).exit_code == 0

    result = CliRunner().invoke(
        cli,
        [
            "zarr",
            "index",
            "show",
            "--target",
            f"file://{target}",
            "--product-name",
            "calendar_axis_regular",
            "--derived",
        ],
    )
    assert result.exit_code == 0, result.output
    assert "2049-02-30" in result.output
    assert f"derived_coordinates[{_GROUP!r}]" in result.output


# ---------------------------------------------------------------------------
# 10. `zarr validate` / `zarr slots` run without crashing on the calendar store.
# ---------------------------------------------------------------------------


def test_zarr_validate_runs_without_crashing_on_calendar_store(tmp_path: Path) -> None:
    target = tmp_path / "cube.zarr"
    options = {"calendar": "360_day", "slot_count": 10}
    assert _preallocate("calendar_axis_regular", target, options=options).exit_code == 0
    assert _ingest("calendar_axis_regular", target, options=options).exit_code == 0

    result = CliRunner().invoke(cli, ["zarr", "validate", "-p", f"file://{target}", "-g", _GROUP])
    assert result.exit_code == 0, result.output
    report = _last_pretty_json_object(result.output)
    assert report["is_valid"] is True
    assert report["validity_issues"] == []


def test_zarr_slots_runs_without_crashing_on_calendar_store(tmp_path: Path) -> None:
    target = tmp_path / "cube.zarr"
    options = {"calendar": "360_day", "slot_count": 10}
    assert _preallocate("calendar_axis_regular", target, options=options).exit_code == 0
    assert _ingest("calendar_axis_regular", target, options=options).exit_code == 0

    result = CliRunner().invoke(
        cli,
        [
            "zarr",
            "slots",
            "calendar_axis_regular",
            "--target",
            f"file://{target}",
            "--product-name",
            "calendar_axis_regular",
            "--write-mode",
            "direct",
            "--storage-type",
            "local",
            "--storage-driver",
            "fsspec",
            "--option",
            "calendar=360_day",
            "--option",
            "slot_count=10",
            "--format",
            "json",
            "--no-resume",
        ],
    )
    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert len(payload["ranges"]) >= 1


# ---------------------------------------------------------------------------
# 11. `zarr consolidate-time-coord` on the calendar store: safe no-op.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("explicit_time_dim", [True, False], ids=["time-dim-given", "discovered"])
def test_consolidate_time_coord_on_calendar_store_is_a_safe_noop(
    tmp_path: Path, explicit_time_dim: bool
) -> None:
    """Consolidation reports a calendar coordinate as already sealed and changes nothing.

    A calendar coordinate is fully materialized and sealed at preallocate, like
    a dense Gregorian exact-grid coordinate, so there is nothing to consolidate.
    The command must say so whether the time dimension is named or discovered:
    discovery has to recognise the encoded coordinate, not report that the
    group has no time coordinate at all.
    """
    target = tmp_path / "cube.zarr"
    options = {"calendar": "360_day", "slot_count": 10}
    assert _preallocate("calendar_axis_regular", target, options=options).exit_code == 0
    assert _ingest("calendar_axis_regular", target, options=options).exit_code == 0

    args = [
        "zarr",
        "consolidate-time-coord",
        "--target",
        f"file://{target}",
        "--product-name",
        "calendar_axis_regular",
        "--storage-type",
        "local",
        "--storage-driver",
        "fsspec",
    ]
    if explicit_time_dim:
        args += ["--time-dim", _COORD]

    before = _snapshot(target)
    result = CliRunner().invoke(cli, args)

    assert result.exit_code == 0, result.output
    assert f"Group /{_GROUP}: already sealed, skipping" in result.output
    assert f"Group /{_GROUP}: no time coord" not in result.output
    assert _snapshot(target) == before, "consolidate-time-coord must not mutate a sealed store"


# ---------------------------------------------------------------------------
# 12. Control: an existing Gregorian regular-axis fixture is unaffected.
# ---------------------------------------------------------------------------


def test_explicit_proleptic_gregorian_calendar_behaves_like_unset(tmp_path: Path) -> None:
    """An explicitly declared ``calendar="proleptic_gregorian"`` axis behaves like an unset one.

    Unlike a genuine (non-Gregorian) calendar axis, ingest succeeds without a
    prior ``firecube zarr preallocate`` call, and the resulting coordinate
    array is plain ``datetime64`` with no ``units``/``calendar`` attrs:
    ``proleptic_gregorian`` is Gregorian-like, so it is treated identically
    to no calendar declared at all.
    """
    target = tmp_path / "cube.zarr"
    options = {"calendar": "proleptic_gregorian", "slot_count": 10}

    ing = _ingest("calendar_axis_regular", target, options=options)
    assert ing.exit_code == 0, _error_text(ing)

    coord = cast(Any, _root(target)[f"{_GROUP}/{_COORD}"])
    assert coord.dtype.kind == "M"
    assert "units" not in coord.attrs
    assert "calendar" not in coord.attrs

    values = np.asarray(cast(Any, _root(target)[f"{_GROUP}/values"])[:]).reshape(-1)
    assert list(values) == list(range(10))


def test_gregorian_regular_axis_fixture_unchanged(tmp_path: Path) -> None:
    target = tmp_path / "cube.zarr"
    result = CliRunner().invoke(
        cli,
        [
            "zarr",
            "preallocate",
            "regular_axis_dense_coord",
            "--target",
            f"file://{target}",
            "--product-name",
            "regular_axis_dense_coord",
            "--storage-type",
            "local",
            "--storage-driver",
            "fsspec",
            "--write-mode",
            "direct",
            "--option",
            "no_progress=true",
        ],
    )
    assert result.exit_code == 0, result.output

    coord = cast(Any, _root(target)[f"{_GROUP}/{_COORD}"])
    assert coord.dtype.kind == "M"
    assert "units" not in coord.attrs
    assert "calendar" not in coord.attrs


# ---------------------------------------------------------------------------
# 13. Staged write-mode: the calendar-preallocate startup guard must resolve
#     the final target, not the staged workspace, so a successful preallocate
#     at the final URI followed by a staged ingest does not falsely refuse.
# ---------------------------------------------------------------------------


def _staged_base_args(
    plugin: str,
    product: str,
    target: Path,
    *,
    workspace: Path,
    options: dict[str, Any] | None = None,
) -> list[str]:
    """Staged-mode counterpart of ``_base_args``.

    Passes ``--write-mode staged`` so the engine's ambient ``write_mode``
    is *not* ``"direct"``, and pins the staged workspace to *workspace*
    via ``--option workspace=<path>``. The workspace pin is the invariant
    that makes the "error must NOT name the workspace path" check
    falsifiable: without pinning it, the workspace lands under an
    unpredictable ``tempfile.mkdtemp`` path and the assertion has no
    meaningful subject. The staged shape also exercises the invariant that
    the calendar-preallocate startup guard resolves the FINAL target, not
    the staged workspace: without that invariant the guard would refuse a
    successful preallocate at the final target.
    """
    args = [
        "--target",
        f"file://{target}",
        "--product-name",
        product,
        "--storage-type",
        "local",
        "--storage-driver",
        "fsspec",
        "--write-mode",
        "staged",
        "--option",
        f"workspace={workspace}",
        "--option",
        "no_progress=true",
    ]
    for key, value in (options or {}).items():
        args.extend(["--option", f"{key}={value}"])
    return args


def _preallocate_staged(
    plugin: str,
    target: Path,
    *,
    workspace: Path,
    product: str | None = None,
    options: dict[str, Any] | None = None,
) -> Any:
    return CliRunner().invoke(
        cli,
        [
            "zarr",
            "preallocate",
            plugin,
            *_staged_base_args(
                plugin, product or plugin, target, workspace=workspace, options=options
            ),
        ],
    )


def _ingest_staged(
    plugin: str,
    target: Path,
    *,
    workspace: Path,
    product: str | None = None,
    options: dict[str, Any] | None = None,
) -> Any:
    return CliRunner().invoke(
        cli,
        [
            "ingest",
            plugin,
            *_staged_base_args(
                plugin, product or plugin, target, workspace=workspace, options=options
            ),
        ],
    )


def test_staged_preallocate_calendar_happy_path(tmp_path: Path) -> None:
    """Staged DirectZarr + preallocate + calendar axis must complete end-to-end.

    Preallocate writes the calendar coordinate to the final target (its
    ``--write-mode`` flag is accepted for CLI parity but ignored, so the
    coordinate lands at the final URI regardless). A subsequent staged
    ingest must find that preallocated coordinate at startup and proceed,
    not refuse because it probed the (empty) workspace instead.
    """
    target = tmp_path / "cube.zarr"
    workspace = tmp_path / "staged_workspace"
    slot_count = 360
    options = {"calendar": "360_day", "slot_count": slot_count}

    pre = _preallocate_staged("calendar_axis_regular", target, workspace=workspace, options=options)
    assert pre.exit_code == 0, pre.output
    ing = _ingest_staged("calendar_axis_regular", target, workspace=workspace, options=options)
    assert ing.exit_code == 0, _error_text(ing)

    coord = cast(Any, _root(target)[f"{_GROUP}/{_COORD}"])
    assert coord.dtype == np.dtype("int64")
    assert coord.attrs["calendar"] == "360_day"
    assert coord.attrs["units"] == "seconds since 2049-01-01 00:00:00"
    assert coord.attrs["firecube_preallocated"] is True
    assert coord.shape == (slot_count,)

    values = np.asarray(cast(Any, _root(target)[f"{_GROUP}/values"])[:]).reshape(-1)
    assert len(values) == slot_count
    assert list(values) == list(range(slot_count)), (
        "every one of the 360 slots must be filled (360/360) -- a lost seed on"
        " the staged promotion path would have left holes at fill_value=-1.0"
    )

    ds = _open_xr(target)
    isoformats = [value.isoformat() for value in ds[_COORD].values]

    import cftime

    axis_units = "seconds since 2049-01-01 00:00:00"
    expected = cftime.num2date(np.arange(slot_count) * 86400, units=axis_units, calendar="360_day")
    expected_iso = [value.isoformat() for value in np.asarray(expected).reshape(-1)]
    assert isoformats == expected_iso
    assert any(iso.startswith("2049-02-30") for iso in isoformats), (
        "a 360_day calendar with 360 daily slots starting 2049-01-01 must span"
        " Feb 30, a date that is only representable in a non-Gregorian calendar"
    )


def test_staged_without_preallocate_refused(tmp_path: Path) -> None:
    """Staged DirectZarr + calendar axis, skip preallocate: refusal names the final target.

    The refusal must point the operator at the FINAL target URI (where they
    are meant to run preallocate), never at the staged workspace path. The
    error message must name the final target path and must NOT name the
    staged workspace path, so the operator is told exactly where to run
    ``firecube zarr preallocate``.
    """
    target = tmp_path / "cube.zarr"
    workspace = tmp_path / "staged_workspace"
    options = {"calendar": "360_day", "slot_count": 90}

    result = _ingest_staged("calendar_axis_regular", target, workspace=workspace, options=options)
    assert result.exit_code != 0
    text = _error_text(result)
    assert "firecube zarr preallocate" in text
    assert "360_day" in text
    assert "'data'" in text or "group 'data'" in text
    assert str(target) in text, (
        "refusal must name the final target URI where preallocate is expected"
        f" to run; got error text: {text!r}"
    )
    assert str(workspace) not in text, (
        "refusal must NOT leak the staged workspace path; that would be the"
        " pre-fix behaviour where the guard resolved to the workspace instead"
        f" of the final target. workspace={workspace!s}; error text: {text!r}"
    )
    assert not target.joinpath(_GROUP, _COORD).exists(), (
        "a calendar coordinate array must not be created by staged ingest without preallocate"
    )


try:
    from moto.server import ThreadedMotoServer as _ThreadedMotoServer  # noqa: F401

    _MOTO_S3_AVAILABLE = True
except ImportError:  # pragma: no cover - environment-dependent
    _MOTO_S3_AVAILABLE = False


# ---------------------------------------------------------------------------
# 14. R1 regression: windowed preallocate on a calendar axis must still write
#     the full coord grid, so `xr.open_zarr` can decode the store the moment
#     the command returns. The window scopes data-array preallocation, not
#     the engine-owned calendar coordinate.
# ---------------------------------------------------------------------------


def _preallocate_windowed(
    plugin: str,
    target: Path,
    *,
    slot_start: int,
    slot_end: int,
    product: str | None = None,
    options: dict[str, Any] | None = None,
) -> Any:
    return CliRunner().invoke(
        cli,
        [
            "zarr",
            "preallocate",
            plugin,
            *_base_args(plugin, product or plugin, target, options=options),
            "--slot-start",
            str(slot_start),
            "--slot-end",
            str(slot_end),
        ],
    )


_CALENDAR_WINDOW_SUFFIX = "window not applied to calendar coordinate"


def test_regular_calendar_windowed_preallocate_opens_full_store(tmp_path: Path) -> None:
    """R1: partial-window preallocate on a calendar axis writes the full grid.

    Pre-fix, ``--slot-start 0 --slot-end 30`` on a 90-slot calendar axis left
    slots 30..90 at the ``int64.min`` fill sentinel and ``xr.open_zarr``
    refused to decode the store. Because the coord array is engine-owned and
    the window applies only to data-array preallocation, every slot must
    still hold ``n * cadence_s`` after preallocate returns.
    """
    target = tmp_path / "cube.zarr"
    slot_count = 90
    options = {"calendar": "360_day", "slot_count": slot_count}

    result = _preallocate_windowed(
        "calendar_axis_regular",
        target,
        slot_start=0,
        slot_end=30,
        options=options,
    )
    assert result.exit_code == 0, result.output
    assert _CALENDAR_WINDOW_SUFFIX in result.output, (
        "calendar coord windowed preallocate must announce that the window "
        "was not applied to the coord array"
    )

    coord = cast(Any, _root(target)[f"{_GROUP}/{_COORD}"])
    values = np.asarray(coord[:])
    fill = np.iinfo(np.int64).min
    written = int((values != fill).sum())
    assert values.size == slot_count
    assert written == slot_count, (
        f"expected all {slot_count} calendar coord slots written; got {written}. "
        "Pre-fix, only the requested window was written and the rest held the "
        "int64 fill sentinel, which made xr.open_zarr refuse the store."
    )
    expected = np.arange(slot_count, dtype=np.int64) * 86400
    assert np.array_equal(values, expected)

    ds = _open_xr(target)
    isoformats = [value.isoformat() for value in ds[_COORD].values]
    assert len(isoformats) == slot_count
    assert any(iso.startswith("2049-02-30") for iso in isoformats), (
        "a 360_day calendar with 90 daily slots starting 2049-01-01 must span "
        "Feb 30, which only decodes when the full coord grid is materialized"
    )


def test_gregorian_windowed_preallocate_unchanged(tmp_path: Path) -> None:
    """R1 negative space: windowed preallocate on a Gregorian axis is unchanged.

    Only the requested slot range is written; the rest keep the NaT fill
    sentinel and the report line does NOT carry the calendar-window suffix.
    Gregorian axes stay ``datetime64``-typed, so a partially-written coord
    array is still openable by xarray.
    """
    target = tmp_path / "cube.zarr"
    # regular_axis_dense_coord: RegularTimeAxis(mode="exact", slot_count=1000),
    # no calendar declared; datetime64[ns] coord dtype.
    slot_end = 500
    result = _preallocate_windowed(
        "regular_axis_dense_coord",
        target,
        slot_start=0,
        slot_end=slot_end,
    )
    assert result.exit_code == 0, result.output
    assert _CALENDAR_WINDOW_SUFFIX not in result.output, (
        "Gregorian axes must not carry the calendar-window suffix: their "
        "windowed preallocate honors --slot-start/--slot-end unchanged."
    )

    coord = cast(Any, _root(target)[f"{_GROUP}/{_COORD}"])
    values = np.asarray(coord[:])
    assert values.dtype.kind == "M"
    assert values.size == 1000
    written_mask = ~np.isnat(values)
    assert int(written_mask.sum()) == slot_end, (
        f"Gregorian coord: expected exactly {slot_end} slots written, "
        f"got {int(written_mask.sum())}; the window semantics must not change."
    )
    assert bool(np.all(np.isnat(values[slot_end:]))), (
        "Gregorian coord: slots outside the requested window must remain NaT"
    )


def test_irregular_calendar_axis_windowed_preallocate_unchanged(tmp_path: Path) -> None:
    """R1 negative space: irregular calendar axes already materialize every value.

    The irregular-encoded path resolves ``axis.values`` up front and writes
    every one of them regardless of ``--slot-start``/``--slot-end`` (its
    values are enumerated by the plugin, not derived from the window). The
    report line does NOT carry the calendar-window suffix because the
    irregular code path does not accept the window at all.
    """
    target = tmp_path / "cube.zarr"
    options = {"calendar": "360_day"}
    # calendar_axis_irregular_explicit publishes 5 gapped values (day offsets
    # 0, 2, 5, 9, 14). The plugin declares slot_count = len(values) = 5.
    result = _preallocate("calendar_axis_irregular_explicit", target, options=options)
    assert result.exit_code == 0, result.output
    # Irregular path never emits the calendar-window suffix: it always
    # materializes every declared value, so there is no windowed variant.
    assert _CALENDAR_WINDOW_SUFFIX not in result.output

    coord = cast(Any, _root(target)[f"{_GROUP}/{_COORD}"])
    values = np.asarray(coord[:])
    expected = np.array([0, 2, 5, 9, 14], dtype=np.int64)
    assert np.array_equal(values, expected), (
        "irregular calendar coord must materialize every declared value; "
        "windowed preallocate is not a mode the irregular path exposes"
    )


def test_calendar_axis_empty_window_still_writes_full_grid(tmp_path: Path) -> None:
    """R1 edge: --slot-start=0 --slot-end=0 on a calendar axis writes the full grid.

    A calendar coord array is engine-owned and fully materialized regardless
    of the requested window. The CLI accepts a zero-slot window on a
    calendar axis (the window would otherwise gate data-array preallocation)
    because the coord array itself never applies it.
    """
    target = tmp_path / "cube.zarr"
    slot_count = 90
    options = {"calendar": "360_day", "slot_count": slot_count}

    result = _preallocate_windowed(
        "calendar_axis_regular",
        target,
        slot_start=0,
        slot_end=0,
        options=options,
    )
    assert result.exit_code == 0, result.output
    assert _CALENDAR_WINDOW_SUFFIX in result.output

    coord = cast(Any, _root(target)[f"{_GROUP}/{_COORD}"])
    values = np.asarray(coord[:])
    fill = np.iinfo(np.int64).min
    assert values.size == slot_count
    assert int((values != fill).sum()) == slot_count
    expected = np.arange(slot_count, dtype=np.int64) * 86400
    assert np.array_equal(values, expected)


def test_calendar_axis_interior_window_writes_full_grid(tmp_path: Path) -> None:
    """R1 edge: an interior --slot-start=10 --slot-end=20 still writes full grid.

    Neither the window's inner edge nor its position matters for the coord
    array: every slot from ``0`` to ``slot_count`` carries the nominal
    encoded value ``n * cadence_s``, and the report announces that the
    window was not applied.
    """
    target = tmp_path / "cube.zarr"
    slot_count = 90
    options = {"calendar": "360_day", "slot_count": slot_count}

    result = _preallocate_windowed(
        "calendar_axis_regular",
        target,
        slot_start=10,
        slot_end=20,
        options=options,
    )
    assert result.exit_code == 0, result.output
    assert _CALENDAR_WINDOW_SUFFIX in result.output

    coord = cast(Any, _root(target)[f"{_GROUP}/{_COORD}"])
    values = np.asarray(coord[:])
    fill = np.iinfo(np.int64).min
    assert values.size == slot_count
    assert int((values != fill).sum()) == slot_count, (
        "interior windows must not leave any calendar coord slot at fill"
    )
    expected = np.arange(slot_count, dtype=np.int64) * 86400
    assert np.array_equal(values, expected)


@pytest.mark.s3
@pytest.mark.skipif(not _MOTO_S3_AVAILABLE, reason="moto s3 fixture unavailable on this run")
def test_staged_s3_calendar_happy_path(tmp_path: Path) -> None:
    """Same shape as ``test_staged_preallocate_calendar_happy_path`` on moto S3.

    Uses ``ThreadedMotoServer`` (not ``moto.mock_aws()``) because staged
    promotion can traverse obstore, a Rust extension that bypasses
    Python-level mocks. Mirrors the setup in ``test_staged_seed_publish_s3``.
    """
    import boto3
    from moto.server import ThreadedMotoServer

    bucket = "calendar-cf-360-bucket"
    key = "staged-calendar.zarr"
    region = "us-east-1"
    access_key = "testing"
    secret_key = "testing"

    server = ThreadedMotoServer(port=0)
    server.start()
    try:
        host, port = server.get_host_and_port()
        endpoint = f"http://{host}:{port}"
        boto3_client = boto3.client(
            "s3",
            endpoint_url=endpoint,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name=region,
        )
        boto3_client.create_bucket(Bucket=bucket)

        target_uri = f"s3://{bucket}/{key}"
        workspace = tmp_path / "staged_workspace"
        slot_count = 90
        options = {"calendar": "360_day", "slot_count": slot_count}

        s3_env = {
            "AWS_ACCESS_KEY_ID": access_key,
            "AWS_SECRET_ACCESS_KEY": secret_key,
            "AWS_DEFAULT_REGION": region,
            "AWS_EC2_METADATA_DISABLED": "true",
            "AWS_S3_ADDRESSING_STYLE": "path",
            "FIRECUBE_ENDPOINT_URL": endpoint,
            "FIRECUBE_ACCESS_KEY": access_key,
            "FIRECUBE_SECRET_KEY": secret_key,
            "FIRECUBE_REGION": region,
            "FIRECUBE_PATH_STYLE": "true",
        }

        base_args = [
            "--target",
            target_uri,
            "--product-name",
            "calendar_axis_regular",
            "--storage-type",
            "s3",
            "--storage-driver",
            "fsspec",
            "--write-mode",
            "staged",
            "--option",
            f"workspace={workspace}",
            "--option",
            "no_progress=true",
        ]
        for k, v in options.items():
            base_args.extend(["--option", f"{k}={v}"])

        runner = CliRunner(env=s3_env)
        pre = runner.invoke(cli, ["zarr", "preallocate", "calendar_axis_regular", *base_args])
        assert pre.exit_code == 0, pre.output
        ing = runner.invoke(cli, ["ingest", "calendar_axis_regular", *base_args])
        assert ing.exit_code == 0, _error_text(ing)

        import fsspec

        s3fs = fsspec.filesystem(
            "s3",
            key=access_key,
            secret=secret_key,
            client_kwargs={"endpoint_url": endpoint},
        )
        from zarr.storage import FsspecStore

        root = zarr.open_group(
            store=FsspecStore(s3fs, path=f"{bucket}/{key}"),
            mode="r",
            zarr_format=3,
        )
        coord = cast(Any, root[f"{_GROUP}/{_COORD}"])
        assert coord.dtype == np.dtype("int64")
        assert coord.attrs["calendar"] == "360_day"
        assert coord.attrs["firecube_preallocated"] is True
        assert coord.shape == (slot_count,)
    finally:
        server.stop()
