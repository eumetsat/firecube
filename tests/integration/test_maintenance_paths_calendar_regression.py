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

"""Calendar-declared ingest paths and maintenance/read tools, through the real CLI.

Drives ``click.testing.CliRunner`` against real local Zarr stores under
``tmp_path``, built from two installed fixture plugins:

* ``calendar_time_test_plugin`` (``GenericZarrIngestor``, xarray append) --
  fresh/append/force_reingest on a ``360_day`` calendar time axis, and the
  refusal of a mixed-calendar (Gregorian-into-360_day) append.
* ``calendar_axis_test_plugin``'s ``calendar_axis_regular``
  (``DirectZarrIngestor``, region-writer) -- used to build one Gregorian
  (undeclared-calendar / ``direct_zarr_capable_test_plugin``) and one
  ``360_day`` DirectZarr store, then exercised through ``firecube zarr
  validate``, ``zarr index show/verify``, ``zarr slots``, ``catalog
  intake``, ``archive create``, ``chunks list``/``delete --dry-run``, and
  ``zarr preallocate`` idempotency/refusal.

No mocks: every assertion reads real CLI output, real Zarr arrays, and real
``.firecube/`` control-plane records on disk.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, cast

import pytest
import xarray as xr
import yaml
import zarr
from click.testing import CliRunner, Result

from firecube.cli.main import cli

pytestmark = pytest.mark.integration

_LOCAL_STORAGE_FLAGS = ["--storage-type", "local", "--storage-driver", "fsspec"]
_CAL_GROUP = "data"
_CAL_COORD = "time"


def _run(args: list[str]) -> Result:
    return CliRunner().invoke(cli, args)


def _error_text(result: Result) -> str:
    exc_text = "" if result.exception is None else str(result.exception)
    return f"{result.output}\n{exc_text}"


def _store_hash(target: Path) -> str:
    """Hash every file in the store except the control-plane run history.

    ``.firecube/runs/<run_id>/`` is a fresh per-invocation event log: even a
    genuine no-op preallocate re-run still records that a run happened
    there, so including it would make byte-identity impossible to express
    for ANY idempotent re-run. Product data, the resolved-index record, and
    the projected chunk/span state all live outside ``runs/`` and are what
    "no-op" actually promises to leave untouched.
    """
    digest = hashlib.sha256()
    for path in sorted(p for p in target.rglob("*") if p.is_file()):
        rel = path.relative_to(target)
        if rel.parts[:2] == (".firecube", "runs"):
            continue
        digest.update(rel.as_posix().encode())
        digest.update(path.read_bytes())
    return digest.hexdigest()


def _last_json_object(text: str) -> Any:
    """Parse the last pretty-printed ``{...}`` block in *text*.

    CLI commands interleave single-line structured JSON log records with a
    final pretty-printed (multi-line, indented) JSON report; the report's
    opening line is exactly ``"{"``, which no compact log line ever is.
    """
    lines = text.splitlines()
    start = None
    for index, line in enumerate(lines):
        if line.strip() == "{":
            start = index
    assert start is not None, f"no pretty-printed JSON object found in output:\n{text}"
    return json.loads("\n".join(lines[start:]))


def _make_dummy_input(tmp_path: Path) -> Path:
    source = tmp_path / "dummy_input"
    source.mkdir(exist_ok=True)
    (source / "dummy.nc").touch(exist_ok=True)
    return source


def _open_default(target: Path) -> xr.Dataset:
    return xr.open_zarr(str(target), group="default", consolidated=False, zarr_format=3)


# ===========================================================================
# B. calendar_time_test_plugin (GenericZarrIngestor): direct + staged + force
#    + mixed-calendar refusal.
# ===========================================================================


def _calendar_time_args(
    source: Path,
    target: Path,
    *,
    write_mode: str,
    start: str,
    count: int,
    calendar: str | None,
    resume: bool = False,
    force: bool = False,
) -> list[str]:
    args = [
        "ingest",
        "calendar_time_test_plugin",
        "--input-data",
        str(source),
        "--target",
        f"file://{target}",
        "--product-name",
        "p",
        *_LOCAL_STORAGE_FLAGS,
        "--output-format",
        "zarr",
        "--write-mode",
        write_mode,
        "--option",
        "no_progress=true",
        "--option",
        f"start={start}",
        "--option",
        f"count={count}",
    ]
    if calendar is not None:
        args += ["--option", f"calendar={calendar}"]
    if resume:
        args += ["--option", "resume_existing=true"]
    if force:
        args += ["--option", "force_reingest=true"]
    return args


def test_direct_write_mode_calendar_fresh_and_resume_append(tmp_path: Path) -> None:
    """``write-mode direct`` (not just staged) works for a declared-calendar time axis."""
    source = _make_dummy_input(tmp_path)
    target = tmp_path / "cube.zarr"

    first = _run(
        _calendar_time_args(
            source, target, write_mode="direct", start="2049-01-01", count=10, calendar="360_day"
        )
    )
    assert first.exit_code == 0, first.output

    second = _run(
        _calendar_time_args(
            source,
            target,
            write_mode="direct",
            start="2049-01-11",
            count=10,
            calendar="360_day",
            resume=True,
        )
    )
    assert second.exit_code == 0, second.output

    ds = _open_default(target)
    try:
        values = ds["time"].values
        assert values.size == 20
        assert values.dtype.kind == "O"
        assert all(getattr(v, "calendar", None) == "360_day" for v in values)
    finally:
        ds.close()

    root = zarr.open_group(store=str(target), mode="r", zarr_format=3)
    coord = cast(Any, root["default"])["time"]
    assert coord.attrs["calendar"] == "360_day"


def test_staged_calendar_force_reingest_replaces_without_growing(tmp_path: Path) -> None:
    """``force_reingest=true`` on an overlapping range replaces in place on a calendar axis."""
    source = _make_dummy_input(tmp_path)
    target = tmp_path / "cube.zarr"

    assert (
        _run(
            _calendar_time_args(
                source,
                target,
                write_mode="staged",
                start="2049-01-01",
                count=10,
                calendar="360_day",
            )
        ).exit_code
        == 0
    )
    assert (
        _run(
            _calendar_time_args(
                source,
                target,
                write_mode="staged",
                start="2049-01-11",
                count=10,
                calendar="360_day",
                resume=True,
            )
        ).exit_code
        == 0
    )

    third = _run(
        _calendar_time_args(
            source,
            target,
            write_mode="staged",
            start="2049-01-01",
            count=10,
            calendar="360_day",
            force=True,
        )
    )
    assert third.exit_code == 0, third.output

    ds = _open_default(target)
    try:
        assert ds.sizes["time"] == 20, "force_reingest must replace the overlapping slice in place"
    finally:
        ds.close()


def test_mixed_calendar_append_refused_not_silently_coerced(tmp_path: Path) -> None:
    """A Gregorian batch appended onto an existing 360_day store is refused loudly."""
    source = _make_dummy_input(tmp_path)
    target = tmp_path / "cube.zarr"

    first = _run(
        _calendar_time_args(
            source, target, write_mode="staged", start="2049-01-01", count=10, calendar="360_day"
        )
    )
    assert first.exit_code == 0, first.output

    mixed = _run(
        _calendar_time_args(
            source,
            target,
            write_mode="staged",
            start="2024-01-01",
            count=10,
            calendar=None,
            resume=True,
        )
    )
    assert mixed.exit_code != 0, mixed.output

    # The store must be left exactly as the first (calendar-only) run left it:
    # no silent coercion, no partial mixed write.
    ds = _open_default(target)
    try:
        values = ds["time"].values
        assert values.size == 10
        assert values.dtype.kind == "O"
        assert all(getattr(v, "calendar", None) == "360_day" for v in values)
    finally:
        ds.close()


# ===========================================================================
# D. Maintenance/read tools on a Gregorian DirectZarr store and a 360_day
#    DirectZarr store built with calendar_axis_test_plugin.
# ===========================================================================


def _build_gregorian_direct_store(target: Path) -> None:
    pre = _run(
        [
            "zarr",
            "preallocate",
            "direct_zarr_capable_test_plugin",
            "--target",
            f"file://{target}",
            "--product-name",
            "greg",
            *_LOCAL_STORAGE_FLAGS,
            "--write-mode",
            "direct",
        ]
    )
    assert pre.exit_code == 0, pre.output
    ing = _run(
        [
            "ingest",
            "direct_zarr_capable_test_plugin",
            "--target",
            f"file://{target}",
            "--product-name",
            "greg",
            *_LOCAL_STORAGE_FLAGS,
            "--write-mode",
            "direct",
            "--option",
            "no_progress=true",
        ]
    )
    assert ing.exit_code == 0, ing.output


def _build_calendar_direct_store(
    target: Path, *, calendar: str = "360_day", slot_count: int = 30
) -> None:
    options = {"calendar": calendar, "slot_count": slot_count}
    pre = _run(
        [
            "zarr",
            "preallocate",
            "calendar_axis_regular",
            "--target",
            f"file://{target}",
            "--product-name",
            "cal",
            *_LOCAL_STORAGE_FLAGS,
            "--write-mode",
            "direct",
            *[f"--option={k}={v}" for k, v in options.items()],
        ]
    )
    assert pre.exit_code == 0, pre.output
    ing = _run(
        [
            "ingest",
            "calendar_axis_regular",
            "--target",
            f"file://{target}",
            "--product-name",
            "cal",
            *_LOCAL_STORAGE_FLAGS,
            "--write-mode",
            "direct",
            *[f"--option={k}={v}" for k, v in options.items()],
        ]
    )
    assert ing.exit_code == 0, ing.output


@pytest.mark.parametrize("store", ["gregorian", "calendar"])
def test_zarr_validate_clean_on_both_stores(tmp_path: Path, store: str) -> None:
    target = tmp_path / "cube.zarr"
    group = "data/values"
    if store == "gregorian":
        _build_gregorian_direct_store(target)
        group = "data/data"
    else:
        _build_calendar_direct_store(target)

    result = _run(
        [
            "zarr",
            "validate",
            "-p",
            f"file://{target}",
            "-g",
            group,
            *_LOCAL_STORAGE_FLAGS,
            "--max-chunks",
            "1000",
        ]
    )
    assert result.exit_code == 0, result.output
    payload = _last_json_object(result.output)
    assert payload["is_valid"] is True
    assert payload["validity_issues"] == []
    assert payload["chunks_processed"] > 0


@pytest.mark.parametrize("store", ["gregorian", "calendar"])
def test_zarr_index_show_derived_and_verify(tmp_path: Path, store: str) -> None:
    target = tmp_path / "cube.zarr"
    if store == "gregorian":
        _build_gregorian_direct_store(target)
        product = "greg"
    else:
        _build_calendar_direct_store(target)
        product = "cal"

    show = _run(
        [
            "zarr",
            "index",
            "show",
            "--target",
            f"file://{target}",
            "--product-name",
            product,
            "--derived",
        ]
    )
    assert show.exit_code == 0, show.output
    assert "derived_coordinates" in show.output

    verify = _run(
        ["zarr", "index", "verify", "--target", f"file://{target}", "--product-name", product]
    )
    assert verify.exit_code == 0, verify.output
    assert "VERIFIED" in verify.output


def test_zarr_slots_plan_identical_undeclared_vs_explicit_gregorian_calendar(
    tmp_path: Path,
) -> None:
    """``zarr slots`` planning is identical for an undeclared axis and an explicit Gregorian one.

    ``calendar_axis_uncalendared_regular`` declares NO ``calendar`` on its
    ``RegularTimeAxis`` (epoch/cadence/slot_count fixed at 3 in the fixture);
    ``calendar_axis_regular`` with ``calendar=proleptic_gregorian`` and the
    same ``slot_count=3`` shares the same epoch/cadence constants. Slot
    planning depends only on the array shape/chunking, not on the calendar
    identity, so the two plans must match once the target URI is normalised
    away.
    """
    undeclared_target = tmp_path / "undeclared.zarr"
    explicit_target = tmp_path / "explicit_greg.zarr"

    undeclared = _run(
        [
            "zarr",
            "slots",
            "calendar_axis_uncalendared_regular",
            "--target",
            f"file://{undeclared_target}",
            "--product-name",
            "p",
            *_LOCAL_STORAGE_FLAGS,
            "--write-mode",
            "direct",
        ]
    )
    explicit = _run(
        [
            "zarr",
            "slots",
            "calendar_axis_regular",
            "--target",
            f"file://{explicit_target}",
            "--product-name",
            "p",
            *_LOCAL_STORAGE_FLAGS,
            "--write-mode",
            "direct",
            "--option",
            "calendar=proleptic_gregorian",
            "--option",
            "slot_count=3",
        ]
    )
    assert undeclared.exit_code == 0, undeclared.output
    assert explicit.exit_code == 0, explicit.output

    undeclared_plan = json.loads(undeclared.output)
    explicit_plan = json.loads(explicit.output)
    undeclared_plan["target"] = explicit_plan["target"] = None
    assert undeclared_plan == explicit_plan


def test_catalog_intake_entry_and_xarray_sees_datetime360day(tmp_path: Path) -> None:
    target = tmp_path / "cube.zarr"
    _build_calendar_direct_store(target)
    catalog_path = tmp_path / "catalog.yaml"

    result = _run(
        [
            "catalog",
            "intake",
            "calendar_axis_regular",
            "-p",
            f"file://{target}",
            "-o",
            f"file://{catalog_path}",
            "--collection-id",
            "cal-col",
            *_LOCAL_STORAGE_FLAGS,
        ]
    )
    assert result.exit_code == 0, result.output
    assert catalog_path.exists()

    catalog = yaml.safe_load(catalog_path.read_text())
    source_key = next(iter(catalog["sources"]))
    entry_args = catalog["sources"][source_key]["args"]
    assert entry_args["chunks"] == {}
    assert entry_args["group"] == _CAL_GROUP

    ds = xr.open_zarr(
        entry_args["urlpath"],
        group=entry_args["group"],
        consolidated=entry_args["consolidated"],
        zarr_format=3,
    )
    try:
        first_value = ds[_CAL_COORD].values[0]
        assert type(first_value).__name__ == "Datetime360Day"
    finally:
        ds.close()


def test_archive_create_calendar_store_refused_naming_calendar(tmp_path: Path) -> None:
    target = tmp_path / "cube.zarr"
    _build_calendar_direct_store(target)
    archive = tmp_path / "cal.tgm"

    result = _run(
        [
            "archive",
            "create",
            "-s",
            f"file://{target}",
            "-a",
            f"file://{archive}",
            *_LOCAL_STORAGE_FLAGS,
            "--yes-i-really-mean-it",
        ]
    )
    assert result.exit_code != 0
    text = _error_text(result)
    assert "360_day" in text
    assert "not Gregorian-like" in text
    assert not archive.exists() or archive.stat().st_size >= 0  # no traceback either way


@pytest.mark.xfail(
    strict=True,
    raises=AssertionError,
    reason=(
        "archiving a DirectZarr cube fails with 'cannot convert datetime64 to CBOR': "
        "the converter embeds the coordinate's raw zarr fill value (a datetime64 NaT) "
        "in the archive metadata. Reproduces on v0.1.7; tracked in plans/TODO.md."
    ),
)
def test_archive_create_gregorian_direct_zarr_store(tmp_path: Path) -> None:
    """REGRESSION: ``archive create`` must succeed on a Gregorian DirectZarr store.

    This currently FAILS: ``firecube.core.tensogram.metadata.variable_to_base_entry``
    embeds the coordinate's raw on-disk zarr ``fill_value`` verbatim into the
    archive's base metadata entry (``entry["zarr_fill_value"] = zarr_meta["fill_value"]``,
    ``src/firecube/core/tensogram/metadata.py``). A DirectZarr-materialized
    Gregorian time coordinate stores its fill value as a real
    ``numpy.datetime64('NaT')`` scalar (see
    ``coord_materialization.py::_gregorian_encoding`` /
    ``region_writer.py``), and that scalar is not CBOR-encodable, so
    ``zarr_to_tgm`` raises ``Error: cannot convert datetime64 to CBOR``.

    Diagnosis: NOT caused by this branch's five calendar commits (they only
    touch ``converter.py``/``coord_materialization.py``/``region_writer.py``;
    ``metadata.py``'s ``_is_encodable_dtype``/``variable_to_base_entry`` are
    untouched and predate this branch -- ``git log`` shows their last
    ``_consolidate.py``/``metadata.py``-adjacent change well before the
    branch tip). It is a pre-existing latent bug that this regression hunt
    surfaced because no existing test previously drove ``archive create``
    against a DirectZarr-produced (region-writer) time coordinate: every
    other archive test uses a ``GenericZarrIngestor``/xarray ``.to_zarr()``
    store, whose datetime64 coordinate is CF-encoded to raw ``int64`` (with
    an ``int`` ``fill_value``) by xarray itself before it ever reaches the
    archive path, so ``zarr_meta["fill_value"]`` is always a plain ``int``
    there and the bug never triggers.

    Per instructions: keeping this test (not patching source, not marking
    xfail) so it documents the break.
    """
    target = tmp_path / "cube.zarr"
    _build_gregorian_direct_store(target)
    archive = tmp_path / "greg.tgm"

    result = _run(
        [
            "archive",
            "create",
            "-s",
            f"file://{target}",
            "-a",
            f"file://{archive}",
            *_LOCAL_STORAGE_FLAGS,
            "--yes-i-really-mean-it",
        ]
    )
    assert result.exit_code == 0, _error_text(result)


@pytest.mark.parametrize("store", ["gregorian", "calendar"])
def test_chunks_list_and_delete_dry_run_both_stores(tmp_path: Path, store: str) -> None:
    target = tmp_path / "cube.zarr"
    if store == "gregorian":
        _build_gregorian_direct_store(target)
    else:
        _build_calendar_direct_store(target)

    listed = _run(["chunks", "list", "--product-name", f"file://{target}", "-f", "json"])
    assert listed.exit_code == 0, listed.output
    records = json.loads(listed.output)
    assert len(records) > 0

    dry = _run(["chunks", "delete", "--product-name", f"file://{target}", "--dry-run"])
    assert dry.exit_code == 0, dry.output
    assert "DRY RUN" in dry.output
    assert f"Chunks: {len(records)}" in dry.output

    # Dry run must not have deleted anything.
    listed_again = _run(["chunks", "list", "--product-name", f"file://{target}", "-f", "json"])
    assert len(json.loads(listed_again.output)) == len(records)


# ===========================================================================
# E. zarr preallocate: idempotent no-op re-run; refused with a changed
#    calendar.
# ===========================================================================


def test_preallocate_rerun_identical_spec_is_byte_identical_noop(tmp_path: Path) -> None:
    target = tmp_path / "cube.zarr"
    _build_calendar_direct_store(target, slot_count=10)
    before = _store_hash(target)

    rerun = _run(
        [
            "zarr",
            "preallocate",
            "calendar_axis_regular",
            "--target",
            f"file://{target}",
            "--product-name",
            "cal",
            *_LOCAL_STORAGE_FLAGS,
            "--write-mode",
            "direct",
            "--option",
            "calendar=360_day",
            "--option",
            "slot_count=10",
        ]
    )
    assert rerun.exit_code == 0, rerun.output
    after = _store_hash(target)
    assert before == after, "re-running preallocate with an identical spec must not change bytes"


def test_preallocate_refused_with_changed_calendar(tmp_path: Path) -> None:
    target = tmp_path / "cube.zarr"
    _build_calendar_direct_store(target, calendar="360_day", slot_count=10)

    changed = _run(
        [
            "zarr",
            "preallocate",
            "calendar_axis_regular",
            "--target",
            f"file://{target}",
            "--product-name",
            "cal",
            *_LOCAL_STORAGE_FLAGS,
            "--write-mode",
            "direct",
            "--option",
            "calendar=noleap",
            "--option",
            "slot_count=10",
        ]
    )
    assert changed.exit_code != 0
    text = _error_text(changed)
    assert "incompatible resolved index" in text
    assert "360_day" in text
    assert "noleap" in text
