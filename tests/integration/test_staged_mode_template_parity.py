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

"""Parametrized regression test for the staged-mode template parity bug.

Bug
---
``DirectZarrIngestor`` does NOT call ``seed_staged_metadata_for_batch``
when running with ``write_mode="staged"`` and ``resume_existing=True``.
Without seeding, the temporary store starts empty so plugin logic that
reads the existing array length to derive the next ``ts_index`` sees 0
and Run B overwrites slot 0 instead of appending to slot 1.

``GenericZarrIngestor`` already calls the seeding helper from
``_build_zarr_batch_runtime`` in
``src/firecube/ingestor/templates/generic.py``; ``AppendStrategy`` then
reads the cumulative shape from the seeded ``zarr.json`` and appends to
the next slot correctly.

The test runs both templates with the same scenario (Run A writes T1 at
slot 0; Run B writes T2 with ``resume_existing=True``) and asserts the
physical slot placement on the final Zarr store after both runs.
Generic PASSES today. DirectZarr FAILS — that is the RED state required
by the staged-mode template-agnostic seeding plan (T1).
"""

from __future__ import annotations

import contextlib
from collections.abc import Iterable
from pathlib import Path
from typing import Any, ClassVar

import numpy as np
import pandas as pd
import pyarrow.parquet as pq
import pytest
import xarray as xr
import zarr

from firecube.ingestor.api import (
    DirectZarrIngestor,
    GenericParquetIngestor,
    GenericZarrIngestor,
    PipelineBatch,
    PluginContext,
    WriteIntent,
    ZarrArraySpec,
    ZarrGroupSpec,
)
from tests.helpers.storage import make_test_context

pytestmark = pytest.mark.integration


_SOURCE_TOKEN = "dummy"
_T1 = "2024-10-01T00:00:00"
_T2 = "2024-10-01T01:00:00"
_SENTINEL_A = 100.0
_SENTINEL_B = 200.0


class _GenericParityIngestor(GenericZarrIngestor):
    PRODUCT_NAME: ClassVar[str] = "generic_parity_test"
    name = "generic_parity_test"

    def discover_source_files(self, ctx: PluginContext) -> Iterable[Any]:
        return [_SOURCE_TOKEN]

    def get_batch_groups(self, items: Any, ctx: PluginContext) -> list[str]:
        return ["data"]

    def build_dataset(self, group: str, items: list[Any], ctx: PluginContext) -> xr.Dataset | None:
        ts_iso = str(ctx.option("x_ts_iso", _T1))
        sentinel = float(ctx.option("x_sentinel", _SENTINEL_A))
        ds = xr.Dataset(
            {"val": (["timestamp"], np.array([sentinel], dtype="float32"))},
            coords={"timestamp": pd.to_datetime([ts_iso])},
        )
        ds["timestamp"].encoding = {
            "units": "seconds since 1970-01-01",
            "dtype": "int64",
            "calendar": "proleptic_gregorian",
        }
        return ds


def _next_ts_index_from_store(store_uri: str, time_coord_path: str) -> int:
    """Return the existing time-coord array length, or 0 if no array yet.

    Real DirectZarr plugins commonly compute ``ts_index`` from the
    cumulative time-coord length (e.g. "append after existing"). For
    ``write_mode="staged"`` that means reading the temporary store; the
    staged metadata seeding contract is what makes the temp store reflect
    the final target on resume runs. Without seeding this function
    returns 0 and the plugin collides with slot 0 from a previous run.
    """
    try:
        arr = zarr.open_array(store_uri + "/" + time_coord_path, mode="r")
        return int(arr.shape[0])
    except Exception:
        return 0


def _decode_timestamp_array(arr: Any) -> np.ndarray:
    raw = np.asarray(arr[:])
    if raw.dtype.kind == "M":
        return raw.astype("datetime64[s]")
    units = arr.attrs.get("units") if hasattr(arr, "attrs") else None
    if units and "since" in str(units):
        from xarray.coding.times import decode_cf_datetime

        decoded = decode_cf_datetime(raw, units=str(units))
        return np.asarray(decoded).astype("datetime64[s]")
    return raw


def _value_dedup_next_ts_index(store_uri: str, group: str, ts_val: np.datetime64) -> int:
    """Return existing ts_val index if present, else shape[0] (append), else 0."""
    try:
        arr = zarr.open_array(f"{store_uri}/{group}/timestamp", mode="r")
        decoded = _decode_timestamp_array(arr)
        ts_s = np.asarray(ts_val).astype("datetime64[s]")
        for i, v in enumerate(decoded):
            if np.asarray(v).astype("datetime64[s]") == ts_s:
                return i
        return int(arr.shape[0])
    except Exception:
        return 0


class _DirectParityIngestor(DirectZarrIngestor):
    PRODUCT_NAME: ClassVar[str] = "direct_parity_test"
    name = "direct_parity_test"

    def discover_source_files(self, ctx: PluginContext) -> Iterable[Any]:
        return [_SOURCE_TOKEN]

    def zarr_schema(self, ctx: PluginContext) -> list[ZarrGroupSpec]:
        return [
            ZarrGroupSpec(
                group="data",
                arrays=[
                    ZarrArraySpec(
                        name="timestamp",
                        shape=(0,),
                        dtype="datetime64[s]",
                        chunks=(1,),
                    ),
                    ZarrArraySpec(
                        name="val",
                        shape=(0,),
                        dtype="float32",
                        chunks=(1,),
                        fill_value=float("nan"),
                    ),
                ],
            ),
        ]

    def build_write_intents(self, batch: PipelineBatch, ctx: PluginContext) -> list[WriteIntent]:
        write_mode = self.engine_config.write_mode
        store_uri = self.resolve_output_uri(ctx, write_mode=write_mode)
        ts_index = _next_ts_index_from_store(store_uri, "data/timestamp")
        ts_iso = str(ctx.option("x_ts_iso", _T1))
        sentinel = float(ctx.option("x_sentinel", _SENTINEL_A))
        return [
            WriteIntent(
                group="data",
                array="timestamp",
                ts_index=ts_index,
                data=None,
                kind="timestamp",
                timestamp_val=np.datetime64(ts_iso, "s"),
            ),
            WriteIntent(
                group="data",
                array="val",
                ts_index=ts_index,
                data=np.array([sentinel], dtype="float32"),
                kind="1d",
            ),
        ]


class _ValueDedupParityIngestor(DirectZarrIngestor):
    PRODUCT_NAME: ClassVar[str] = "value_dedup_parity_test"
    name = "value_dedup_parity_test"

    def discover_source_files(self, ctx: PluginContext) -> Iterable[Any]:
        return [_SOURCE_TOKEN]

    def zarr_schema(self, ctx: PluginContext) -> list[ZarrGroupSpec]:
        return [
            ZarrGroupSpec(
                group="data",
                arrays=[
                    ZarrArraySpec(
                        name="timestamp",
                        shape=(0,),
                        dtype="datetime64[s]",
                        chunks=(1,),
                    ),
                    ZarrArraySpec(
                        name="val",
                        shape=(0,),
                        dtype="float32",
                        chunks=(1,),
                        fill_value=float("nan"),
                    ),
                ],
            ),
        ]

    def build_write_intents(self, batch: PipelineBatch, ctx: PluginContext) -> list[WriteIntent]:
        write_mode = self.engine_config.write_mode
        store_uri = self.resolve_output_uri(ctx, write_mode=write_mode)
        ts_iso = str(ctx.option("x_ts_iso", _T1))
        ts_val = np.datetime64(ts_iso, "s")
        ts_index = _value_dedup_next_ts_index(store_uri, "data", ts_val)
        sentinel = float(ctx.option("x_sentinel", _SENTINEL_A))
        return [
            WriteIntent(
                group="data",
                array="timestamp",
                ts_index=ts_index,
                data=None,
                kind="timestamp",
                timestamp_val=ts_val,
            ),
            WriteIntent(
                group="data",
                array="val",
                ts_index=ts_index,
                data=np.array([sentinel], dtype="float32"),
                kind="1d",
            ),
        ]


class _ParquetParityIngestor(GenericParquetIngestor):
    PRODUCT_NAME: ClassVar[str] = "parquet_parity_test"
    name = "parquet_parity_test"

    def discover_source_files(self, ctx: PluginContext) -> Iterable[Any]:
        return [_SOURCE_TOKEN]

    def build_dataset(self, group: str, batch: PipelineBatch, ctx: PluginContext) -> Any | None:
        return pd.DataFrame({"val": [float(ctx.option("x_sentinel", _SENTINEL_A))]})


def _run_staged_ingest(
    ingestor_cls: type,
    tmp_path: Path,
    *,
    product_filename: str,
    ts_iso: str,
    sentinel: float,
    resume_existing: bool,
    write_mode: str = "staged",
) -> None:
    source_dir = tmp_path / "source"
    source_dir.mkdir(exist_ok=True)
    marker = source_dir / "input.nc"
    if not marker.exists():
        marker.touch()
    ctx = make_test_context(
        tmp_path,
        source=str(source_dir),
        product=product_filename,
        options={
            "write_mode": write_mode,
            "resume_existing": resume_existing,
            "pipeline_batch_size": 1,
            "pipeline_workers": 1,
            "no_progress": True,
            "cleanup_workspace": True,
            "x_ts_iso": ts_iso,
            "x_sentinel": sentinel,
        },
    )
    ingestor = ingestor_cls()
    result = ingestor.run(ctx)
    assert result.output_path, f"{ingestor_cls.__name__} run produced no output_path: {result!r}"


_TEMPLATE_PARAMS = [
    pytest.param(_GenericParityIngestor, "generic_parity.zarr", id="generic"),
    pytest.param(_DirectParityIngestor, "direct_parity.zarr", id="direct_zarr"),
]


_VALUE_DEDUP_PARAMS = [
    pytest.param("direct", id="value_dedup_direct"),
    pytest.param("staged", id="value_dedup_staged"),
]


@pytest.mark.parametrize(("ingestor_cls", "product_filename"), _TEMPLATE_PARAMS)
def test_staged_mode_preserves_cumulative_state(
    tmp_path: Path,
    ingestor_cls: type,
    product_filename: str,
) -> None:
    """Run A writes T1; Run B writes T2 with ``resume_existing=True``.

    The final Zarr must contain both timestamps in their physical slots
    (``shape==(2,)``, ``timestamp[0]==T1``, ``timestamp[1]==T2``, with
    the paired sentinel values).
    """
    _run_staged_ingest(
        ingestor_cls,
        tmp_path,
        product_filename=product_filename,
        ts_iso=_T1,
        sentinel=_SENTINEL_A,
        resume_existing=False,
    )
    _run_staged_ingest(
        ingestor_cls,
        tmp_path,
        product_filename=product_filename,
        ts_iso=_T2,
        sentinel=_SENTINEL_B,
        resume_existing=True,
    )

    final_zarr = tmp_path / product_filename
    assert final_zarr.exists(), f"Final Zarr not found at {final_zarr}"

    timestamp_arr = zarr.open_array(str(final_zarr / "data" / "timestamp"), mode="r")
    val_arr = zarr.open_array(str(final_zarr / "data" / "val"), mode="r")

    assert timestamp_arr.shape == (2,), (
        f"{ingestor_cls.__name__}: expected timestamp shape (2,), got "
        f"{timestamp_arr.shape}. Run B did not append a new slot."
    )
    assert val_arr.shape == (2,), (
        f"{ingestor_cls.__name__}: expected val shape (2,), got {val_arr.shape}."
    )

    timestamps = _decode_timestamp_array(timestamp_arr)
    values = np.asarray(val_arr[:])
    expected_t1 = np.datetime64(_T1, "s")
    expected_t2 = np.datetime64(_T2, "s")

    assert timestamps[0] == expected_t1, (
        f"{ingestor_cls.__name__}: slot 0 timestamp is {timestamps[0]} "
        f"(expected {expected_t1}). Run B overwrote slot 0 instead of "
        "appending."
    )
    assert timestamps[1] == expected_t2, (
        f"{ingestor_cls.__name__}: slot 1 timestamp is {timestamps[1]} (expected {expected_t2})."
    )
    assert float(values[0]) == pytest.approx(_SENTINEL_A), (
        f"{ingestor_cls.__name__}: slot 0 data is {values[0]} "
        f"(expected {_SENTINEL_A}). Run B overwrote slot 0's data."
    )
    assert float(values[1]) == pytest.approx(_SENTINEL_B), (
        f"{ingestor_cls.__name__}: slot 1 data is {values[1]} (expected {_SENTINEL_B})."
    )


@pytest.mark.parametrize(("ingestor_cls", "product_filename"), _TEMPLATE_PARAMS)
def test_staged_mode_fresh_target_is_noop(
    tmp_path: Path,
    ingestor_cls: type,
    product_filename: str,
) -> None:
    """Fresh staged ingest (no prior target) must not raise a seeding error.

    Seeding is a no-op when the final target does not exist yet.
    This invariant must hold both before and after the runtime hook (T5) lands.
    """
    # Do NOT pre-create the target — fresh ingest scenario
    _run_staged_ingest(
        ingestor_cls,
        tmp_path,
        product_filename=product_filename,
        ts_iso=_T1,
        sentinel=_SENTINEL_A,
        resume_existing=False,
    )

    final_zarr = tmp_path / product_filename
    assert final_zarr.exists(), f"Fresh staged ingest produced no output at {final_zarr}"

    timestamp_arr = zarr.open_array(str(final_zarr / "data" / "timestamp"), mode="r")
    assert timestamp_arr.shape == (1,), (
        f"{ingestor_cls.__name__}: expected shape (1,) after fresh ingest, "
        f"got {timestamp_arr.shape}"
    )


@pytest.mark.parametrize("write_mode", _VALUE_DEDUP_PARAMS)
def test_value_dedup_idempotent_reingest(tmp_path: Path, write_mode: str) -> None:
    """Re-ingesting the same timestamp ≥2 times in both modes must stay shape==(1,).

    staged mode is RED until coord-chunk seeding lands (T6).
    direct mode must always pass.
    """
    _run_staged_ingest(
        _ValueDedupParityIngestor,
        tmp_path,
        product_filename="value_dedup_parity.zarr",
        ts_iso=_T1,
        sentinel=_SENTINEL_A,
        resume_existing=False,
        write_mode=write_mode,
    )
    _run_staged_ingest(
        _ValueDedupParityIngestor,
        tmp_path,
        product_filename="value_dedup_parity.zarr",
        ts_iso=_T1,
        sentinel=_SENTINEL_A,
        resume_existing=True,
        write_mode=write_mode,
    )
    _run_staged_ingest(
        _ValueDedupParityIngestor,
        tmp_path,
        product_filename="value_dedup_parity.zarr",
        ts_iso=_T1,
        sentinel=_SENTINEL_A,
        resume_existing=True,
        write_mode=write_mode,
    )

    final_zarr = tmp_path / "value_dedup_parity.zarr"
    ts_arr = zarr.open_array(str(final_zarr / "data" / "timestamp"), mode="r")
    timestamps = _decode_timestamp_array(ts_arr)

    assert ts_arr.shape == (1,), (
        f"[{write_mode}] Expected shape (1,) after 2 re-ingests, got {ts_arr.shape}. "
        "Value-based dedup failed — timestamp coordinate chunks not seeded to workspace."
    )
    assert len(set(timestamps.astype(str))) == 1, (
        f"[{write_mode}] Expected 1 distinct timestamp, got {set(timestamps.astype(str))}"
    )


@pytest.mark.parametrize("write_mode", _VALUE_DEDUP_PARAMS)
def test_value_dedup_distinct_append(tmp_path: Path, write_mode: str) -> None:
    """Appending a DIFFERENT timestamp must still produce shape==(2,) in both modes."""
    _run_staged_ingest(
        ingestor_cls=_ValueDedupParityIngestor,
        tmp_path=tmp_path,
        product_filename="value_dedup_parity.zarr",
        ts_iso=_T1,
        sentinel=_SENTINEL_A,
        write_mode=write_mode,
        resume_existing=False,
    )
    _run_staged_ingest(
        ingestor_cls=_ValueDedupParityIngestor,
        tmp_path=tmp_path,
        product_filename="value_dedup_parity.zarr",
        ts_iso=_T2,
        sentinel=_SENTINEL_B,
        write_mode=write_mode,
        resume_existing=True,
    )

    final_zarr = tmp_path / "value_dedup_parity.zarr"
    ts_arr = zarr.open_array(str(final_zarr / "data" / "timestamp"), mode="r")
    timestamps = _decode_timestamp_array(ts_arr)

    assert ts_arr.shape == (2,), (
        f"[{write_mode}] Expected shape (2,) after distinct-timestamp append, got {ts_arr.shape}."
    )
    assert timestamps[0] == np.datetime64(_T1, "s"), (
        f"[{write_mode}] Slot 0 should be T1 ({_T1}), got {timestamps[0]}"
    )
    assert timestamps[1] == np.datetime64(_T2, "s"), (
        f"[{write_mode}] Slot 1 should be T2 ({_T2}), got {timestamps[1]}"
    )


def test_staged_hook_does_not_fire_for_non_zarr_outputs(tmp_path: Path) -> None:
    source_dir = tmp_path / "source"
    source_dir.mkdir(exist_ok=True)
    (source_dir / "input.csv").touch()
    ctx = make_test_context(
        tmp_path,
        source=str(source_dir),
        product="parquet_parity.parquet",
        format="parquet",
        options={
            "write_mode": "staged",
            "pipeline_batch_size": 1,
            "pipeline_workers": 1,
            "no_progress": True,
            "cleanup_workspace": True,
        },
    )

    result = _ParquetParityIngestor().run(ctx)

    assert result.output_path
    final_dir = tmp_path / "parquet_parity.parquet"
    parquet_files = sorted(final_dir.glob("*.parquet"))
    assert len(parquet_files) == 1

    table = pq.read_table(parquet_files[0])
    assert table.column_names == ["val"]
    assert table.to_pydict() == {"val": [_SENTINEL_A]}


@pytest.mark.parametrize(("ingestor_cls", "product_filename"), _TEMPLATE_PARAMS)
def test_staged_mode_strict_failure_surface_preserved(
    tmp_path: Path,
    ingestor_cls: type,
    product_filename: str,
) -> None:
    """Strict-mode seeding I/O failure must surface as a hard pipeline failure.

    Injection surgery
    -----------------
    A naive chmod of every ``zarr.json`` under the target trips
    ``existing_cube_check.py:142`` (``_read_json`` opens each array-level
    ``zarr.json`` to verify time-dim compatibility) BEFORE seeding fires,
    producing a raw ``PermissionError`` from the batch planner rather than a
    ``StagedMetadataError`` from the seeding helper. To isolate the seeding
    path this test chmods ONLY the GROUP-LEVEL ``zarr.json``
    (``{target}/{group}/zarr.json``):

    * ``existing_cube_check.py:125`` explicitly skips the group-level entry
      (it filters out ``uri.path == group_uri.join("zarr.json").path``) and
      only opens array-level metadata, so the dim-compatibility check is
      unaffected.
    * ``staged_metadata.py:107`` includes every ``*zarr.json`` in the
      seeding loop, so the chmod denial fires on the group-level file inside
      the inner ``try`` at ``staged_metadata.py:120-123``.

    Audited failure surface (read top-to-bottom)
    --------------------------------------------
    1. ``staged_metadata.py:122-123`` opens the source ``zarr.json`` via
       ``fs.open(src_uri, "rb")``. The chmod 0o000 makes the read raise
       ``PermissionError``; the inner ``except`` at
       ``staged_metadata.py:125-129`` wraps it as ``StagedMetadataError``
       because ``strict=True``.
    2. ``batch_runner.py:39`` computes ``strict = bool(resume_existing and
       not force_reingest)`` — Run B (resume_existing=True,
       force_reingest=False) takes the strict path. ``batch_runner.py:54-55``
       re-raises ``StagedMetadataError`` unchanged.
    3. ``generic.py:266-273`` catches the ``Exception`` from
       ``seed_staged_metadata_for_batch`` and produces
       ``PipelineResult(success=False, error=str(exc))``. The error string
       carries the original ``StagedMetadataError`` text, which begins with
       ``"Failed to seed staged metadata"`` (see ``staged_metadata.py:128``).
    4. ``engine.py:606-613`` sees the failed batch in ``state.results`` and
       raises ``PipelineFailedBatchesError`` whose message embeds the
       per-batch ``res.error`` string.

    The observable surface of ``ingestor.run(ctx)`` is therefore
    ``PipelineFailedBatchesError`` with the substring ``"staged metadata"``
    in its message. This test pins that surface so any future change which
    downgrades strict mode to a warning, swallows the exception in the
    batch-runner shim, or silently retries the run becomes a test failure.

    Why not semantic JSON corruption
    --------------------------------
    ``staged_metadata.py:103-123`` does a raw-byte copy without JSON
    parsing, so writing ``"{}"`` over a valid ``zarr.json`` is silently
    propagated into the temp store and the strict-mode failure never fires.
    Only a real I/O denial trips the read inside the inner ``try`` and
    exercises the strict-mode wrapping.

    DirectZarr coverage
    -------------------
    DirectZarr uses the runtime-level staged seeding hook before intent
    construction, so the same strict-mode failure surface must now apply to
    both Zarr templates.
    """
    import os

    if hasattr(os, "geteuid") and os.geteuid() == 0:
        pytest.skip(
            "chmod(0o000) I/O denial is ineffective for the root user "
            "(CI runners run as root), so the strict-mode seeding failure "
            "cannot be injected this way."
        )

    from firecube.ingestor.runtime.engine import PipelineFailedBatchesError

    # Run A populates the final target with valid Zarr metadata that Run B
    # will (attempt to) seed from. This step must succeed for both templates.
    _run_staged_ingest(
        ingestor_cls,
        tmp_path,
        product_filename=product_filename,
        ts_iso=_T1,
        sentinel=_SENTINEL_A,
        resume_existing=False,
    )

    final_zarr = tmp_path / product_filename
    assert final_zarr.exists(), f"Run A did not produce a target at {final_zarr}"

    # Inject a real I/O failure ONLY on the group-level zarr.json so the
    # dim-compat check (which skips this file) still passes while the
    # seeding helper (which reads it) is denied.
    group_metadata = final_zarr / "data" / "zarr.json"
    assert group_metadata.exists(), (
        f"Run A did not produce group-level metadata at {group_metadata}; "
        "cannot inject the strict-mode seeding failure surface."
    )

    original_mode = group_metadata.stat().st_mode
    try:
        group_metadata.chmod(0o000)

        # Run B requests strict-mode resume; the chmod denial must surface as
        # a PipelineFailedBatchesError carrying the StagedMetadataError text.
        with pytest.raises(PipelineFailedBatchesError) as exc_info:
            _run_staged_ingest(
                ingestor_cls,
                tmp_path,
                product_filename=product_filename,
                ts_iso=_T2,
                sentinel=_SENTINEL_B,
                resume_existing=True,
            )

        err_str = str(exc_info.value).lower()
        # Both StagedMetadataError message templates contain "staged metadata"
        # (staged_metadata.py:78 group-level wrap; staged_metadata.py:128
        # per-file wrap). Matching on this substring proves the inner
        # StagedMetadataError text was carried through generic.py:272 and
        # engine.py:608 without being swallowed.
        assert "staged metadata" in err_str, (
            f"Expected 'staged metadata' substring in failure message, got: {exc_info.value!r}"
        )

    finally:
        # Restore the file mode so pytest's tmp_path cleanup can delete the
        # final target; suppress FileNotFoundError if the file vanished
        # between chmod and restore.
        with contextlib.suppress(FileNotFoundError):
            group_metadata.chmod(original_mode)
