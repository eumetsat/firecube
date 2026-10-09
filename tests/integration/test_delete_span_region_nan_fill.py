# Copyright 2025-2026 EUMETSAT
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Deleting a span: append-store region fill, chunk-key deletion, and refusals.

The default append cube is shaped like one written by 0.1.7 GenericZarr: a
time-indexed data variable, a CF-encoded ``time_bnds`` and static
``lat_bnds``/``lon_bnds`` that the span record lists (static one first).
Deleting a span must fill every time-indexed array with a value that reads back
as missing, leave static arrays untouched and say so, and never delete shared
chunks. A span the engine cannot delete completely is refused with zero bytes
mutated. A raw store without a state array keeps chunk-key deletion, which
must skip static arrays and refuse spans it cannot resolve.
"""

from __future__ import annotations

import hashlib
from collections.abc import Sequence
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Literal, cast

import numpy as np
import pytest
import xarray as xr
import zarr
from click.testing import CliRunner, Result

from firecube.cli.main import cli
from firecube.core.controlplane import ChunkManager, SpanCoverage
from tests.helpers.storage import make_test_binding

pytestmark = pytest.mark.integration


PRODUCT = "product.zarr"
GROUP = "default"
ARRAY_NAME = "precipitation"
ARRAY_PATH = f"{GROUP}/{ARRAY_NAME}"
STATE_NAME = "firecube_timestamp_state"
STATE_PATH = f"{GROUP}/{STATE_NAME}"
SIBLING_STATE_PATH = f"state/{STATE_NAME}"
TIME_DIM = "timestamp"
TIME_BNDS = "time_bnds"
LAT_BNDS = "lat_bnds"
LON_BNDS = "lon_bnds"
UNKNOWN_DIMS = "unknown_dims"
LEGACY_DIMS = "legacy_dims"
SCALAR = "scalar"
TIME_UNITS = "seconds since 2000-01-01"
N_TIMES = 60
# 0.1.7 listed static data variables in the span record, and could list one first.
APPEND_SPAN_ARRAYS = [LAT_BNDS, ARRAY_NAME, TIME_BNDS, LON_BNDS]

DEFAULT_RUNS = (("run-a", 0, 29), ("run-b", 30, 59))
# A deleted run with a predecessor and a successor: guards the span's start and end.
INTERIOR_RUNS = (("run-a", 0, 19), ("run-mid", 20, 39), ("run-c", 40, 59))

StateLayout = Literal["time-1d", "time-2d", "other-axis-1d"]


def _manager(tmp_path: Path) -> ChunkManager:
    return ChunkManager(binding=make_test_binding(tmp_path, product=PRODUCT), workspace=tmp_path)


def _store_root(tmp_path: Path) -> Path:
    return tmp_path / PRODUCT


def _append_chunk_key(tmp_path: Path) -> Path:
    return _store_root(tmp_path) / GROUP / ARRAY_NAME / "c" / "0" / "0"


def _group_dir(tmp_path: Path) -> Path:
    return _store_root(tmp_path) / GROUP


def _open_group(tmp_path: Path, mode: Literal["r", "r+"] = "r") -> Any:
    return cast(
        Any,
        zarr.open_group(store=str(_group_dir(tmp_path)), mode=mode, zarr_format=3),
    )


def _hash_run_a_slots(tmp_path: Path) -> str:
    group = _open_group(tmp_path)
    return hashlib.sha256(group[ARRAY_NAME][0:30].tobytes()).hexdigest()


def _hash_files(root: Path) -> dict[str, str]:
    """sha256 of every file under *root*, keyed by relative path."""
    return {
        str(path.relative_to(root)): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in sorted(root.rglob("*"))
        if path.is_file()
    }


def _precip_values() -> np.ndarray:
    """Run A (slots 0-29) holds 1.0, run B (slots 30-59) holds 2.0."""
    values = np.full((N_TIMES, 2), 1.0, dtype=np.float32)
    values[30:] = 2.0
    return values


def _time_bnds_values(length: int, dtype: str) -> np.ndarray:
    """Non-zero CF bounds: 0 is the Zarr fill value and would read back as NaT."""
    starts = np.arange(length, dtype=np.int64) * 3600 + 3600
    return np.stack([starts, starts + 1800], axis=1).astype(dtype)


def _span_key(tmp_path: Path, run_id: str) -> str:
    manager = _manager(tmp_path)
    try:
        spans = manager.list_chunks(product=PRODUCT, chunk_type="span")
    finally:
        manager.close()
    keys = [span.key for span in spans if (span.meta or {}).get("run_id") == run_id]
    assert len(keys) == 1, keys
    return keys[0]


def _span_history(tmp_path: Path) -> list[tuple[str, str | None, dict[str, Any]]]:
    manager = _manager(tmp_path)
    try:
        spans = manager.list_chunks(product=PRODUCT, chunk_type="span", include_replaced=True)
    finally:
        manager.close()
    return [(span.key, span.status, dict(span.meta or {})) for span in spans]


def _delete_spans(tmp_path: Path, run_id: str) -> dict[str, Any]:
    """Run the engine call ``chunks delete-span --force --yes-i-really-mean-it`` makes."""
    manager = _manager(tmp_path)
    try:
        spans = [
            span
            for span in manager.list_chunks(product=PRODUCT, chunk_type="span")
            if (span.meta or {}).get("run_id") == run_id
        ]
        assert len(spans) == 1
        return manager.delete_spans(spans, force=True, yes_i_really_mean_it=True)
    finally:
        manager.close()


def _delete_run_with_cli(
    tmp_path: Path,
    run_id: str,
    *,
    force: bool = True,
    include_replaced: bool = False,
) -> Result:
    args = [
        "chunks",
        "--workspace",
        str(tmp_path),
        "delete-span",
        "--product-name",
        _store_root(tmp_path).as_uri(),
        "--run-id",
        run_id,
        "--yes-i-really-mean-it",
    ]
    if force:
        args.append("--force")
    if include_replaced:
        args.append("--include-replaced")
    result = CliRunner().invoke(cli, args)
    assert result.exit_code == 0, result.output
    assert "Errors:" not in result.output, result.output
    return result


def _record_span(
    manager: ChunkManager,
    *,
    run_id: str,
    batch_id: str,
    start: int,
    end: int,
    append_store: bool = True,
    state_array_path: str | None = None,
    arrays: Sequence[str] | None = None,
    state_deleted_value: int = 2,
    aligned: bool = False,
) -> None:
    """Record a span; *arrays* are names inside the group, default per store kind."""
    if arrays is None:
        arrays = APPEND_SPAN_ARRAYS if append_store else [ARRAY_NAME]
    output_path = str(_store_root(manager.workspace))
    base_time = datetime(2024, 1, 1)
    time_min = (base_time + timedelta(days=start)).isoformat()
    time_max = (base_time + timedelta(days=end)).isoformat()
    coverage = SpanCoverage(
        group=GROUP,
        arrays=[f"{GROUP}/{name}" for name in arrays],
        time_index_ranges=[[start, end]],
        aligned=aligned,
        state_array=state_array_path or (STATE_PATH if append_store else None),
        state_deleted_value=state_deleted_value,
        time_min=time_min,
        time_max=time_max,
        time_dim_name=TIME_DIM,
    )

    manager.record_run_started(
        product=PRODUCT,
        run_id=run_id,
        output_path=output_path,
        output_format="zarr",
        size=0,
        meta={"plugin": "test"},
    )
    manager.record_span(
        product=PRODUCT,
        run_id=run_id,
        batch_id=batch_id,
        group=GROUP,
        status="active",
        coverage=coverage,
        meta={
            "plugin": "test",
            "group": GROUP,
            "time_min": time_min,
            "time_max": time_max,
        },
    )
    manager.record_run_terminal(
        product=PRODUCT,
        run_id=run_id,
        output_path=output_path,
        output_format="zarr",
        size=1,
        meta={"plugin": "test"},
        status="complete",
    )


def _add_static_bounds(group: Any) -> None:
    """Static ``lat_bnds (lat, nv)`` / ``lon_bnds (lon, nv)`` with no time dimension."""
    for name, dim, values in (
        (LAT_BNDS, "lat", [[10.0, 10.5], [10.5, 11.0]]),
        (LON_BNDS, "lon", [[20.0, 20.5], [20.5, 21.0]]),
    ):
        static = group.create_array(
            name,
            shape=(2, 2),
            chunks=(2, 2),
            dtype="f8",
            fill_value=np.nan,
            dimension_names=(dim, "nv"),
        )
        static[:] = np.asarray(values, dtype=np.float64)


def _add_arrays_without_dimension_names(group: Any, *, length: int, chunk: int) -> None:
    """Add arrays that carry no zarr v3 ``dimension_names``.

    ``unknown_dims`` has no dimension metadata at all, so whether it is
    time-indexed is unknowable; ``legacy_dims`` declares its time axis only
    through the ``_ARRAY_DIMENSIONS`` attr; ``scalar`` is 0-d.
    """
    unknown = group.create_array(
        UNKNOWN_DIMS, shape=(length,), chunks=(chunk,), dtype="f8", fill_value=np.nan
    )
    unknown[:] = np.arange(length, dtype=np.float64) + 10.0
    legacy = group.create_array(
        LEGACY_DIMS, shape=(length,), chunks=(chunk,), dtype="f8", fill_value=np.nan
    )
    legacy.attrs["_ARRAY_DIMENSIONS"] = [TIME_DIM]
    legacy[:] = np.arange(length, dtype=np.float64) + 0.5
    scalar = group.create_array(SCALAR, shape=(), dtype="f8", fill_value=np.nan)
    scalar[...] = 7.0


def _seed_append_store(
    tmp_path: Path,
    *,
    time_bnds_dtype: str = "int64",
    time_bnds_length: int = N_TIMES,
    state_layout: StateLayout = "time-1d",
    extra_arrays: bool = False,
    span_arrays: Sequence[str] | None = None,
    state_deleted_value: int = 2,
    runs: Sequence[tuple[str, int, int]] = DEFAULT_RUNS,
) -> None:
    """0.1.7-shaped append cube; *runs* default to ``run-a`` (0-29) and ``run-b`` (30-59).

    Besides the time-indexed ``precipitation`` it holds a CF-encoded
    ``time_bnds (timestamp, nv)`` and static ``lat_bnds``/``lon_bnds``. The
    keyword arguments bend that cube for the refusal cases: bounds dtype and
    length, state array layout, arrays without dimension names, and what the
    span records list.
    """
    root = zarr.open_group(store=str(_store_root(tmp_path)), mode="w", zarr_format=3)
    group = root.require_group(GROUP)
    data = group.create_array(
        ARRAY_NAME,
        shape=(N_TIMES, 2),
        chunks=(365, 2),
        dtype="f4",
        fill_value=np.nan,
        dimension_names=(TIME_DIM, "x"),
        overwrite=True,
    )
    data[:] = _precip_values()

    timestamp = group.create_array(
        TIME_DIM,
        shape=(N_TIMES,),
        chunks=(365,),
        dtype="i8",
        fill_value=0,
        dimension_names=(TIME_DIM,),
        overwrite=True,
    )
    timestamp[:] = np.arange(N_TIMES, dtype=np.int64)

    time_bnds = group.create_array(
        TIME_BNDS,
        shape=(time_bnds_length, 2),
        chunks=(365, 2),
        dtype=time_bnds_dtype,
        fill_value=0,
        dimension_names=(TIME_DIM, "nv"),
        overwrite=True,
    )
    time_bnds.attrs.update({"units": TIME_UNITS, "calendar": "standard"})
    time_bnds[:] = _time_bnds_values(time_bnds_length, time_bnds_dtype)

    _add_static_bounds(group)
    if extra_arrays:
        _add_arrays_without_dimension_names(group, length=N_TIMES, chunk=365)

    state_shape: tuple[int, ...]
    state_dims: tuple[str, ...]
    if state_layout == "time-1d":
        state_shape, state_dims = (N_TIMES,), (TIME_DIM,)
    elif state_layout == "time-2d":
        state_shape, state_dims = (N_TIMES, 2), (TIME_DIM, "x")
    else:
        state_shape, state_dims = (N_TIMES,), ("other",)
    state = group.create_array(
        STATE_NAME,
        shape=state_shape,
        chunks=(365, *state_shape[1:]),
        dtype="u1",
        fill_value=0,
        dimension_names=state_dims,
        overwrite=True,
    )
    state[:] = np.uint8(1)

    manager = _manager(tmp_path)
    try:
        for run_id, start, end in runs:
            _record_span(
                manager,
                run_id=run_id,
                batch_id=f"batch-{run_id}",
                start=start,
                end=end,
                arrays=span_arrays,
                state_deleted_value=state_deleted_value,
            )
    finally:
        manager.close()


def _create_raw_data_array(
    tmp_path: Path, *, static_bounds: bool = False, extra_arrays: bool = False
) -> Any:
    """Create the raw (non-append) store and return its root group."""
    root = zarr.open_group(store=str(_store_root(tmp_path)), mode="w", zarr_format=3)
    group = root.require_group(GROUP)
    data = group.create_array(
        ARRAY_NAME,
        shape=(12, 2),
        chunks=(4, 2),
        dtype="f4",
        fill_value=np.nan,
        dimension_names=(TIME_DIM, "x"),
        overwrite=True,
    )
    data[:] = np.ones((12, 2), dtype=np.float32)
    if static_bounds:
        _add_static_bounds(group)
    if extra_arrays:
        _add_arrays_without_dimension_names(group, length=12, chunk=4)
    return root


def _seed_raw_store(
    tmp_path: Path,
    *,
    static_bounds: bool = False,
    extra_arrays: bool = False,
    span_arrays: Sequence[str] | None = None,
    aligned: bool = False,
    start: int = 0,
    end: int = 3,
) -> None:
    """Raw store (3 time chunks of 4 slots) with one span ``run-raw`` over slots start-end."""
    _create_raw_data_array(tmp_path, static_bounds=static_bounds, extra_arrays=extra_arrays)

    manager = _manager(tmp_path)
    try:
        _record_span(
            manager,
            run_id="run-raw",
            batch_id="batch-raw",
            start=start,
            end=end,
            append_store=False,
            arrays=span_arrays,
            aligned=aligned,
        )
    finally:
        manager.close()


def _seed_raw_store_with_sibling_state(tmp_path: Path) -> None:
    """Raw store whose recorded state array lives outside the data group.

    ``_zarr_group_has_array`` does not find the state array in the data
    group, so ``delete_spans`` takes the chunk-key path; the misaligned
    range then reaches the state-range expansion, which reads the sibling
    state array's chunk grid.
    """
    root = _create_raw_data_array(tmp_path)
    state = root.require_group("state").create_array(
        STATE_NAME,
        shape=(8,),
        chunks=(4,),
        dtype="u1",
        fill_value=0,
        dimension_names=("timestamp",),
        overwrite=True,
    )
    state[:] = np.uint8(1)

    manager = _manager(tmp_path)
    try:
        _record_span(
            manager,
            run_id="run-sibling",
            batch_id="batch-sibling",
            start=0,
            end=2,
            append_store=False,
            state_array_path=SIBLING_STATE_PATH,
        )
    finally:
        manager.close()


def _replaced_span_meta(tmp_path: Path, run_id: str) -> dict:
    manager = _manager(tmp_path)
    try:
        spans = manager.list_chunks(product=PRODUCT, chunk_type="span", include_replaced=True)
    finally:
        manager.close()

    for span in spans:
        meta = span.meta if isinstance(span.meta, dict) else {}
        if meta.get("run_id") == run_id and span.status == "replaced":
            return meta
    raise AssertionError(f"replaced span for run_id={run_id!r} not found")


def test_delete_span_on_append_store_no_collateral(tmp_path: Path) -> None:
    """delete-span on append store does not touch neighbouring chunk slots."""
    _seed_append_store(tmp_path)
    hash_before = _hash_run_a_slots(tmp_path)
    chunk_key = _append_chunk_key(tmp_path)
    assert chunk_key.exists()

    _delete_run_with_cli(tmp_path, "run-b")

    assert _hash_run_a_slots(tmp_path) == hash_before
    assert chunk_key.exists(), "append-store deletion must not remove shared physical chunk keys"


def test_delete_span_sets_state_to_2(tmp_path: Path) -> None:
    """state[30:60] == [2]*30 after delete-span run_B."""
    _seed_append_store(tmp_path)

    _delete_run_with_cli(tmp_path, "run-b")

    group = _open_group(tmp_path)
    assert group[STATE_NAME][30:60].tolist() == [2] * 30
    assert group[STATE_NAME][0:30].tolist() == [1] * 30


def test_delete_span_data_is_nan_or_fill_value(tmp_path: Path) -> None:
    """data[30:60] is NaN/fill after deletion; CF bounds read as NaT; statics untouched."""
    _seed_append_store(tmp_path)
    span_key = _span_key(tmp_path, "run-b")
    group_dir = _group_dir(tmp_path)
    lat_before = _hash_files(group_dir / LAT_BNDS)
    lon_before = _hash_files(group_dir / LON_BNDS)
    time_bnds_attrs_before = dict(_open_group(tmp_path)[TIME_BNDS].attrs)

    result = _delete_run_with_cli(tmp_path, "run-b")

    assert "NaN-filled 1 spans" in result.output
    assert "Warnings: 1" in result.output
    assert (
        f"Span {span_key}: skipped 2 arrays without time dimension '{TIME_DIM}': "
        f"{LAT_BNDS}, {LON_BNDS}"
    ) in result.output

    group = _open_group(tmp_path)
    deleted = np.asarray(group[ARRAY_NAME][30:60])
    neighbours = np.asarray(group[ARRAY_NAME][0:30])
    assert np.isnan(deleted).all()
    assert np.array_equal(neighbours, np.full((30, 2), 1.0, dtype=np.float32))

    dataset = xr.open_zarr(str(_store_root(tmp_path)), group=GROUP, consolidated=False)
    try:
        decoded_bnds = np.asarray(dataset[TIME_BNDS].values)
    finally:
        dataset.close()
    assert decoded_bnds.dtype.kind == "M"
    assert np.isnat(decoded_bnds[30:]).all()
    assert not np.isnat(decoded_bnds[:30]).any()
    epoch = np.datetime64("2000-01-01T00:00:00")
    raw = _time_bnds_values(N_TIMES, "int64")
    assert (decoded_bnds[:30] == epoch + raw[:30].astype("timedelta64[s]")).all()

    assert _hash_files(group_dir / LAT_BNDS) == lat_before
    assert _hash_files(group_dir / LON_BNDS) == lon_before
    assert dict(group[TIME_BNDS].attrs) == time_bnds_attrs_before


def test_delete_span_recorded_in_wal_as_region_nan_fill(tmp_path: Path) -> None:
    """WAL replacement event has meta.write_strategy == region_nan_fill."""
    _seed_append_store(tmp_path)

    _delete_run_with_cli(tmp_path, "run-b")

    assert _replaced_span_meta(tmp_path, "run-b")["write_strategy"] == "region_nan_fill"


def test_delete_span_on_non_append_store_still_uses_chunk_delete(tmp_path: Path) -> None:
    """Verified-correct: raw zarr without state array keeps chunk-key deletion."""
    _seed_raw_store(tmp_path)
    chunk_key = _append_chunk_key(tmp_path)
    assert chunk_key.exists()

    result = _delete_run_with_cli(tmp_path, "run-raw")

    assert "Deleted 1 chunk keys" in result.output
    assert not chunk_key.exists()
    assert "write_strategy" not in _replaced_span_meta(tmp_path, "run-raw")


def test_chunk_key_delete_skips_static_array_with_warning(tmp_path: Path) -> None:
    """Without a state array, chunk-key deletion removes only time-indexed chunks."""
    _seed_raw_store(
        tmp_path,
        static_bounds=True,
        span_arrays=[ARRAY_NAME, LAT_BNDS],
        aligned=True,
        start=4,
        end=7,
    )
    span_key = _span_key(tmp_path, "run-raw")
    precip_chunks = _group_dir(tmp_path) / ARRAY_NAME / "c"
    assert sorted(p.name for p in precip_chunks.iterdir()) == ["0", "1", "2"]
    lat_before = _hash_files(_group_dir(tmp_path) / LAT_BNDS)
    kept_before = {chunk: _hash_files(precip_chunks / chunk) for chunk in ("0", "2")}

    result = _delete_spans(tmp_path, "run-raw")

    assert result["errors"] == []
    assert result["warnings"] == [
        f"Span {span_key}: skipped 1 arrays without time dimension '{TIME_DIM}': {GROUP}/{LAT_BNDS}"
    ]
    assert result["deleted_keys"] == 1
    assert result["deleted_spans"] == 1
    assert not (precip_chunks / "1" / "0").exists()
    for chunk, hashes in kept_before.items():
        assert (precip_chunks / chunk / "0").exists()
        assert _hash_files(precip_chunks / chunk) == hashes
    assert _hash_files(_group_dir(tmp_path) / LAT_BNDS) == lat_before
    [(key, status, meta)] = _span_history(tmp_path)
    assert (key, status) == (span_key, "replaced")
    assert "write_strategy" not in meta


def test_chunk_key_delete_of_only_static_arrays_is_refused(tmp_path: Path) -> None:
    """Failure mode: chunk-key deletion aborts loudly on a span with no time-indexed array.

    Every array lacking the time dim is also what a wrong time-dim name looks
    like, so this keeps the resolver's loud abort and its remediation hint.
    """
    _seed_raw_store(tmp_path, static_bounds=True, span_arrays=[LAT_BNDS, LON_BNDS], aligned=True)
    span_key = _span_key(tmp_path, "run-raw")
    before = _hash_files(_group_dir(tmp_path))

    with pytest.raises(ValueError) as excinfo:
        _delete_spans(tmp_path, "run-raw")

    message = str(excinfo.value)
    assert span_key in message
    assert f"no array in this span carries time dimension '{TIME_DIM}'" in message
    assert "--time-dim" in message
    assert _hash_files(_group_dir(tmp_path)) == before
    assert [(key, status) for key, status, _meta in _span_history(tmp_path)] == [
        (span_key, "active")
    ]


def test_chunk_key_delete_refuses_array_with_unknown_dimensions(tmp_path: Path) -> None:
    """Failure mode: chunk-key deletion aborts before removing any chunk of the span."""
    _seed_raw_store(
        tmp_path, extra_arrays=True, span_arrays=[ARRAY_NAME, UNKNOWN_DIMS], aligned=True
    )
    span_key = _span_key(tmp_path, "run-raw")
    before = _hash_files(_group_dir(tmp_path))

    with pytest.raises(ValueError) as excinfo:
        _delete_spans(tmp_path, "run-raw")

    assert f"{GROUP}/{UNKNOWN_DIMS}" in str(excinfo.value)
    assert _hash_files(_group_dir(tmp_path)) == before
    assert [(key, status) for key, status, _meta in _span_history(tmp_path)] == [
        (span_key, "active")
    ]


def test_chunk_key_delete_removes_array_dimensioned_by_array_dimensions_attr(
    tmp_path: Path,
) -> None:
    """Chunk-key deletion honours ``_ARRAY_DIMENSIONS`` and skips a 0-d scalar with a warning."""
    _seed_raw_store(
        tmp_path,
        extra_arrays=True,
        span_arrays=[ARRAY_NAME, LEGACY_DIMS, SCALAR],
        aligned=True,
        start=4,
        end=7,
    )
    span_key = _span_key(tmp_path, "run-raw")
    legacy_chunks = _group_dir(tmp_path) / LEGACY_DIMS / "c"
    precip_chunks = _group_dir(tmp_path) / ARRAY_NAME / "c"
    kept_before = {
        (name, chunk): _hash_files(root / chunk)
        for name, root in ((LEGACY_DIMS, legacy_chunks), (ARRAY_NAME, precip_chunks))
        for chunk in ("0", "2")
    }
    scalar_before = _hash_files(_group_dir(tmp_path) / SCALAR)

    result = _delete_spans(tmp_path, "run-raw")

    assert result["errors"] == []
    assert result["warnings"] == [
        f"Span {span_key}: skipped 1 arrays without time dimension '{TIME_DIM}': {GROUP}/{SCALAR}"
    ]
    assert result["deleted_keys"] == 2
    assert not (legacy_chunks / "1").exists()
    assert not (precip_chunks / "1" / "0").exists()
    for (name, chunk), hashes in kept_before.items():
        root = legacy_chunks if name == LEGACY_DIMS else precip_chunks
        assert (root / chunk).exists()
        assert _hash_files(root / chunk) == hashes
    assert _hash_files(_group_dir(tmp_path) / SCALAR) == scalar_before


def test_delete_span_reports_state_expansion_failure_and_keeps_span_active(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Failure mode: a state-grid read error is reported and the span is not replaced."""
    from firecube.core.zarr import validation as validation_mod

    _seed_raw_store_with_sibling_state(tmp_path)
    real_read_chunk_grid = validation_mod.read_chunk_grid

    def _raise_for_state(store_uri: Any, array_path: Any, *args: Any, **kwargs: Any) -> Any:
        if str(array_path) == SIBLING_STATE_PATH:
            raise FileNotFoundError(f"simulated missing zarr.json for {array_path}")
        return real_read_chunk_grid(store_uri, array_path, *args, **kwargs)

    monkeypatch.setattr(validation_mod, "read_chunk_grid", _raise_for_state)

    manager = _manager(tmp_path)
    try:
        spans = [
            span
            for span in manager.list_chunks(product=PRODUCT, chunk_type="span")
            if (span.meta or {}).get("run_id") == "run-sibling"
        ]
        assert len(spans) == 1
        span_key = spans[0].key

        result = manager.delete_spans(spans, force=True, yes_i_really_mean_it=True)

        assert result["deleted_spans"] == 0
        assert len(result["errors"]) == 1, result["errors"]
        assert span_key in result["errors"][0]
        assert SIBLING_STATE_PATH in result["errors"][0]
        assert "simulated missing zarr.json" in result["errors"][0]

        history = manager.list_chunks(product=PRODUCT, chunk_type="span", include_replaced=True)
        assert [(span.key, span.status) for span in history] == [(span_key, "active")]
    finally:
        manager.close()


def test_include_replaced_lists_each_span_once_and_refills_only_with_force(
    tmp_path: Path,
) -> None:
    """a replaced span appears once in history and is refilled only with --force."""
    _seed_append_store(tmp_path)
    _delete_run_with_cli(tmp_path, "run-b")

    manager = _manager(tmp_path)
    try:
        history = manager.list_chunks(product=PRODUCT, chunk_type="span", include_replaced=True)
    finally:
        manager.close()
    keys = [span.key for span in history]
    assert len(keys) == len(set(keys)) == 2
    assert {(span.meta or {})["run_id"]: span.status for span in history} == {
        "run-a": "active",
        "run-b": "replaced",
    }

    rewritten = np.full((30, 2), 5.0, dtype=np.float32)
    group = _open_group(tmp_path, mode="r+")
    group[ARRAY_NAME][30:60] = rewritten
    group[STATE_NAME][30:60] = np.uint8(1)

    skipped = _delete_run_with_cli(tmp_path, "run-b", force=False, include_replaced=True)
    assert "Warnings: 1" in skipped.output
    assert "already replaced by an in-place fill" in skipped.output
    assert "NaN-filled" not in skipped.output
    group = _open_group(tmp_path)
    assert np.array_equal(np.asarray(group[ARRAY_NAME][30:60]), rewritten)
    assert group[STATE_NAME][30:60].tolist() == [1] * 30

    refilled = _delete_run_with_cli(tmp_path, "run-b", force=True, include_replaced=True)
    assert "NaN-filled 1 spans" in refilled.output
    group = _open_group(tmp_path)
    assert np.isnan(np.asarray(group[ARRAY_NAME][30:60])).all()
    assert group[STATE_NAME][30:60].tolist() == [2] * 30
    assert np.array_equal(np.asarray(group[ARRAY_NAME][0:30]), np.full((30, 2), 1.0, np.float32))

    manager = _manager(tmp_path)
    try:
        history = manager.list_chunks(product=PRODUCT, chunk_type="span", include_replaced=True)
    finally:
        manager.close()
    assert sorted(span.key for span in history) == sorted(keys)


@pytest.mark.parametrize(
    ("runs", "target", "lo", "hi"),
    [
        pytest.param(DEFAULT_RUNS, "run-b", 30, 60, id="tail-run"),
        pytest.param(INTERIOR_RUNS, "run-mid", 20, 40, id="interior-run"),
    ],
)
def test_region_fill_fills_array_dimensioned_by_array_dimensions_attr(
    tmp_path: Path, runs: tuple[tuple[str, int, int], ...], target: str, lo: int, hi: int
) -> None:
    """An ``_ARRAY_DIMENSIONS``-declared time array is filled only inside the span; 0-d skipped."""
    _seed_append_store(
        tmp_path, extra_arrays=True, span_arrays=[ARRAY_NAME, LEGACY_DIMS, SCALAR], runs=runs
    )
    span_key = _span_key(tmp_path, target)

    result = _delete_spans(tmp_path, target)

    assert result["errors"] == []
    assert result["region_filled_spans"] == 1
    assert result["warnings"] == [
        f"Span {span_key}: skipped 1 arrays without time dimension '{TIME_DIM}': {SCALAR}"
    ]
    group = _open_group(tmp_path)
    original = np.arange(N_TIMES, dtype=np.float64) + 0.5
    legacy = np.asarray(group[LEGACY_DIMS][:])
    assert np.isnan(legacy).tolist() == [lo <= slot < hi for slot in range(N_TIMES)]
    assert legacy[:lo].tolist() == original[:lo].tolist()
    assert legacy[hi:].tolist() == original[hi:].tolist()
    assert float(np.asarray(group[SCALAR][...])) == 7.0
    expected_state = np.ones(N_TIMES, dtype=np.uint8)
    expected_state[lo:hi] = 2
    assert np.asarray(group[STATE_NAME][:]).tolist() == expected_state.tolist()


def test_delete_span_interior_run_leaves_both_neighbours_untouched(tmp_path: Path) -> None:
    """Deleting the middle run fills exactly its slots; predecessor and successor survive."""
    _seed_append_store(tmp_path, runs=INTERIOR_RUNS)

    result = _delete_run_with_cli(tmp_path, "run-mid")

    assert "NaN-filled 1 spans" in result.output
    group = _open_group(tmp_path)
    precip = np.asarray(group[ARRAY_NAME][:])
    expected = _precip_values()
    assert np.isnan(precip[20:40]).all()
    assert np.array_equal(precip[:20], expected[:20])
    assert np.array_equal(precip[40:], expected[40:])

    dataset = xr.open_zarr(str(_store_root(tmp_path)), group=GROUP, consolidated=False)
    try:
        decoded_bnds = np.asarray(dataset[TIME_BNDS].values)
    finally:
        dataset.close()
    assert np.isnat(decoded_bnds[20:40]).all()
    epoch = np.datetime64("2000-01-01T00:00:00")
    raw = _time_bnds_values(N_TIMES, "int64").astype("timedelta64[s]")
    for keep in (slice(0, 20), slice(40, 60)):
        assert not np.isnat(decoded_bnds[keep]).any()
        assert (decoded_bnds[keep] == epoch + raw[keep]).all()

    assert np.asarray(group[STATE_NAME][:]).tolist() == [1] * 20 + [2] * 20 + [1] * 20


def _assert_refused_without_mutation(
    tmp_path: Path,
    *,
    result: dict[str, Any],
    span_key: str,
    before: dict[str, str],
    history_before: list[tuple[str, str | None]],
    error_fragments: list[str],
) -> None:
    """One error naming the span; zero bytes of the cube changed; span still active."""
    group = _open_group(tmp_path)
    assert np.array_equal(np.asarray(group[ARRAY_NAME][:]), _precip_values()), (
        "precipitation was filled although the span was refused"
    )
    assert (
        np.asarray(group[STATE_NAME][:]).tolist()
        == np.ones(group[STATE_NAME].shape, dtype=np.uint8).tolist()
    ), "state was marked although the span was refused"
    assert _hash_files(_group_dir(tmp_path)) == before
    assert result["deleted_spans"] == 0
    assert len(result["errors"]) == 1, result["errors"]
    assert span_key in result["errors"][0]
    for fragment in error_fragments:
        assert fragment in result["errors"][0], result["errors"][0]
    history = [(key, status) for key, status, _meta in _span_history(tmp_path)]
    assert history == history_before
    assert dict(history)[span_key] == "active"
    assert {status for _key, status in history} == {"active"}


@pytest.mark.parametrize(
    ("seed_kwargs", "error_fragments"),
    [
        pytest.param(
            {"span_arrays": [ARRAY_NAME, "not_an_array"]},
            ["not_an_array"],
            id="missing-array",
        ),
        pytest.param(
            {"span_arrays": [ARRAY_NAME, TIME_BNDS], "state_deleted_value": 256},
            ["256", STATE_NAME],
            id="state-value-256-overflows-uint8",
        ),
        pytest.param(
            {"span_arrays": [ARRAY_NAME, TIME_BNDS], "time_bnds_dtype": "int32"},
            [f"{GROUP}/{TIME_BNDS}", "int32", "declare _FillValue"],
            id="int32-cf-time-bounds-cannot-hold-nat",
        ),
        pytest.param(
            {"span_arrays": [LAT_BNDS, LON_BNDS]},
            [f"no array in this span carries time dimension '{TIME_DIM}'", LAT_BNDS, LON_BNDS],
            id="span-of-only-static-arrays",
        ),
        pytest.param(
            {"span_arrays": [ARRAY_NAME, TIME_BNDS], "time_bnds_length": 30},
            [f"{GROUP}/{TIME_BNDS}"],
            id="range-past-a-short-array",
        ),
        pytest.param(
            {"span_arrays": [ARRAY_NAME, TIME_BNDS], "state_layout": "time-2d"},
            [STATE_NAME],
            id="state-array-is-2d",
        ),
        pytest.param(
            {"span_arrays": [ARRAY_NAME, TIME_BNDS], "state_layout": "other-axis-1d"},
            [STATE_NAME],
            id="state-array-on-another-axis",
        ),
        pytest.param(
            {"span_arrays": [ARRAY_NAME, UNKNOWN_DIMS], "extra_arrays": True},
            [f"{GROUP}/{UNKNOWN_DIMS}"],
            id="array-with-unknown-dimensions",
        ),
    ],
)
def test_region_fill_refusal_mutates_nothing(
    tmp_path: Path, seed_kwargs: dict[str, Any], error_fragments: list[str]
) -> None:
    """Failure mode: a span the engine cannot delete completely aborts before any fill."""
    _seed_append_store(tmp_path, **seed_kwargs)
    span_key = _span_key(tmp_path, "run-b")
    before = _hash_files(_group_dir(tmp_path))
    history_before = [(key, status) for key, status, _meta in _span_history(tmp_path)]

    result = _delete_spans(tmp_path, "run-b")

    _assert_refused_without_mutation(
        tmp_path,
        result=result,
        span_key=span_key,
        before=before,
        history_before=history_before,
        error_fragments=error_fragments,
    )
    if seed_kwargs.get("extra_arrays"):
        unknown = np.asarray(_open_group(tmp_path)[UNKNOWN_DIMS][:])
        assert unknown.tolist() == (np.arange(N_TIMES) + 10.0).tolist()
