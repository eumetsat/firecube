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

"""Deleting a span whose record lists static (non-time-indexed) arrays.

Span records written by 0.1.7 list static data vars such as ``lat_bnds`` and
``lon_bnds`` next to the time-indexed arrays. Deletion must leave the static
arrays untouched and say so, fill every time-indexed array with a value that
reads back as missing, and refuse -- with zero bytes mutated -- any span it
cannot delete completely.
"""

from __future__ import annotations

import hashlib
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Literal, cast

import numpy as np
import pytest
import xarray as xr
import zarr
from click.testing import CliRunner

from firecube.cli.main import cli
from firecube.core.controlplane import ChunkManager, SpanCoverage
from tests.helpers.storage import make_test_binding

pytestmark = pytest.mark.integration


PRODUCT = "product.zarr"
GROUP = "default"
TIME_DIM = "time"
PRECIP = "precipitation"
TIME_BNDS = "time_bnds"
LAT_BNDS = "lat_bnds"
LON_BNDS = "lon_bnds"
STATE_NAME = "firecube_timestamp_state"
STATE_PATH = f"{GROUP}/{STATE_NAME}"
RUN_ID = "run-a"
UNITS = "seconds since 2000-01-01"
N_TIMES = 3
DELETED_SLOT = 1

StateLayout = Literal["time-1d", "time-2d", "other-axis-1d", "absent"]


def _manager(tmp_path: Path) -> ChunkManager:
    return ChunkManager(binding=make_test_binding(tmp_path, product=PRODUCT), workspace=tmp_path)


def _store_root(tmp_path: Path) -> Path:
    return tmp_path / PRODUCT


def _group_dir(tmp_path: Path) -> Path:
    return _store_root(tmp_path) / GROUP


def _open_group(tmp_path: Path, mode: Literal["r", "r+"] = "r") -> Any:
    return cast(Any, zarr.open_group(store=str(_group_dir(tmp_path)), mode=mode, zarr_format=3))


def _precip_values() -> np.ndarray:
    return np.arange(1, N_TIMES * 4 + 1, dtype=np.float32).reshape(N_TIMES, 2, 2)


def _time_bnds_values(length: int, dtype: str) -> np.ndarray:
    starts = np.arange(length, dtype=np.int64) * 3600 + 3600
    return np.stack([starts, starts + 1800], axis=1).astype(dtype)


def _seed_store(
    tmp_path: Path,
    *,
    time_bnds_dtype: str = "int64",
    time_bnds_length: int = N_TIMES,
    state_layout: StateLayout = "time-1d",
) -> None:
    """Cube with one time-indexed data var, CF time bounds and static bounds."""
    root = zarr.open_group(store=str(_store_root(tmp_path)), mode="w", zarr_format=3)
    group = root.require_group(GROUP)

    precip = group.create_array(
        PRECIP,
        shape=(N_TIMES, 2, 2),
        chunks=(1, 2, 2),
        dtype="f4",
        fill_value=np.nan,
        dimension_names=(TIME_DIM, "lat", "lon"),
    )
    precip[:] = _precip_values()

    time_bnds = group.create_array(
        TIME_BNDS,
        shape=(time_bnds_length, 2),
        chunks=(1, 2),
        dtype=time_bnds_dtype,
        fill_value=0,
        dimension_names=(TIME_DIM, "nv"),
    )
    time_bnds.attrs.update({"units": UNITS, "calendar": "standard"})
    time_bnds[:] = _time_bnds_values(time_bnds_length, time_bnds_dtype)

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

    if state_layout == "absent":
        return
    shape: tuple[int, ...]
    dims: tuple[str, ...]
    if state_layout == "time-1d":
        shape, dims = (N_TIMES,), (TIME_DIM,)
    elif state_layout == "time-2d":
        shape, dims = (N_TIMES, 2), (TIME_DIM, "x")
    else:
        shape, dims = (N_TIMES,), ("other",)
    state = group.create_array(
        STATE_NAME, shape=shape, chunks=shape, dtype="u1", fill_value=0, dimension_names=dims
    )
    state[:] = np.uint8(1)


UNKNOWN_DIMS = "unknown_dims"
LEGACY_DIMS = "legacy_dims"
SCALAR = "scalar"


def _add_arrays_without_dimension_names(tmp_path: Path) -> None:
    """Add arrays that carry no zarr v3 ``dimension_names``.

    ``unknown_dims`` has no dimension metadata at all, so whether it is
    time-indexed is unknowable; ``legacy_dims`` declares its time axis only
    through the ``_ARRAY_DIMENSIONS`` attr; ``scalar`` is 0-d.
    """
    group = _open_group(tmp_path, mode="r+")
    unknown = group.create_array(
        UNKNOWN_DIMS, shape=(N_TIMES,), chunks=(1,), dtype="f8", fill_value=np.nan
    )
    unknown[:] = np.asarray([10.0, 20.0, 30.0])
    legacy = group.create_array(
        LEGACY_DIMS, shape=(N_TIMES,), chunks=(1,), dtype="f8", fill_value=np.nan
    )
    legacy.attrs["_ARRAY_DIMENSIONS"] = [TIME_DIM]
    legacy[:] = np.asarray([1.5, 2.5, 3.5])
    scalar = group.create_array(SCALAR, shape=(), dtype="f8", fill_value=np.nan)
    scalar[...] = 7.0


def _record_span(
    tmp_path: Path,
    *,
    arrays: list[str],
    start: int = DELETED_SLOT,
    end: int = DELETED_SLOT,
    state_array: str | None = STATE_PATH,
    state_deleted_value: int = 2,
    aligned: bool = False,
) -> str:
    """Record a span shaped like a 0.1.7 generic-append record and return its key."""
    manager = _manager(tmp_path)
    output_path = str(_store_root(tmp_path))
    base_time = datetime(2000, 1, 1)
    time_min = (base_time + timedelta(hours=start + 1)).isoformat()
    time_max = (base_time + timedelta(hours=end + 1)).isoformat()
    coverage = SpanCoverage(
        group=GROUP,
        arrays=[f"{GROUP}/{name}" for name in arrays],
        time_index_ranges=[[start, end]],
        aligned=aligned,
        state_array=state_array,
        state_deleted_value=state_deleted_value,
        time_min=time_min,
        time_max=time_max,
        time_dim_name=TIME_DIM,
    )
    try:
        manager.record_run_started(
            product=PRODUCT,
            run_id=RUN_ID,
            output_path=output_path,
            output_format="zarr",
            size=0,
            meta={"plugin": "test"},
        )
        manager.record_span(
            product=PRODUCT,
            run_id=RUN_ID,
            batch_id="batch-a",
            group=GROUP,
            status="active",
            coverage=coverage,
            meta={"plugin": "test", "group": GROUP, "time_min": time_min, "time_max": time_max},
        )
        manager.record_run_terminal(
            product=PRODUCT,
            run_id=RUN_ID,
            output_path=output_path,
            output_format="zarr",
            size=1,
            meta={"plugin": "test"},
            status="complete",
        )
        spans = manager.list_chunks(product=PRODUCT, chunk_type="span")
    finally:
        manager.close()
    assert len(spans) == 1
    return spans[0].key


def _delete_spans(tmp_path: Path) -> dict[str, Any]:
    """Run the same engine call ``chunks delete-span --force --yes-i-really-mean-it`` makes."""
    manager = _manager(tmp_path)
    try:
        spans = manager.list_chunks(product=PRODUCT, chunk_type="span")
        return manager.delete_spans(spans, force=True, yes_i_really_mean_it=True)
    finally:
        manager.close()


def _span_history(tmp_path: Path) -> list[tuple[str, str | None, dict[str, Any]]]:
    manager = _manager(tmp_path)
    try:
        spans = manager.list_chunks(product=PRODUCT, chunk_type="span", include_replaced=True)
    finally:
        manager.close()
    return [(span.key, span.status, dict(span.meta or {})) for span in spans]


def _hash_files(root: Path) -> dict[str, str]:
    """sha256 of every file under *root*, keyed by relative path."""
    return {
        str(path.relative_to(root)): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in sorted(root.rglob("*"))
        if path.is_file()
    }


def _assert_refused_without_mutation(
    tmp_path: Path,
    *,
    result: dict[str, Any],
    span_key: str,
    before: dict[str, str],
    error_fragments: list[str],
) -> None:
    """One error naming the span; zero bytes of the cube changed; span still active."""
    group = _open_group(tmp_path)
    assert np.array_equal(np.asarray(group[PRECIP][:]), _precip_values()), (
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
    assert [(key, status) for key, status, _meta in _span_history(tmp_path)] == [
        (span_key, "active")
    ]


def test_delete_span_listing_static_arrays_fills_time_arrays_and_skips_statics(
    tmp_path: Path,
) -> None:
    """A 0.1.7 span listing lat_bnds/lon_bnds deletes cleanly through the CLI."""
    _seed_store(tmp_path)
    span_key = _record_span(tmp_path, arrays=[LAT_BNDS, LON_BNDS, PRECIP, TIME_BNDS])
    lat_before = _hash_files(_group_dir(tmp_path) / LAT_BNDS)
    lon_before = _hash_files(_group_dir(tmp_path) / LON_BNDS)
    time_bnds_attrs_before = dict(_open_group(tmp_path)[TIME_BNDS].attrs)

    result = CliRunner().invoke(
        cli,
        [
            "chunks",
            "--workspace",
            str(tmp_path),
            "delete-span",
            "--product-name",
            _store_root(tmp_path).as_uri(),
            "--run-id",
            RUN_ID,
            "--yes-i-really-mean-it",
        ],
    )

    assert result.exit_code == 0, result.output
    assert "NaN-filled 1 spans" in result.output
    assert "Warnings: 1" in result.output
    assert (
        f"Span {span_key}: skipped 2 arrays without time dimension '{TIME_DIM}': "
        f"{LAT_BNDS}, {LON_BNDS}"
    ) in result.output

    group = _open_group(tmp_path)
    precip = np.asarray(group[PRECIP][:])
    assert np.isnan(precip[DELETED_SLOT]).all()
    expected = _precip_values()
    for slot in (0, 2):
        assert np.array_equal(precip[slot], expected[slot])

    assert _hash_files(_group_dir(tmp_path) / LAT_BNDS) == lat_before
    assert _hash_files(_group_dir(tmp_path) / LON_BNDS) == lon_before
    assert dict(group[TIME_BNDS].attrs) == time_bnds_attrs_before

    dataset = xr.open_zarr(str(_store_root(tmp_path)), group=GROUP, consolidated=False)
    try:
        decoded_bnds = np.asarray(dataset[TIME_BNDS].values)
    finally:
        dataset.close()
    assert decoded_bnds.dtype.kind == "M"
    assert np.isnat(decoded_bnds).tolist() == [[False, False], [True, True], [False, False]]
    epoch = np.datetime64("2000-01-01T00:00:00")
    raw = _time_bnds_values(N_TIMES, "int64")
    for slot in (0, 2):
        assert (decoded_bnds[slot] == epoch + raw[slot].astype("timedelta64[s]")).all()

    assert np.asarray(group[STATE_NAME][:]).tolist() == [1, 2, 1]
    [(key, status, meta)] = _span_history(tmp_path)
    assert (key, status) == (span_key, "replaced")
    assert meta["write_strategy"] == "region_nan_fill"


def test_delete_span_with_missing_array_mutates_nothing(tmp_path: Path) -> None:
    """Failure mode: an array the span lists but the cube lacks aborts before any fill."""
    _seed_store(tmp_path)
    span_key = _record_span(tmp_path, arrays=[PRECIP, "not_an_array"])
    before = _hash_files(_group_dir(tmp_path))

    result = _delete_spans(tmp_path)

    _assert_refused_without_mutation(
        tmp_path,
        result=result,
        span_key=span_key,
        before=before,
        error_fragments=["not_an_array"],
    )


def test_delete_span_with_unrepresentable_state_value_mutates_nothing(tmp_path: Path) -> None:
    """Failure mode: a state value that does not fit uint8 aborts before any fill."""
    _seed_store(tmp_path)
    span_key = _record_span(tmp_path, arrays=[PRECIP, TIME_BNDS], state_deleted_value=256)
    before = _hash_files(_group_dir(tmp_path))

    result = _delete_spans(tmp_path)

    _assert_refused_without_mutation(
        tmp_path,
        result=result,
        span_key=span_key,
        before=before,
        error_fragments=["256", STATE_NAME],
    )


def test_delete_span_with_int32_cf_time_array_mutates_nothing(tmp_path: Path) -> None:
    """Failure mode: an int32 CF time array cannot carry NaT, so nothing is filled."""
    _seed_store(tmp_path, time_bnds_dtype="int32")
    span_key = _record_span(tmp_path, arrays=[PRECIP, TIME_BNDS])
    before = _hash_files(_group_dir(tmp_path))

    result = _delete_spans(tmp_path)

    _assert_refused_without_mutation(
        tmp_path,
        result=result,
        span_key=span_key,
        before=before,
        error_fragments=[f"{GROUP}/{TIME_BNDS}", "int32", "declare _FillValue"],
    )


def test_delete_span_without_any_time_indexed_array_mutates_nothing(tmp_path: Path) -> None:
    """Failure mode: a span of only static arrays is refused rather than marked deleted."""
    _seed_store(tmp_path)
    span_key = _record_span(tmp_path, arrays=[LAT_BNDS, LON_BNDS])
    before = _hash_files(_group_dir(tmp_path))

    result = _delete_spans(tmp_path)

    _assert_refused_without_mutation(
        tmp_path,
        result=result,
        span_key=span_key,
        before=before,
        error_fragments=[
            f"no array in this span carries time dimension '{TIME_DIM}'",
            LAT_BNDS,
            LON_BNDS,
        ],
    )


def test_delete_span_range_past_a_short_array_mutates_nothing(tmp_path: Path) -> None:
    """Failure mode: zarr clips out-of-range slices, so the range is checked up front."""
    _seed_store(tmp_path, time_bnds_length=1)
    span_key = _record_span(tmp_path, arrays=[PRECIP, TIME_BNDS])
    before = _hash_files(_group_dir(tmp_path))

    result = _delete_spans(tmp_path)

    _assert_refused_without_mutation(
        tmp_path,
        result=result,
        span_key=span_key,
        before=before,
        error_fragments=[f"{GROUP}/{TIME_BNDS}"],
    )


@pytest.mark.parametrize("state_layout", ["time-2d", "other-axis-1d"])
def test_delete_span_with_malformed_state_array_mutates_nothing(
    tmp_path: Path, state_layout: StateLayout
) -> None:
    """Failure mode: a state array that is not 1-D on the time dim aborts before any fill."""
    _seed_store(tmp_path, state_layout=state_layout)
    span_key = _record_span(tmp_path, arrays=[PRECIP, TIME_BNDS])
    before = _hash_files(_group_dir(tmp_path))

    result = _delete_spans(tmp_path)

    _assert_refused_without_mutation(
        tmp_path,
        result=result,
        span_key=span_key,
        before=before,
        error_fragments=[STATE_NAME],
    )


def test_chunk_key_delete_skips_static_array_with_warning(tmp_path: Path) -> None:
    """Without a state array, chunk-key deletion removes only time-indexed chunks."""
    _seed_store(tmp_path, state_layout="absent")
    span_key = _record_span(tmp_path, arrays=[PRECIP, LAT_BNDS], state_array=None, aligned=True)
    precip_chunks = _group_dir(tmp_path) / PRECIP / "c"
    assert sorted(p.name for p in precip_chunks.iterdir()) == ["0", "1", "2"]
    lat_before = _hash_files(_group_dir(tmp_path) / LAT_BNDS)
    kept_before = {slot: _hash_files(precip_chunks / str(slot)) for slot in (0, 2)}

    result = _delete_spans(tmp_path)

    assert result["errors"] == []
    assert result["warnings"] == [
        f"Span {span_key}: skipped 1 arrays without time dimension '{TIME_DIM}': {GROUP}/{LAT_BNDS}"
    ]
    assert result["deleted_keys"] == 1
    assert result["deleted_spans"] == 1
    assert not (precip_chunks / str(DELETED_SLOT) / "0" / "0").exists()
    for slot, hashes in kept_before.items():
        assert _hash_files(precip_chunks / str(slot)) == hashes
    assert _hash_files(_group_dir(tmp_path) / LAT_BNDS) == lat_before
    [(key, status, meta)] = _span_history(tmp_path)
    assert (key, status) == (span_key, "replaced")
    assert "write_strategy" not in meta


def test_chunk_key_delete_of_only_static_arrays_is_refused(tmp_path: Path) -> None:
    """Failure mode: chunk-key deletion aborts loudly on a span with no time-indexed array.

    Every array lacking the time dim is also what a wrong time-dim name looks
    like, so this keeps the resolver's loud abort and its remediation hint.
    """
    _seed_store(tmp_path, state_layout="absent")
    span_key = _record_span(tmp_path, arrays=[LAT_BNDS, LON_BNDS], state_array=None, aligned=True)
    before = _hash_files(_group_dir(tmp_path))

    with pytest.raises(ValueError) as excinfo:
        _delete_spans(tmp_path)

    message = str(excinfo.value)
    assert span_key in message
    assert f"no array in this span carries time dimension '{TIME_DIM}'" in message
    assert "--time-dim" in message
    assert _hash_files(_group_dir(tmp_path)) == before
    assert [(key, status) for key, status, _meta in _span_history(tmp_path)] == [
        (span_key, "active")
    ]


def test_region_fill_refuses_array_with_unknown_dimensions(tmp_path: Path) -> None:
    """Failure mode: an array with no dimension metadata is not proven static, so refuse."""
    _seed_store(tmp_path)
    _add_arrays_without_dimension_names(tmp_path)
    span_key = _record_span(tmp_path, arrays=[PRECIP, UNKNOWN_DIMS])
    before = _hash_files(_group_dir(tmp_path))

    result = _delete_spans(tmp_path)

    _assert_refused_without_mutation(
        tmp_path,
        result=result,
        span_key=span_key,
        before=before,
        error_fragments=[f"{GROUP}/{UNKNOWN_DIMS}"],
    )
    assert np.asarray(_open_group(tmp_path)[UNKNOWN_DIMS][:]).tolist() == [10.0, 20.0, 30.0]


def test_region_fill_fills_array_dimensioned_by_array_dimensions_attr(tmp_path: Path) -> None:
    """An ``_ARRAY_DIMENSIONS``-declared time array is filled; a 0-d scalar is skipped."""
    _seed_store(tmp_path)
    _add_arrays_without_dimension_names(tmp_path)
    span_key = _record_span(tmp_path, arrays=[PRECIP, LEGACY_DIMS, SCALAR])

    result = _delete_spans(tmp_path)

    assert result["errors"] == []
    assert result["region_filled_spans"] == 1
    assert result["warnings"] == [
        f"Span {span_key}: skipped 1 arrays without time dimension '{TIME_DIM}': {SCALAR}"
    ]
    group = _open_group(tmp_path)
    legacy = np.asarray(group[LEGACY_DIMS][:])
    assert np.isnan(legacy).tolist() == [False, True, False]
    assert legacy[[0, 2]].tolist() == [1.5, 3.5]
    assert float(np.asarray(group[SCALAR][...])) == 7.0
    assert np.asarray(group[STATE_NAME][:]).tolist() == [1, 2, 1]


def test_chunk_key_delete_refuses_array_with_unknown_dimensions(tmp_path: Path) -> None:
    """Failure mode: chunk-key deletion aborts before removing any chunk of the span."""
    _seed_store(tmp_path, state_layout="absent")
    _add_arrays_without_dimension_names(tmp_path)
    span_key = _record_span(tmp_path, arrays=[PRECIP, UNKNOWN_DIMS], state_array=None, aligned=True)
    before = _hash_files(_group_dir(tmp_path))

    with pytest.raises(ValueError) as excinfo:
        _delete_spans(tmp_path)

    assert f"{GROUP}/{UNKNOWN_DIMS}" in str(excinfo.value)
    assert _hash_files(_group_dir(tmp_path)) == before
    assert [(key, status) for key, status, _meta in _span_history(tmp_path)] == [
        (span_key, "active")
    ]


def test_chunk_key_delete_removes_array_dimensioned_by_array_dimensions_attr(
    tmp_path: Path,
) -> None:
    """Chunk-key deletion honours ``_ARRAY_DIMENSIONS`` and skips a 0-d scalar with a warning."""
    _seed_store(tmp_path, state_layout="absent")
    _add_arrays_without_dimension_names(tmp_path)
    span_key = _record_span(
        tmp_path, arrays=[PRECIP, LEGACY_DIMS, SCALAR], state_array=None, aligned=True
    )
    legacy_chunks = _group_dir(tmp_path) / LEGACY_DIMS / "c"
    scalar_before = _hash_files(_group_dir(tmp_path) / SCALAR)

    result = _delete_spans(tmp_path)

    assert result["errors"] == []
    assert result["warnings"] == [
        f"Span {span_key}: skipped 1 arrays without time dimension '{TIME_DIM}': {GROUP}/{SCALAR}"
    ]
    assert result["deleted_keys"] == 2
    assert not (legacy_chunks / str(DELETED_SLOT)).exists()
    assert (legacy_chunks / "0").exists() and (legacy_chunks / "2").exists()
    assert not (_group_dir(tmp_path) / PRECIP / "c" / str(DELETED_SLOT) / "0" / "0").exists()
    assert _hash_files(_group_dir(tmp_path) / SCALAR) == scalar_before
