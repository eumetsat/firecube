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

"""Deleting an append span fills its region without deleting shared chunks."""

from __future__ import annotations

import hashlib
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Literal, cast

import numpy as np
import pytest
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


def _manager(tmp_path: Path) -> ChunkManager:
    return ChunkManager(binding=make_test_binding(tmp_path, product=PRODUCT), workspace=tmp_path)


def _store_root(tmp_path: Path) -> Path:
    return tmp_path / PRODUCT


def _append_chunk_key(tmp_path: Path) -> Path:
    return _store_root(tmp_path) / GROUP / ARRAY_NAME / "c" / "0" / "0"


def _open_group(tmp_path: Path, mode: Literal["r", "r+"] = "r") -> Any:
    return cast(
        Any,
        zarr.open_group(store=str(_store_root(tmp_path) / GROUP), mode=mode, zarr_format=3),
    )


def _hash_run_a_slots(tmp_path: Path) -> str:
    group = _open_group(tmp_path)
    return hashlib.sha256(group[ARRAY_NAME][0:30].tobytes()).hexdigest()


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
) -> None:
    output_path = str(_store_root(manager.workspace))
    base_time = datetime(2024, 1, 1)
    time_min = (base_time + timedelta(days=start)).isoformat()
    time_max = (base_time + timedelta(days=end)).isoformat()
    coverage = SpanCoverage(
        group=GROUP,
        arrays=[ARRAY_PATH],
        time_index_ranges=[[start, end]],
        aligned=False,
        state_array=state_array_path or (STATE_PATH if append_store else None),
        state_deleted_value=2,
        time_min=time_min,
        time_max=time_max,
        time_dim_name="timestamp",
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


def _seed_append_store(tmp_path: Path) -> None:
    root = zarr.open_group(store=str(_store_root(tmp_path)), mode="w", zarr_format=3)
    group = root.require_group(GROUP)
    data = group.create_array(
        ARRAY_NAME,
        shape=(60, 2),
        chunks=(365, 2),
        dtype="f4",
        fill_value=np.nan,
        dimension_names=("timestamp", "x"),
        overwrite=True,
    )
    data[0:30] = np.full((30, 2), 1.0, dtype=np.float32)
    data[30:60] = np.full((30, 2), 2.0, dtype=np.float32)

    timestamp = group.create_array(
        "timestamp",
        shape=(60,),
        chunks=(365,),
        dtype="i8",
        fill_value=0,
        dimension_names=("timestamp",),
        overwrite=True,
    )
    timestamp[:] = np.arange(60, dtype=np.int64)

    state = group.create_array(
        STATE_NAME,
        shape=(60,),
        chunks=(365,),
        dtype="u1",
        fill_value=0,
        dimension_names=("timestamp",),
        overwrite=True,
    )
    state[:] = np.uint8(1)

    manager = _manager(tmp_path)
    try:
        _record_span(manager, run_id="run-a", batch_id="batch-a", start=0, end=29)
        _record_span(manager, run_id="run-b", batch_id="batch-b", start=30, end=59)
    finally:
        manager.close()


def _create_raw_data_array(tmp_path: Path) -> Any:
    """Create the raw (non-append) store and return its root group."""
    root = zarr.open_group(store=str(_store_root(tmp_path)), mode="w", zarr_format=3)
    group = root.require_group(GROUP)
    data = group.create_array(
        ARRAY_NAME,
        shape=(8, 2),
        chunks=(4, 2),
        dtype="f4",
        fill_value=np.nan,
        dimension_names=("timestamp", "x"),
        overwrite=True,
    )
    data[:] = np.ones((8, 2), dtype=np.float32)
    return root


def _seed_raw_store(tmp_path: Path) -> None:
    _create_raw_data_array(tmp_path)

    manager = _manager(tmp_path)
    try:
        _record_span(
            manager,
            run_id="run-raw",
            batch_id="batch-raw",
            start=0,
            end=3,
            append_store=False,
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
    """data[30:60] is NaN/fill after deletion."""
    _seed_append_store(tmp_path)

    _delete_run_with_cli(tmp_path, "run-b")

    group = _open_group(tmp_path)
    deleted = np.asarray(group[ARRAY_NAME][30:60])
    neighbours = np.asarray(group[ARRAY_NAME][0:30])
    assert np.isnan(deleted).all()
    assert np.array_equal(neighbours, np.full((30, 2), 1.0, dtype=np.float32))


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
