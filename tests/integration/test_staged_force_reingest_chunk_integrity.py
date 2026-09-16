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

"""force_reingest neighbour-slot preservation + per-batch clobber regression.

Covers:

* Single-batch force_reingest of an existing timestamp (region_overwrite
  path) preserves the untouched neighbouring slots of the physical chunk
  it lands in.
* Multi-batch force_reingest into the same physical chunk: a staged
  ``append_time_groups`` call with ``batch_size=1`` and
  ``force_reingest=True`` produces two batches that BOTH land in the same
  physical chunk. The workspace-existence check in
  ``seed_touched_data_chunks`` prevents batch B from re-seeding a chunk
  that batch A has already written to.
* Append-path drift: force_reingest with NEW timestamps whose incoming
  static variable value differs from the stored one raises
  ``SchemaDriftError``. The region_overwrite path skips the preflight
  compare (see ``write.py::write_dataset_to_zarr`` region branch), so
  drift on existing-timestamp force_reingest is a separate concern and is
  not covered here.
* Sharded multi-batch variant: with the shard aligned to cover both
  touched chunks, the workspace-existence check operates on the SHARD
  key and still refuses to overwrite a shard batch A wrote.
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Sequence
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
import xarray as xr

from firecube.core.storage.uri import StorageUri
from firecube.ingestor.errors import SchemaDriftError
from firecube.ingestor.runtime.zarr.append import append_time_groups
from firecube.ingestor.runtime.zarr.append_failure import AppendBatchFailed
from firecube.ingestor.runtime.zarr.staged_metadata import seed_staged_store_metadata
from tests.helpers.storage import local_zarr_handle, make_local_session, make_test_session

pytestmark = pytest.mark.integration

_GROUP = "G"
_START = pd.Timestamp("2024-01-01")

# Distinct sentinel values keep the assertion errors self-describing when a
# slot has been clobbered.
_ORIGINAL_VAL = {d: 100.0 + d for d in range(10)}
_FORCE_REINGEST_VAL = {d: 900.0 + d for d in range(10)}


def _day_index(ts: pd.Timestamp) -> int:
    return int((ts - _START).days)


def _make_dataset(
    _group: str,
    batch: Sequence[pd.Timestamp],
    *,
    values_map: dict[int, float],
) -> xr.Dataset:
    """Build the ``val`` dataset for a batch of daily timestamps."""
    ts = pd.to_datetime(list(batch))
    day_ids = [_day_index(t) for t in ts]
    val = np.array([values_map[d] for d in day_ids], dtype=np.float32).reshape(len(ts), 1)
    return xr.Dataset(
        {"val": (("timestamp", "x"), val)},
        coords={"timestamp": ts, "x": np.arange(1)},
    )


def _make_dataset_with_static(
    group: str,
    batch: Sequence[pd.Timestamp],
    *,
    values_map: dict[int, float],
    lat_bnds: np.ndarray,
    attrs: dict[str, str] | None = None,
) -> xr.Dataset:
    """Build the ``val`` dataset augmented with a static ``lat_bnds`` variable."""
    ds = _make_dataset(group, batch, values_map=values_map)
    ds = ds.assign(lat_bnds=(("lat", "nv"), lat_bnds))
    if attrs is not None:
        ds.attrs.update(attrs)
    return ds


def _run_initial_write(
    *,
    final_store: Path,
    days: list[int],
    chunk_shape: dict[str, int],
    shard_shape: dict[str, int] | None = None,
    sharding: bool = False,
) -> None:
    """Populate the final target with days worth of ``val`` data."""
    timestamps = [_START + pd.Timedelta(days=d) for d in days]
    append_time_groups(
        store=str(final_store),
        zarr_store=local_zarr_handle(final_store),
        session=make_local_session(str(final_store)),
        group_to_timestamps={_GROUP: timestamps},
        dataset_for_batch=lambda g, b: _make_dataset(g, b, values_map=_ORIGINAL_VAL),
        arrays_for_group=lambda g: [f"{g}/val"],
        chunk_shape=chunk_shape,
        shard_shape=shard_shape,
        sharding=sharding,
        batch_size=len(timestamps),
    )


def _run_initial_write_with_static(
    *,
    final_store: Path,
    days: list[int],
    lat_bnds: np.ndarray,
    chunk_shape: dict[str, int],
    attrs: dict[str, str] | None = None,
) -> None:
    """Populate the final target with a static ``lat_bnds`` variable."""
    timestamps = [_START + pd.Timedelta(days=d) for d in days]
    append_time_groups(
        store=str(final_store),
        zarr_store=local_zarr_handle(final_store),
        session=make_local_session(str(final_store)),
        group_to_timestamps={_GROUP: timestamps},
        dataset_for_batch=lambda g, b: _make_dataset_with_static(
            g, b, values_map=_ORIGINAL_VAL, lat_bnds=lat_bnds, attrs=attrs
        ),
        arrays_for_group=lambda g: [f"{g}/val"],
        chunk_shape=chunk_shape,
        batch_size=len(timestamps),
    )


def _seed_workspace_metadata(*, workspace_store: Path, final_store: Path) -> None:
    """Seed workspace zarr.json + coord chunks so it looks resumable."""
    workspace_store.parent.mkdir(parents=True, exist_ok=True)
    seed_staged_store_metadata(
        temp_store_uri=str(workspace_store),
        final_target_uri=str(final_store),
        groups=[_GROUP],
        session=make_local_session(str(workspace_store)),
        coordinate_arrays=["timestamp", "firecube_timestamp_state"],
    )


def _default_force_reingest_builder(group: str, batch: Sequence[pd.Timestamp]) -> xr.Dataset:
    return _make_dataset(group, batch, values_map=_FORCE_REINGEST_VAL)


def _run_force_reingest(
    *,
    workspace_store: Path,
    final_store: Path,
    days: list[int],
    chunk_shape: dict[str, int],
    shard_shape: dict[str, int] | None = None,
    sharding: bool = False,
    batch_size: int,
    dataset_builder: Callable[[str, Sequence[pd.Timestamp]], xr.Dataset] | None = None,
) -> None:
    """Run a staged force-reingest of the given day indices."""
    timestamps = [_START + pd.Timedelta(days=d) for d in days]
    builder = dataset_builder if dataset_builder is not None else _default_force_reingest_builder

    append_time_groups(
        store=str(workspace_store),
        zarr_store=local_zarr_handle(workspace_store),
        session=make_local_session(str(workspace_store)),
        resume_zarr_store=local_zarr_handle(final_store, mode="r"),
        group_to_timestamps={_GROUP: timestamps},
        dataset_for_batch=builder,
        arrays_for_group=lambda g: [f"{g}/val"],
        chunk_shape=chunk_shape,
        shard_shape=shard_shape,
        sharding=sharding,
        resume_existing=True,
        force_reingest=True,
        batch_size=batch_size,
        pipeline_write_mode="staged",
        final_target_uri=str(final_store),
        preflight_compare_zarr_store=local_zarr_handle(final_store, mode="r"),
    )


def _promote_workspace_to_final(
    *, tmp_path: Path, workspace_store: Path, final_store: Path
) -> None:
    make_test_session(tmp_path).upload_tree(
        StorageUri.from_local_path(workspace_store),
        StorageUri.from_local_path(final_store),
    )


def _read_final(final_store: Path) -> xr.Dataset:
    return xr.open_zarr(str(final_store), group=_GROUP, consolidated=False)


def _attr_drift_warnings(caplog: pytest.LogCaptureFixture) -> list[logging.LogRecord]:
    return [
        record
        for record in caplog.records
        if record.levelno == logging.WARNING
        and record.message == "Group attributes differ from stored; keeping first-write values"
    ]


def test_single_batch_force_reingest_preserves_neighbouring_slots(tmp_path: Path) -> None:
    """Single-slot force_reingest preserves the untouched neighbouring slots.

    Setup: days 0..3 with ``chunk_shape=2`` — physical chunks ``[0, 1]``
    and ``[2, 3]``. Force-reingest offers day_1 only. The write lands in
    chunk 0 which must be seeded first so day_0 survives promotion.
    """
    final = tmp_path / "final.zarr"
    workspace = tmp_path / "workspace" / "final.zarr"

    chunk_shape = {"timestamp": 2}

    _run_initial_write(final_store=final, days=[0, 1, 2, 3], chunk_shape=chunk_shape)
    _seed_workspace_metadata(workspace_store=workspace, final_store=final)

    _run_force_reingest(
        workspace_store=workspace,
        final_store=final,
        days=[1],
        chunk_shape=chunk_shape,
        batch_size=1,
    )
    _promote_workspace_to_final(tmp_path=tmp_path, workspace_store=workspace, final_store=final)

    ds = _read_final(final)
    assert ds.sizes["timestamp"] == 4

    state = np.asarray(ds["firecube_timestamp_state"].values)
    assert state.tolist() == [1, 1, 1, 1], f"state = {state.tolist()}"

    val = np.asarray(ds["val"].values).reshape(-1)
    assert val[0] == _ORIGINAL_VAL[0], (
        f"slot 0 clobbered on force_reingest of neighbour slot 1: "
        f"got {val[0]}, expected {_ORIGINAL_VAL[0]}"
    )
    assert val[1] == _FORCE_REINGEST_VAL[1], (
        f"slot 1 wrong: got {val[1]}, expected {_FORCE_REINGEST_VAL[1]}"
    )
    assert val[2] == _ORIGINAL_VAL[2], (
        f"slot 2 (untouched chunk) not preserved by promotion: "
        f"got {val[2]}, expected {_ORIGINAL_VAL[2]}"
    )
    assert val[3] == _ORIGINAL_VAL[3], (
        f"slot 3 (untouched chunk) not preserved by promotion: "
        f"got {val[3]}, expected {_ORIGINAL_VAL[3]}"
    )


def test_multi_batch_force_reingest_same_chunk_preserves_all_slots(tmp_path: Path) -> None:
    """Two batches within ONE staged run, both touching the same chunk.

    Setup: days 0..3 with ``chunk_shape=4`` — a single physical chunk
    holds all four days. ``force_reingest=True`` with ``batch_size=1``
    and two input days (day_1 + day_2) produces two batches:

    * Batch A seeds chunk 0 (from the target), writes day_1.
    * Batch B MUST NOT re-seed chunk 0 (that would clobber batch A's
      day_1 write). The workspace-existence check refuses to re-copy an
      already-present chunk key. Batch B just writes day_2.

    Expected workspace: ``[d0_orig, d1_new, d2_new, d3_orig]``.
    The failure mode without the fix drops day_1's force-reingest value.
    """
    final = tmp_path / "final.zarr"
    workspace = tmp_path / "workspace" / "final.zarr"

    chunk_shape = {"timestamp": 4}

    _run_initial_write(final_store=final, days=[0, 1, 2, 3], chunk_shape=chunk_shape)
    _seed_workspace_metadata(workspace_store=workspace, final_store=final)

    _run_force_reingest(
        workspace_store=workspace,
        final_store=final,
        days=[1, 2],
        chunk_shape=chunk_shape,
        batch_size=1,
    )
    _promote_workspace_to_final(tmp_path=tmp_path, workspace_store=workspace, final_store=final)

    ds = _read_final(final)
    assert ds.sizes["timestamp"] == 4

    state = np.asarray(ds["firecube_timestamp_state"].values)
    assert state.tolist() == [1, 1, 1, 1], f"state = {state.tolist()}"

    val = np.asarray(ds["val"].values).reshape(-1)
    assert val[0] == _ORIGINAL_VAL[0], (
        f"slot 0 clobbered by per-batch re-seed: got {val[0]}, expected {_ORIGINAL_VAL[0]}"
    )
    assert val[1] == _FORCE_REINGEST_VAL[1], (
        f"slot 1 batch A write clobbered by batch B's re-seed of chunk 0: "
        f"got {val[1]}, expected {_FORCE_REINGEST_VAL[1]}"
    )
    assert val[2] == _FORCE_REINGEST_VAL[2], (
        f"slot 2 batch B write missing: got {val[2]}, expected {_FORCE_REINGEST_VAL[2]}"
    )
    assert val[3] == _ORIGINAL_VAL[3], (
        f"slot 3 clobbered: got {val[3]}, expected {_ORIGINAL_VAL[3]}"
    )


def test_append_path_force_reingest_static_drift_raises(tmp_path: Path) -> None:
    """force_reingest with NEW timestamps + drifted static var raises.

    Adding a new timestamp routes through the append path (not
    region_overwrite), so the preflight compare in
    ``write_dataset_to_zarr`` fires. When the incoming ``lat_bnds``
    differs from the stored value, ``SchemaDriftError`` is raised with
    the new-store guidance appropriate for ``force_reingest=True``.
    """
    final = tmp_path / "final.zarr"
    workspace = tmp_path / "workspace" / "final.zarr"

    chunk_shape = {"timestamp": 2}
    lat_bnds_original = np.array([[0.0, 1.0]], dtype=np.float32)
    lat_bnds_drifted = np.array([[10.0, 20.0]], dtype=np.float32)

    _run_initial_write_with_static(
        final_store=final,
        days=[0, 1, 2, 3],
        lat_bnds=lat_bnds_original,
        chunk_shape=chunk_shape,
    )
    _seed_workspace_metadata(workspace_store=workspace, final_store=final)

    def _drifted_builder(group, batch):
        return _make_dataset_with_static(
            group, batch, values_map=_FORCE_REINGEST_VAL, lat_bnds=lat_bnds_drifted
        )

    with pytest.raises(AppendBatchFailed, match="lat_bnds") as exc_info:
        _run_force_reingest(
            workspace_store=workspace,
            final_store=final,
            days=[4, 5],
            chunk_shape=chunk_shape,
            batch_size=2,
            dataset_builder=_drifted_builder,
        )

    assert isinstance(exc_info.value.__cause__, SchemaDriftError), (
        f"Expected SchemaDriftError as __cause__, got: {exc_info.value.__cause__!r}"
    )


def test_sharded_multi_batch_force_reingest_preserves_all_slots(tmp_path: Path) -> None:
    """Sharded variant — same preservation semantics as the non-sharded case.

    ``chunk_shape=2`` and ``shard_shape=4`` — one shard covers both
    touched chunks (slots 1 and 2 fall in chunks 0 and 1 which share a
    shard). The workspace-existence check operates on the SHARD key
    (shard-vs-chunk write-unit resolution) and refuses to re-copy a
    shard already written by batch A.
    """
    final = tmp_path / "final.zarr"
    workspace = tmp_path / "workspace" / "final.zarr"

    chunk_shape = {"timestamp": 2}
    shard_shape = {"timestamp": 4}

    _run_initial_write(
        final_store=final,
        days=[0, 1, 2, 3],
        chunk_shape=chunk_shape,
        shard_shape=shard_shape,
        sharding=True,
    )
    _seed_workspace_metadata(workspace_store=workspace, final_store=final)

    _run_force_reingest(
        workspace_store=workspace,
        final_store=final,
        days=[1, 2],
        chunk_shape=chunk_shape,
        shard_shape=shard_shape,
        sharding=True,
        batch_size=1,
    )
    _promote_workspace_to_final(tmp_path=tmp_path, workspace_store=workspace, final_store=final)

    ds = _read_final(final)
    assert ds.sizes["timestamp"] == 4

    state = np.asarray(ds["firecube_timestamp_state"].values)
    assert state.tolist() == [1, 1, 1, 1], f"state = {state.tolist()}"

    val = np.asarray(ds["val"].values).reshape(-1)
    assert val[0] == _ORIGINAL_VAL[0], (
        f"sharded: slot 0 clobbered: got {val[0]}, expected {_ORIGINAL_VAL[0]}"
    )
    assert val[1] == _FORCE_REINGEST_VAL[1], (
        f"sharded: slot 1 batch A write clobbered by batch B's re-seed of the shard: "
        f"got {val[1]}, expected {_FORCE_REINGEST_VAL[1]}"
    )
    assert val[2] == _FORCE_REINGEST_VAL[2], (
        f"sharded: slot 2 batch B write missing: got {val[2]}, expected {_FORCE_REINGEST_VAL[2]}"
    )
    assert val[3] == _ORIGINAL_VAL[3], (
        f"sharded: slot 3 clobbered: got {val[3]}, expected {_ORIGINAL_VAL[3]}"
    )


def test_region_overwrite_force_reingest_static_drift_raises(tmp_path: Path) -> None:
    """force_reingest with EXISTING timestamps checks static vars before region write."""
    final = tmp_path / "final.zarr"
    workspace = tmp_path / "workspace" / "final.zarr"

    chunk_shape = {"timestamp": 2}
    lat_bnds_original = np.array([[0.0, 1.0], [1.0, 2.0], [2.0, 3.0], [3.0, 4.0]])
    lat_bnds_drifted = np.array([[10.0, 20.0], [20.0, 30.0], [30.0, 40.0], [40.0, 50.0]])

    _run_initial_write_with_static(
        final_store=final,
        days=[0, 1, 2, 3],
        lat_bnds=lat_bnds_original,
        chunk_shape=chunk_shape,
    )
    _seed_workspace_metadata(workspace_store=workspace, final_store=final)

    def _drifted_builder(group, batch):
        return _make_dataset_with_static(
            group, batch, values_map=_FORCE_REINGEST_VAL, lat_bnds=lat_bnds_drifted
        )

    with pytest.raises(AppendBatchFailed, match="lat_bnds") as exc_info:
        _run_force_reingest(
            workspace_store=workspace,
            final_store=final,
            days=[1],
            chunk_shape=chunk_shape,
            batch_size=1,
            dataset_builder=_drifted_builder,
        )

    assert isinstance(exc_info.value.__cause__, SchemaDriftError), (
        f"Expected SchemaDriftError as __cause__, got: {exc_info.value.__cause__!r}"
    )
    assert "lat_bnds" in str(exc_info.value.__cause__)
    np.testing.assert_array_equal(
        np.asarray(_read_final(final)["lat_bnds"].values),
        lat_bnds_original,
    )


def test_region_overwrite_matching_static_vars_no_attr_warning(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """Matching static vars on region-overwrite complete without attr-drift warnings."""
    final = tmp_path / "final.zarr"
    workspace = tmp_path / "workspace" / "final.zarr"

    chunk_shape = {"timestamp": 2}
    lat_bnds = np.array([[0.0, 1.0], [1.0, 2.0], [2.0, 3.0], [3.0, 4.0]])

    _run_initial_write_with_static(
        final_store=final,
        days=[0, 1, 2, 3],
        lat_bnds=lat_bnds,
        chunk_shape=chunk_shape,
    )
    _seed_workspace_metadata(workspace_store=workspace, final_store=final)

    def _matching_builder(group, batch):
        return _make_dataset_with_static(
            group, batch, values_map=_FORCE_REINGEST_VAL, lat_bnds=lat_bnds
        )

    with caplog.at_level(logging.WARNING):
        _run_force_reingest(
            workspace_store=workspace,
            final_store=final,
            days=[1],
            chunk_shape=chunk_shape,
            batch_size=1,
            dataset_builder=_matching_builder,
        )

    assert _attr_drift_warnings(caplog) == []


def test_region_overwrite_attr_change_stays_silent(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """Region-overwrite checks static vars only, so changed attrs stay silent."""
    final = tmp_path / "final.zarr"
    workspace = tmp_path / "workspace" / "final.zarr"

    chunk_shape = {"timestamp": 2}
    lat_bnds = np.array([[0.0, 1.0], [1.0, 2.0], [2.0, 3.0], [3.0, 4.0]])

    _run_initial_write_with_static(
        final_store=final,
        days=[0, 1, 2, 3],
        lat_bnds=lat_bnds,
        chunk_shape=chunk_shape,
        attrs={"title": "first"},
    )
    _seed_workspace_metadata(workspace_store=workspace, final_store=final)

    def _changed_attrs_builder(group, batch):
        return _make_dataset_with_static(
            group,
            batch,
            values_map=_FORCE_REINGEST_VAL,
            lat_bnds=lat_bnds,
            attrs={"title": "changed"},
        )

    with caplog.at_level(logging.WARNING):
        _run_force_reingest(
            workspace_store=workspace,
            final_store=final,
            days=[1],
            chunk_shape=chunk_shape,
            batch_size=1,
            dataset_builder=_changed_attrs_builder,
        )

    assert _attr_drift_warnings(caplog) == []
    assert _read_final(final).attrs["title"] == "first"


def test_mixed_batch_append_warns_once_region_batch_silent(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """Append batch warns once for attr drift; following region batch stays silent."""
    final = tmp_path / "final.zarr"
    workspace = tmp_path / "workspace" / "final.zarr"

    chunk_shape = {"timestamp": 2}
    lat_bnds = np.array([[0.0, 1.0], [1.0, 2.0], [2.0, 3.0], [3.0, 4.0]])

    _run_initial_write_with_static(
        final_store=final,
        days=[0, 1, 2, 3],
        lat_bnds=lat_bnds,
        chunk_shape=chunk_shape,
        attrs={"title": "first"},
    )
    _seed_workspace_metadata(workspace_store=workspace, final_store=final)

    def _changed_attrs_builder(group, batch):
        return _make_dataset_with_static(
            group,
            batch,
            values_map=_FORCE_REINGEST_VAL,
            lat_bnds=lat_bnds,
            attrs={"title": "changed"},
        )

    with caplog.at_level(logging.WARNING):
        _run_force_reingest(
            workspace_store=workspace,
            final_store=final,
            days=[4, 1],
            chunk_shape=chunk_shape,
            batch_size=1,
            dataset_builder=_changed_attrs_builder,
        )

    attr_warnings = _attr_drift_warnings(caplog)
    assert len(attr_warnings) == 1
    assert getattr(attr_warnings[0], "changed", ()) == ("title",)
