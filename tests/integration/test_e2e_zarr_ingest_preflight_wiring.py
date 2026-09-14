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

"""End-to-end runtime-wiring lock for ``preflight_compare_target_uri``.

Three engine-driven scenarios exercise the full ``host.run(...)`` call chain
(never ``write_dataset_to_zarr`` directly, never a CLI subprocess) so the
runtime wiring of ``preflight_compare_target_uri`` cannot silently regress.

Call chain locked (any missing hop breaks these tests):

    host.run(ctx)
      -> GenericZarrIngestor._process_batch
      -> _build_zarr_batch_runtime (templates/generic.py)
      -> batch_runner.build_append_strategy
      -> AppendStrategy.write_groups
      -> append_time_groups
      -> write_dataset_to_zarr
      -> _preflight_compare_static_vars / _preflight_compare_attrs_and_warn

Scenarios:

* **Append path.** Run 1 writes days ``[0, 1, 2]`` with a
  static ``lat_bnds`` variable. Run 2 (resume, NEW timestamps ``[3, 4, 5]``)
  attempts to write drifted ``lat_bnds``. The append path fires
  ``_preflight_compare_static_vars`` which raises ``SchemaDriftError``; the
  engine wraps it as ``AppendBatchFailed`` and the pipeline finalizer raises
  ``PipelineFailedBatchesError`` mentioning ``lat_bnds``.

* **Region-overwrite (force_reingest) path.** Run 1 writes days
  ``[0, 1, 2]``. Run 2 force-reingests day ``1`` (EXISTING timestamp -> region
  path) with drifted ``lat_bnds``. Locks that
  ``_preflight_compare_static_vars`` runs inside the ``region is not None``
  branch of ``write_dataset_to_zarr``.

* **Attrs preservation across two batches.** A single run with
  ``pipeline_batch_size=1`` and two days produces two batches against the
  same target. Batch 1 (``mode="w"``) writes attrs ``{"title":"first",
  "history":"run1"}`` to the target. Batch 2 (``mode="a"``) preflights against
  the same target and observes drift for both keys; ``caplog`` captures
  exactly one WARN whose ``changed`` field contains ``title`` and ``history``.
  ``_snapshot_group_attrs`` + ``_restore_group_attrs`` then preserves
  batch-1's attrs, so the stored group ends with ``title=="first"`` and
  ``history=="run1"``. Direct write mode is used because staged mode with a
  fresh target leaves ``preflight_compare_target_uri`` pointing at an empty
  final target during both batches (no attrs to compare against, no WARN);
  the restore path still works in staged single-run mode but the WARN
  signal that proves the preflight is *wired* only fires when the compare
  target is populated. Direct mode is the smallest end-to-end setup that
  exercises both the wiring and the restore behaviour in one run.

RED verification (evidence of behaviour lock):
  1. git worktree add /tmp/firecube-preplan d391cf1
  2. uv pip install -e tests/fixtures/firecube_test_plugins  # install fixture from HEAD
     (fixture package did not exist at d391cf1 - must install from HEAD or copy
     the package tree into the worktree and set PYTHONPATH accordingly)
  3. PYTHONPATH=/tmp/firecube-preplan/src \\
     uv run pytest tests/integration/test_e2e_zarr_ingest_preflight_wiring.py -v
  4. Expected: each scenario fails on its target assertion (SchemaDriftError not raised
     for Scenario 1/2; attrs wiped for Scenario 3). Failures MUST NOT be plugin-import
     or fixture-missing errors - those are invalid REDs.
  5. git worktree remove /tmp/firecube-preplan

Reversion sentinel: removing ``preflight_compare_target_uri`` from
``build_append_strategy(...)`` in ``src/firecube/ingestor/templates/generic.py``
MUST make Scenarios 1 + 2 pass without ``SchemaDriftError`` (proving the wiring
is what this test file locks, not just the static-var check itself).
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import numpy as np
import pytest
import xarray as xr

from firecube.ingestor.api import GenericZarrIngestor, PluginContext
from firecube.ingestor.runtime.engine import PipelineFailedBatchesError
from tests.helpers.storage import make_test_context

pytestmark = pytest.mark.integration

_GROUP = "G"
_PRODUCT = "preflight_wiring.zarr"


def _dataset(
    days: list[int],
    *,
    lat_bnds: np.ndarray | None,
    attrs: dict[str, Any] | None = None,
) -> xr.Dataset:
    """Build a dataset with a time-indexed ``temperature`` and optional static ``lat_bnds``."""
    timestamps = np.datetime64("2024-01-01", "ns") + np.asarray(days).astype("timedelta64[D]")
    values = np.array([10.0 + d for d in days], dtype=np.float32).reshape(len(days), 1)
    ds = xr.Dataset(
        {"temperature": (("timestamp", "x"), values)},
        coords={"timestamp": timestamps, "x": np.arange(1)},
    )
    if lat_bnds is not None:
        ds = ds.assign(lat_bnds=(("lat", "nv"), lat_bnds))
    ds["timestamp"].encoding.update(dtype="int64", units="nanoseconds since 1970-01-01")
    if attrs is not None:
        ds.attrs.update(attrs)
    return ds


class _PreflightWiringZarr(GenericZarrIngestor):
    """Test-local plugin driven by CLI options for reproducible engine runs."""

    PRODUCT_NAME = "preflight_wiring"
    name = "preflight_wiring"
    time_dim_name = "timestamp"

    def discover_source_files(self, ctx: PluginContext) -> list[int]:
        return list(ctx.option("x_days") or [])

    def get_batch_groups(self, items, ctx: PluginContext) -> list[str]:
        _ = items
        _ = ctx
        return [_GROUP]

    def build_dataset(self, group: str, items: list[int], ctx: PluginContext) -> xr.Dataset:
        _ = group
        # Per-batch attrs are keyed by the first day of the batch so a single run
        # can emit different attrs per batch (used by Scenario 3).
        attrs_per_day = ctx.option("x_attrs_per_day", None)
        attrs: dict[str, Any] | None
        if attrs_per_day is not None and items and items[0] in attrs_per_day:
            attrs = dict(attrs_per_day[items[0]])
        else:
            attrs = ctx.option("x_attrs", None)

        lat_bnds_raw = ctx.option("x_lat_bnds", None)
        lat_bnds = np.asarray(lat_bnds_raw, dtype=np.float32) if lat_bnds_raw is not None else None

        return _dataset(list(items), lat_bnds=lat_bnds, attrs=attrs)


def _engine_run(
    tmp_path: Path,
    *,
    days: list[int],
    write_mode: str,
    lat_bnds: np.ndarray | None = None,
    attrs: dict[str, Any] | None = None,
    attrs_per_day: dict[int, dict[str, Any]] | None = None,
    pipeline_batch_size: int = 8,
    resume_existing: bool = False,
    force_reingest: bool = False,
) -> None:
    """Drive one ingest through ``host.run(ctx)``.

    ``write_mode`` picks between ``"direct"`` (single URI, no workspace) and
    ``"staged"`` (workspace + promotion). See scenario docstrings for why each
    scenario picks its mode.
    """
    host = _PreflightWiringZarr()
    options: dict[str, Any] = {
        "write_mode": write_mode,
        "x_days": days,
        "x_attrs": attrs,
        "x_attrs_per_day": attrs_per_day,
        "x_lat_bnds": lat_bnds.tolist() if lat_bnds is not None else None,
        "pipeline_workers": 1,
        "pipeline_batch_size": pipeline_batch_size,
        "no_progress": True,
        "cleanup_workspace": True,
    }
    if resume_existing:
        options["resume_existing"] = True
    if force_reingest:
        options["force_reingest"] = True
    ctx = make_test_context(tmp_path, product=_PRODUCT, options=options)
    host.run(ctx)


def _attr_drift_warnings(caplog: pytest.LogCaptureFixture) -> list[logging.LogRecord]:
    return [
        record
        for record in caplog.records
        if record.levelno == logging.WARNING
        and record.message == "Group attributes differ from stored; keeping first-write values"
    ]


def test_static_drift_append_path_engine_driven(tmp_path: Path) -> None:
    """Wiring lock: static-var drift on the append path via ``host.run``.

    Run 1 populates the target with days ``[0, 1, 2]`` and
    ``lat_bnds=[[0,1],[1,2],[2,3]]``. Run 2 resumes with NEW timestamps
    ``[3, 4, 5]`` and drifted ``lat_bnds``. The append path calls
    ``_preflight_compare_static_vars``; when the wiring is intact the
    engine's finalize step reports the failure via
    ``PipelineFailedBatchesError`` with ``lat_bnds`` in the message.

    Removing ``preflight_compare_target_uri`` from ``build_append_strategy``
    (see the reversion sentinel in the module docstring) makes this test go
    silent - the drift check never sees the final target - which is why
    this test is the primary end-to-end wiring lock.
    """
    lat_bnds_original = np.array([[0, 1], [1, 2], [2, 3]], dtype=np.float32)
    lat_bnds_drifted = np.array([[10, 20], [20, 30], [30, 40]], dtype=np.float32)

    _engine_run(
        tmp_path,
        days=[0, 1, 2],
        write_mode="staged",
        lat_bnds=lat_bnds_original,
    )

    with pytest.raises(PipelineFailedBatchesError, match="lat_bnds"):
        _engine_run(
            tmp_path,
            days=[3, 4, 5],
            write_mode="staged",
            resume_existing=True,
            lat_bnds=lat_bnds_drifted,
        )


def test_static_drift_region_overwrite_engine_driven(tmp_path: Path) -> None:
    """Wiring lock: static-var drift on the region-overwrite path via ``host.run``.

    Run 1 writes days ``[0, 1, 2]`` with ``lat_bnds=[[0,1],[1,2],[2,3]]``. Run
    2 force-reingests day ``1`` (an EXISTING timestamp, which routes through
    the region-overwrite branch, not the append branch) with drifted
    ``lat_bnds``. Locks that ``_preflight_compare_static_vars`` runs
    inside the ``region is not None`` branch of
    ``write_dataset_to_zarr``. Without that wiring, the region path
    silently overwrites static data.
    """
    lat_bnds_original = np.array([[0, 1], [1, 2], [2, 3]], dtype=np.float32)
    lat_bnds_drifted = np.array([[10, 20], [20, 30], [30, 40]], dtype=np.float32)

    _engine_run(
        tmp_path,
        days=[0, 1, 2],
        write_mode="staged",
        lat_bnds=lat_bnds_original,
    )

    with pytest.raises(PipelineFailedBatchesError, match="lat_bnds"):
        _engine_run(
            tmp_path,
            days=[1],
            write_mode="staged",
            resume_existing=True,
            force_reingest=True,
            lat_bnds=lat_bnds_drifted,
        )


def test_two_batch_attrs_preservation_engine_driven(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """Wiring lock: batch-1 attrs win across two engine-driven batches; WARN fires.

    A single ``host.run`` with ``pipeline_batch_size=1`` and two days produces
    two batches:

    * Batch 1 (``mode="w"``) writes ``{"title": "first", "history": "run1"}``
      to the target. First writes take no preflight compare (fresh target).
    * Batch 2 (``mode="a"``) writes ``{"title": "second", "history": "run2"}``.
      The engine passes ``preflight_compare_target_uri`` to the append
      strategy, ``_preflight_compare_attrs_and_warn`` observes ``title`` and
      ``history`` drifted, and emits exactly one WARN whose ``changed``
      contains both keys. ``_snapshot_group_attrs`` then captures
      batch-1's attrs before the write and ``_restore_group_attrs`` restores
      them after, so the stored group ends with batch-1's values.

    Direct mode is used deliberately. In a fresh staged run the
    ``preflight_compare_target_uri`` points at the empty final target during
    both batches, so no WARN would fire even though the restore path
    still preserves attrs from the workspace snapshot. Direct mode is the
    smallest single-run engine setup that both restores attrs AND exercises
    the WARN wiring; a two-run staged setup (see
    ``test_staged_partial_chunk_preserves_data.py``) covers the same
    wiring end-to-end from the staged angle.
    """
    lat_bnds = np.array([[0, 1], [1, 2], [2, 3]], dtype=np.float32)

    with caplog.at_level(logging.WARNING):
        _engine_run(
            tmp_path,
            days=[0, 1],
            write_mode="direct",
            pipeline_batch_size=1,
            lat_bnds=lat_bnds,
            attrs_per_day={
                0: {"title": "first", "history": "run1"},
                1: {"title": "second", "history": "run2"},
            },
        )

    warnings = _attr_drift_warnings(caplog)
    title_count = sum(1 for r in warnings if "title" in getattr(r, "changed", ()))
    history_count = sum(1 for r in warnings if "history" in getattr(r, "changed", ()))
    assert title_count == 1, (
        f"expected exactly 1 attr-drift WARN mentioning 'title'; got {title_count} "
        f"(records: {[getattr(r, 'changed', ()) for r in warnings]})"
    )
    assert history_count == 1, (
        f"expected exactly 1 attr-drift WARN mentioning 'history'; got {history_count} "
        f"(records: {[getattr(r, 'changed', ()) for r in warnings]})"
    )

    target = tmp_path / _PRODUCT
    with xr.open_zarr(str(target), group=_GROUP, consolidated=False) as ds:
        assert ds.attrs["title"] == "first", (
            f"first-write-wins: title should be 'first', got {ds.attrs.get('title')!r}"
        )
        assert ds.attrs["history"] == "run1", (
            f"first-write-wins: history should be 'run1', got {ds.attrs.get('history')!r}"
        )
