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

"""Engine-driven partial-chunk data + attrs preservation on resume-append.

Two engine-driven variants that exercise the full ``host.run(...)`` path (not
``write_dataset_to_zarr`` directly) to lock the following behaviour end-to-end
on plain resume-appends (NO ``force_reingest``):

* **Data preservation.** With ``chunk_shape={"timestamp": 2}`` and
  run 1 writing days ``[0, 1, 2]``, chunk 0 is full and chunk 1 is half-full
  (only ``day_2``). Run 2 appends ``day_3`` to extend chunk 1 to
  ``[day_2, day_3]``. The regression symptom was that the seeding step
  could clobber ``day_2`` during workspace preparation; after the fix,
  ``temperature[2]`` MUST survive promotion with its original run-1 value.

* **Attrs preservation (engine-driven).** Same chunk setup with
  ``pipeline_batch_size=1`` on run 2. Run-1 dataset attrs
  ``{"title": "first", "history": "run1", "layout_tags": ("weather", "eu")}``
  MUST win over run-2 attrs ``{"title": "second", "history": "run2",
  "layout_tags": ("weather", "eu")}`` (first-write-wins). Because the drift
  diff normalises tuples/lists, ``layout_tags`` compares equal after the
  JSON round-trip and MUST NOT show up in any WARN record's ``changed`` field.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import numpy as np
import pytest
import xarray as xr

from firecube.ingestor.api import GenericZarrIngestor, PluginContext
from tests.helpers.storage import make_test_context

pytestmark = pytest.mark.integration

_GROUP = "G"
_PRODUCT = "partial_chunk_preservation.zarr"

# Distinct sentinel values keep the assertion errors self-describing when a
# slot has been clobbered by the seeding step.
_TEMPERATURE = {d: 10.0 + d for d in range(10)}


def _dataset(
    days: list[int],
    *,
    attrs: dict[str, Any] | None = None,
) -> xr.Dataset:
    """Build a small ``temperature`` dataset for the given day indices."""
    timestamps = np.datetime64("2024-01-01", "ns") + np.asarray(days).astype("timedelta64[D]")
    values = np.array([_TEMPERATURE[d] for d in days], dtype=np.float32).reshape(len(days), 1)
    ds = xr.Dataset(
        {"temperature": (("timestamp", "x"), values)},
        coords={"timestamp": timestamps, "x": np.arange(1)},
    )
    ds["timestamp"].encoding.update(dtype="int64", units="nanoseconds since 1970-01-01")
    if attrs is not None:
        ds.attrs.update(attrs)
    return ds


class _PartialChunkZarr(GenericZarrIngestor):
    """Test-local plugin driven by CLI options for reproducible engine runs."""

    PRODUCT_NAME = "partial_chunk_preservation"
    name = "partial_chunk_preservation"
    time_dim_name = "timestamp"

    def discover_source_files(self, ctx: PluginContext) -> list[int]:
        return list(ctx.option("x_days") or [])

    def get_batch_groups(self, items, ctx: PluginContext) -> list[str]:
        return [_GROUP]

    def build_dataset(self, group: str, items: list[int], ctx: PluginContext) -> xr.Dataset:
        _ = group
        return _dataset(list(items), attrs=ctx.option("x_attrs", None))


def _engine_run(
    tmp_path: Path,
    days: list[int],
    *,
    attrs: dict[str, Any] | None = None,
    pipeline_batch_size: int = 8,
    resume_existing: bool = False,
) -> None:
    """Drive one ingest through ``host.run(ctx)`` in staged write mode."""
    host = _PartialChunkZarr()
    options: dict[str, Any] = {
        "write_mode": "staged",
        "x_days": days,
        "x_attrs": attrs,
        "pipeline_workers": 1,
        "pipeline_batch_size": pipeline_batch_size,
        "no_progress": True,
        "cleanup_workspace": True,
        "zarr_chunk_shape": {"timestamp": 2, "x": 1},
    }
    if resume_existing:
        options["resume_existing"] = True
    ctx = make_test_context(tmp_path, product=_PRODUCT, options=options)
    host.run(ctx)


def _attr_drift_warnings(caplog: pytest.LogCaptureFixture) -> list[logging.LogRecord]:
    return [
        record
        for record in caplog.records
        if record.levelno == logging.WARNING
        and record.message == "Group attributes differ from stored; keeping first-write values"
    ]


def test_resume_append_preserves_partial_chunk(tmp_path: Path) -> None:
    """Partial-chunk data survives a resume-append (no ``force_reingest``).

    ``chunk_shape={"timestamp": 2}`` — physical chunks are ``[0, 1]`` and
    ``[2, 3]``. Run 1 writes days ``[0, 1, 2]``, filling chunk 0 and leaving
    chunk 1 half-full with ``day_2`` only. Run 2 appends ``day_3``: the seed
    step MUST NOT clobber the existing ``day_2`` slot in chunk 1. After
    promotion, ``temperature[2]`` is preserved as its run-1 value.

    Regression symptom (pre-fix): ``temperature[2]`` came back NaN because
    the seeding path replaced the partial chunk with fill values before
    batch A wrote ``day_3`` into it.
    """
    _engine_run(tmp_path, days=[0, 1, 2])
    _engine_run(tmp_path, days=[3], resume_existing=True)

    target = tmp_path / _PRODUCT
    with xr.open_zarr(str(target), group=_GROUP, consolidated=False) as ds:
        assert ds.sizes["timestamp"] == 4, (
            f"expected timestamp size 4 after run 2 append, got {ds.sizes['timestamp']}"
        )
        temp = np.asarray(ds["temperature"].values).reshape(-1)
        assert not np.isnan(temp[2]), (
            "temperature[2] (partial-chunk slot from run 1) was clobbered: got NaN"
        )
        assert temp[2] == pytest.approx(_TEMPERATURE[2]), (
            f"temperature[2] should preserve run-1 value {_TEMPERATURE[2]}, got {temp[2]}"
        )
        # Sanity: other slots also match their expected sentinel values.
        assert temp[0] == pytest.approx(_TEMPERATURE[0])
        assert temp[1] == pytest.approx(_TEMPERATURE[1])
        assert temp[3] == pytest.approx(_TEMPERATURE[3])


def test_multi_batch_resume_append_preserves_first_write_attrs_engine_driven(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """First-write-wins group attrs on engine-driven resume-append.

    Same chunk setup as the resume-append case with ``pipeline_batch_size=1``
    on run 2 to force the engine's per-batch write path. Run-1 attrs win;
    the drift diff normalises tuples/lists so ``layout_tags`` compares equal
    after the JSON round-trip and MUST NOT show up in any WARN record's
    ``changed``.
    """
    run1_attrs = {
        "title": "first",
        "history": "run1",
        "layout_tags": ("weather", "eu"),
    }
    run2_attrs = {
        "title": "second",
        "history": "run2",
        "layout_tags": ("weather", "eu"),
    }

    _engine_run(tmp_path, days=[0, 1, 2], attrs=run1_attrs)

    with caplog.at_level(logging.WARNING):
        _engine_run(
            tmp_path,
            days=[3],
            attrs=run2_attrs,
            pipeline_batch_size=1,
            resume_existing=True,
        )

    warnings = _attr_drift_warnings(caplog)
    title_count = sum(1 for r in warnings if "title" in getattr(r, "changed", ()))
    history_count = sum(1 for r in warnings if "history" in getattr(r, "changed", ()))
    tags_count = sum(1 for r in warnings if "layout_tags" in getattr(r, "changed", ()))
    assert title_count == 1, (
        f"expected exactly 1 attr-drift WARN mentioning 'title'; got {title_count} "
        f"(records: {[getattr(r, 'changed', ()) for r in warnings]})"
    )
    assert history_count == 1, (
        f"expected exactly 1 attr-drift WARN mentioning 'history'; got {history_count} "
        f"(records: {[getattr(r, 'changed', ()) for r in warnings]})"
    )
    assert tags_count == 0, (
        f"expected 0 attr-drift WARN mentioning 'layout_tags' (tuple/list "
        f"normalisation); got {tags_count} "
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
        # JSON round-trip lists the tuple; tuple/list normalisation still lets the compare match.
        assert ds.attrs["layout_tags"] == ["weather", "eu"], (
            f"layout_tags should round-trip to list ['weather','eu'], got "
            f"{ds.attrs.get('layout_tags')!r}"
        )
