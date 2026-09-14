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

"""Outcome-level guarantees for staged data-chunk seeding.

Locks the following behaviours via engine-driven runs:

* Staged resume-append that touches a partial chunk preserves the earlier
  batch's data at the shared chunk after promotion.
* Multi-batch staged resume runs preserve every batch's writes in a shared
  physical chunk (workspace-existence check prevents second-batch re-seed).
* Direct mode never emits any staged-metadata seeding log — the workspace
  copy step is bypassed entirely.
"""

from __future__ import annotations

import logging
from collections.abc import Iterable
from pathlib import Path
from typing import Any, ClassVar

import numpy as np
import pandas as pd
import pytest
import xarray as xr

from firecube.ingestor.api import GenericZarrIngestor, PluginContext
from tests.helpers.storage import make_test_context

pytestmark = pytest.mark.unit


_T_BASE = pd.Timestamp("2024-10-01T00:00:00")
_STAGED_METADATA_LOGGER = "firecube.ingestor.runtime.zarr.staged_metadata"


class _SeedingOrderIngestor(GenericZarrIngestor):
    PRODUCT_NAME: ClassVar[str] = "seeding_order_test"
    name = "seeding_order_test"

    def discover_source_files(self, ctx: PluginContext) -> Iterable[Any]:
        count = int(ctx.option("x_item_count", 1))
        offset = int(ctx.option("x_ts_offset_hours", 0))
        return [{"index": i, "offset_hours": offset + i} for i in range(count)]

    def get_batch_groups(self, items: Any, ctx: PluginContext) -> list[str]:
        return ["data"]

    def build_dataset(self, group: str, items: list[Any], ctx: PluginContext) -> xr.Dataset | None:
        offsets = [int(item["offset_hours"]) for item in items]
        timestamps = [_T_BASE + pd.Timedelta(hours=hours) for hours in offsets]
        vals = np.asarray([100.0 + hours for hours in offsets], dtype="float32")
        ds = xr.Dataset(
            {"val": (["timestamp"], vals)},
            coords={"timestamp": pd.to_datetime(timestamps)},
        )
        ds["timestamp"].encoding = {
            "units": "seconds since 1970-01-01",
            "dtype": "int64",
            "calendar": "proleptic_gregorian",
        }
        return ds


def _make_ctx(
    tmp_path: Path,
    *,
    product: str,
    write_mode: str,
    resume_existing: bool,
    item_count: int,
    ts_offset_hours: int,
    zarr_chunk_shape: dict[str, int] | None = None,
) -> Any:
    source_dir = tmp_path / "source"
    source_dir.mkdir(exist_ok=True)
    (source_dir / "input.nc").touch()
    options: dict[str, Any] = {
        "write_mode": write_mode,
        "resume_existing": resume_existing,
        "pipeline_batch_size": 1,
        "pipeline_workers": 1,
        "no_progress": True,
        "cleanup_workspace": True,
        "x_item_count": item_count,
        "x_ts_offset_hours": ts_offset_hours,
    }
    if zarr_chunk_shape is not None:
        options["zarr_chunk_shape"] = zarr_chunk_shape
    return make_test_context(
        tmp_path,
        source=str(source_dir),
        product=product,
        options=options,
    )


def _open_final(tmp_path: Path, product: str) -> xr.Dataset:
    return xr.open_zarr(str(tmp_path / product), group="data", consolidated=False)


def test_staged_resume_append_preserves_first_batch_data_and_writes_new_slot(
    tmp_path: Path,
) -> None:
    """Staged resume-append into a partial chunk preserves the earlier slot value."""
    _SeedingOrderIngestor().run(
        _make_ctx(
            tmp_path,
            product="order_probe.zarr",
            write_mode="staged",
            resume_existing=False,
            item_count=1,
            ts_offset_hours=0,
            zarr_chunk_shape={"timestamp": 2},
        )
    )

    _SeedingOrderIngestor().run(
        _make_ctx(
            tmp_path,
            product="order_probe.zarr",
            write_mode="staged",
            resume_existing=True,
            item_count=1,
            ts_offset_hours=5,
            zarr_chunk_shape={"timestamp": 2},
        )
    )

    with _open_final(tmp_path, "order_probe.zarr") as ds:
        assert ds.sizes["timestamp"] == 2, (
            f"expected 2 timestamps after resume-append, got {ds.sizes['timestamp']}"
        )
        vals = np.asarray(ds["val"].values).reshape(-1)
        assert vals[0] == pytest.approx(100.0), (
            f"slot 0 (batch A write) clobbered by staged seed step: got {vals[0]}, expected 100.0"
        )
        assert vals[1] == pytest.approx(105.0), (
            f"slot 1 (batch B write) missing: got {vals[1]}, expected 105.0"
        )


def test_multi_batch_staged_resume_preserves_all_batch_writes_in_shared_chunk(
    tmp_path: Path,
) -> None:
    """Two batches in one staged resume run share a chunk; slot-1 from batch 1 must survive.

    Without the workspace-existence check, batch 2's seed step re-copies chunk
    0 from the final target (which has slot 0 only), clobbering batch 1's
    write to slot 1 with the target's fill value.
    """
    _SeedingOrderIngestor().run(
        _make_ctx(
            tmp_path,
            product="idempotent_probe.zarr",
            write_mode="staged",
            resume_existing=False,
            item_count=1,
            ts_offset_hours=0,
            zarr_chunk_shape={"timestamp": 3},
        )
    )

    _SeedingOrderIngestor().run(
        _make_ctx(
            tmp_path,
            product="idempotent_probe.zarr",
            write_mode="staged",
            resume_existing=True,
            item_count=2,
            ts_offset_hours=5,
            zarr_chunk_shape={"timestamp": 3},
        )
    )

    with _open_final(tmp_path, "idempotent_probe.zarr") as ds:
        assert ds.sizes["timestamp"] == 3, (
            f"expected 3 timestamps after multi-batch resume, got {ds.sizes['timestamp']}"
        )
        vals = np.asarray(ds["val"].values).reshape(-1)
        assert vals[0] == pytest.approx(100.0), (
            f"slot 0 (Run A) clobbered: got {vals[0]}, expected 100.0"
        )
        assert vals[1] == pytest.approx(105.0), (
            f"slot 1 (Run B batch 1) clobbered by Run B batch 2 re-seed: "
            f"got {vals[1]}, expected 105.0"
        )
        assert vals[2] == pytest.approx(106.0), (
            f"slot 2 (Run B batch 2) missing: got {vals[2]}, expected 106.0"
        )


def test_direct_mode_never_emits_staged_metadata_seeding_logs(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """Direct mode bypasses the staged workspace; the staged-metadata module stays silent."""
    _SeedingOrderIngestor().run(
        _make_ctx(
            tmp_path,
            product="direct_probe.zarr",
            write_mode="direct",
            resume_existing=False,
            item_count=1,
            ts_offset_hours=0,
        )
    )

    with caplog.at_level(logging.DEBUG, logger=_STAGED_METADATA_LOGGER):
        _SeedingOrderIngestor().run(
            _make_ctx(
                tmp_path,
                product="direct_probe.zarr",
                write_mode="direct",
                resume_existing=True,
                item_count=1,
                ts_offset_hours=5,
            )
        )

    staged_records = [r for r in caplog.records if r.name == _STAGED_METADATA_LOGGER]
    assert staged_records == [], (
        f"Direct mode must not exercise staged-metadata seeding; got log records: "
        f"{[(r.levelname, r.getMessage()) for r in staged_records]}"
    )

    with _open_final(tmp_path, "direct_probe.zarr") as ds:
        assert ds.sizes["timestamp"] == 2
        vals = np.asarray(ds["val"].values).reshape(-1)
        assert vals[0] == pytest.approx(100.0)
        assert vals[1] == pytest.approx(105.0)
