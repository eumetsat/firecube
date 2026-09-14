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

"""Data-chunk seeding runs INSIDE the per-(product, group) append claim.

``seed_touched_data_chunks`` is invoked from ``_write_batch`` in
``append.py`` AFTER classification and BEFORE ``writer.execute``. The
surrounding ``AppendStrategy.write_groups`` loop wraps every write with
``claim_for_group(group)``, so the seed call must observe an active
``zarr_append`` claim in the on-disk WAL at the moment it runs.

Two temporal invariants are pinned here:

1. ``seed_touched_data_chunks`` observes a ``:zarr_append:`` claim in
   ``ChunkManager.list_claims`` while it runs, and no ``:zarr_append:``
   claim remains after the run terminates.
2. ``seed_staged_metadata_pre_batch`` (the metadata pre-batch hook that
   runs BEFORE ``host._process_batch`` in
   ``engine._process_batch_timed``) sees NO ``:zarr_append:`` claim. If
   a future change pushed append-claim acquisition earlier — say into
   the pre-batch hook — this assertion catches the ordering shift.

Together these lock down which primitive is the correct target for
"claim held" assertions: data-chunk seeding, not metadata seeding.
"""

from __future__ import annotations

from collections.abc import Iterable
from pathlib import Path
from typing import Any, ClassVar

import numpy as np
import pandas as pd
import pytest
import xarray as xr

from firecube.ingestor.api import GenericZarrIngestor, PluginContext
from firecube.ingestor.runtime.zarr import batch_runner, staged_metadata
from tests.helpers.storage import make_test_context

pytestmark = pytest.mark.unit


_T_BASE = pd.Timestamp("2024-10-01T00:00:00")


class _ClaimHeldIngestor(GenericZarrIngestor):
    PRODUCT_NAME: ClassVar[str] = "claim_held_test"
    name = "claim_held_test"

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
    resume_existing: bool,
    item_count: int,
    ts_offset_hours: int,
    zarr_chunk_shape: dict[str, int] | None = None,
) -> Any:
    source_dir = tmp_path / "source"
    source_dir.mkdir(exist_ok=True)
    (source_dir / "input.nc").touch()
    options: dict[str, Any] = {
        "write_mode": "staged",
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


def test_seed_touched_data_chunks_inside_group_claim(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``seed_touched_data_chunks`` runs inside an active per-(product, group) append claim.

    The resume run's ``_write_batch`` acquires the per-group append claim
    around every write; the seeder call happens between
    ``classify_dataset`` and ``writer.execute``, so a live snapshot of
    ``ChunkManager.list_claims`` must include a ``zarr_append`` domain
    while the spy runs. After the run terminates the claim is released.

    The ``spy invoked >= 1`` assertion is load-bearing: a run that
    silently skipped seeding (e.g. because ``_compute_touched_data_chunks``
    returned empty due to a chunk-shape change) would otherwise pass this
    test without ever exercising the invariant. Chunk shape 2 combined
    with ``ts_offset_hours=5`` places Run B's slot 1 inside chunk 0 —
    the same chunk that holds Run A's slot 0 — forcing the seeder to run.
    """
    product = "claim_probe.zarr"

    _ClaimHeldIngestor().run(
        _make_ctx(
            tmp_path,
            product=product,
            resume_existing=False,
            item_count=1,
            ts_offset_hours=0,
            zarr_chunk_shape={"timestamp": 2},
        )
    )

    host = _ClaimHeldIngestor()
    observed_domains: list[list[str]] = []
    original_seed = staged_metadata.seed_touched_data_chunks

    def spy_seed(**kwargs: Any) -> Any:
        claims = host._chunk_manager.list_claims(product=product)
        observed_domains.append([c.domain for c in claims])
        return original_seed(**kwargs)

    monkeypatch.setattr(staged_metadata, "seed_touched_data_chunks", spy_seed)

    host.run(
        _make_ctx(
            tmp_path,
            product=product,
            resume_existing=True,
            item_count=1,
            ts_offset_hours=5,
            zarr_chunk_shape={"timestamp": 2},
        )
    )

    assert observed_domains, (
        "seed_touched_data_chunks never ran; the claim-held assertion never "
        "executed, so a broken code path could silently pass this test"
    )
    for i, domains in enumerate(observed_domains):
        assert any(":zarr_append:" in d for d in domains), (
            f"Seeding call #{i} observed no active zarr_append claim: domains={domains!r}"
        )

    assert host._chunk_manager.list_claims(product=product) == [], (
        "Append claim leaked after successful run"
    )


def test_metadata_pre_batch_hook_runs_before_claim(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``seed_staged_metadata_pre_batch`` runs BEFORE any append claim is acquired.

    The metadata pre-batch hook lives in ``engine._zarr_pre_batch_hook``
    and is invoked by ``_process_batch_timed`` before
    ``host._process_batch(...)`` — which is where
    ``AppendStrategy.write_groups`` opens the per-group append claim.
    Snapshotting ``list_claims`` from inside the pre-batch hook must show
    NO ``zarr_append`` claim. This documents WHY data-chunk seeding (T8/T9),
    not metadata seeding, is the correct target for a claim-held assertion:
    the metadata hook fires too early to be under the append claim.
    """
    product = "pre_hook_probe.zarr"

    _ClaimHeldIngestor().run(
        _make_ctx(
            tmp_path,
            product=product,
            resume_existing=False,
            item_count=1,
            ts_offset_hours=0,
            zarr_chunk_shape={"timestamp": 2},
        )
    )

    host = _ClaimHeldIngestor()
    observed_domains: list[list[str]] = []
    original_pre_batch = batch_runner.seed_staged_metadata_pre_batch

    def spy_pre_batch(**kwargs: Any) -> Any:
        claims = host._chunk_manager.list_claims(product=product)
        observed_domains.append([c.domain for c in claims])
        return original_pre_batch(**kwargs)

    monkeypatch.setattr(batch_runner, "seed_staged_metadata_pre_batch", spy_pre_batch)

    host.run(
        _make_ctx(
            tmp_path,
            product=product,
            resume_existing=True,
            item_count=1,
            ts_offset_hours=5,
            zarr_chunk_shape={"timestamp": 2},
        )
    )

    assert observed_domains, (
        "seed_staged_metadata_pre_batch never ran; pre-batch ordering unverified"
    )
    for i, domains in enumerate(observed_domains):
        assert not any(":zarr_append:" in d for d in domains), (
            f"Pre-batch hook #{i} observed an active zarr_append claim "
            f"({domains!r}); the metadata pre-batch hook is supposed to fire "
            "BEFORE the append claim is acquired"
        )
