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

from __future__ import annotations

import logging
import threading
from types import SimpleNamespace
from unittest.mock import ANY, MagicMock, patch

import numpy as np
import pandas as pd
import pytest
import xarray as xr
import zarr

from firecube.core.config import StorageConfig
from firecube.core.controlplane.types import WriteDomain
from firecube.ingestor.runtime.zarr.batch_runner import (
    assemble_batch_metrics,
    build_append_strategy,
    build_claim_closure_for_append,
    build_zarr_write_context,
    seed_staged_metadata_for_batch,
)
from firecube.ingestor.runtime.zarr.staged_metadata import StagedMetadataError
from firecube.ingestor.runtime.zarr.write_context import ZarrWriteContext
from tests.helpers.storage import make_local_session


def test_assemble_batch_metrics_merges_all_sources() -> None:
    result = assemble_batch_metrics(
        prep_metrics={"prep_count": 5},
        zarr_metrics={"coverage": ["g1"], "duration_s": 1.2},
        file_count=10,
        write_mode="direct",
    )

    assert result == {
        "prep_count": 5,
        "zarr": {"coverage": ["g1"], "duration_s": 1.2},
        "coverage": ["g1"],
        "count": 10,
        "storage_handled": True,
    }


def test_assemble_batch_metrics_staged_mode_storage_handled_false() -> None:
    result = assemble_batch_metrics(
        prep_metrics={}, zarr_metrics={}, file_count=1, write_mode="staged"
    )

    assert result["storage_handled"] is False


def test_assemble_batch_metrics_empty_coverage_defaults_to_list() -> None:
    result = assemble_batch_metrics(
        prep_metrics={}, zarr_metrics={"duration_s": 1.2}, file_count=1, write_mode="direct"
    )

    assert result["coverage"] == []


def test_assemble_batch_metrics_coverage_extracted_from_zarr() -> None:
    result = assemble_batch_metrics(
        prep_metrics={},
        zarr_metrics={"coverage": ["F024", "F048"]},
        file_count=1,
        write_mode="direct",
    )

    assert result["coverage"] == ["F024", "F048"]


def test_build_zarr_write_context_creates_correct_instance() -> None:
    lock = threading.Lock()

    result = build_zarr_write_context(
        zarr_config={"dask_scheduler": "synchronous", "write_threads": 2, "async_concurrency": 5},
        write_lock=lock,
    )

    assert isinstance(result, ZarrWriteContext)
    assert result._write_lock is lock
    assert result._configured_scheduler == "synchronous"
    assert result._write_threads == 2
    assert result._async_concurrency == 5


def test_build_zarr_write_context_defaults() -> None:
    lock = threading.Lock()

    result = build_zarr_write_context(zarr_config={}, write_lock=lock)

    assert isinstance(result, ZarrWriteContext)
    assert result._write_lock is lock
    assert result._configured_scheduler is None
    assert result._write_threads == 0
    assert result._async_concurrency == 10


def test_build_claim_closure_for_append_correct_domain() -> None:
    chunk_manager = MagicMock()
    chunk_manager.acquire_claim.return_value = object()
    closure = build_claim_closure_for_append(
        chunk_manager=chunk_manager, product="TEST_PRODUCT.zarr", run_id="run-001"
    )

    closure("F024")

    chunk_manager.acquire_claim.assert_called_once_with(
        product="TEST_PRODUCT.zarr",
        domain=WriteDomain(product="TEST_PRODUCT.zarr", category="zarr_append", name="F024"),
        owner_id="run-001:F024",
    )


def test_build_append_strategy_writes_group_metrics(tmp_path) -> None:
    store_uri = str(tmp_path / "out.zarr")
    chunk_manager = SimpleNamespace(
        storage_config=StorageConfig(storage_type="local", storage_driver="fsspec")
    )
    logger = logging.getLogger("test_batch_runner")
    session = make_local_session(store_uri)

    strategy = build_append_strategy(
        store_uri=store_uri,
        final_target_uri=None,
        zarr_config={"chunk_shape": {"timestamp": 2, "lat": 2, "lon": 3}},
        resume_existing=True,
        force_reingest=False,
        chunk_manager=chunk_manager,
        session=session,
        logger=logger,
    )

    timestamps = pd.date_range("2024-01-01", periods=2, freq="h")

    def dataset_for_batch(group: str, items) -> xr.Dataset:
        return xr.Dataset(
            {
                "FWI": (
                    ("timestamp", "lat", "lon"),
                    np.ones((len(items), 2, 3), dtype=np.float32),
                )
            },
            coords={"timestamp": list(items), "lat": np.arange(2), "lon": np.arange(3)},
        )

    metrics = strategy.write_groups(
        group_to_timestamps={"G1": list(timestamps)},
        dataset_for_batch=dataset_for_batch,
        batch_size=2,
        claim_for_group=None,
    )

    written = xr.open_zarr(store_uri, group="G1", consolidated=False)
    assert written.sizes == {"timestamp": 2, "lat": 2, "lon": 3}
    assert metrics["batch_processing"]["timestamps_written"] == 2
    assert metrics["coverage"][0]["time_index_ranges"] == [[0, 1]]
    assert metrics["coverage"][0]["arrays"] == ["G1/FWI"]


def test_seed_staged_metadata_skips_in_direct_mode() -> None:
    ctx = SimpleNamespace(storage=SimpleNamespace(output=object()))
    logger = MagicMock()

    with patch(
        "firecube.ingestor.runtime.zarr.staged_metadata.seed_staged_store_metadata"
    ) as seed_mock:
        seed_staged_metadata_for_batch(
            ctx=ctx,
            store_uri="file:///tmp/staged.zarr",
            final_target_uri="file:///tmp/final.zarr",
            groups=["F024"],
            resume_existing=True,
            force_reingest=False,
            write_mode="direct",
            logger=logger,
        )

    seed_mock.assert_not_called()


def test_seed_staged_metadata_copies_metadata_and_requested_coordinate_chunks(tmp_path) -> None:
    final_store = tmp_path / "final.zarr"
    staged_store = tmp_path / "staged.zarr"
    group = zarr.open_group(str(final_store), mode="w", zarr_format=3).require_group("data")
    group.create_array("time", shape=(3,), chunks=(3,), dtype="datetime64[s]")
    group["time"][:] = np.array(  # type: ignore[index]
        ["2024-01-01T00:00:00", "2024-01-01T01:00:00", "2024-01-01T02:00:00"],
        dtype="datetime64[s]",
    )
    group.create_array("values", shape=(3,), chunks=(3,), dtype=np.float32)
    group["values"][:] = np.array([1.0, 2.0, 3.0], dtype=np.float32)  # type: ignore[index]
    ctx = SimpleNamespace(storage=SimpleNamespace(output=make_local_session(str(final_store))))

    seed_staged_metadata_for_batch(
        ctx=ctx,
        store_uri=str(staged_store),
        final_target_uri=str(final_store),
        groups=["data"],
        resume_existing=True,
        force_reingest=False,
        write_mode="staged",
        logger=MagicMock(),
        coordinate_arrays=["time"],
    )

    files = {
        path.relative_to(staged_store).as_posix()
        for path in staged_store.rglob("*")
        if path.is_file()
    }
    assert "data/zarr.json" in files
    assert "data/time/zarr.json" in files
    assert "data/time/c/0" in files
    assert "data/values/zarr.json" in files
    assert "data/values/c/0" not in files


def test_seed_staged_metadata_re_raises_staged_metadata_error() -> None:
    ctx = SimpleNamespace(storage=SimpleNamespace(output=object()))
    logger = MagicMock()

    with (
        patch(
            "firecube.ingestor.runtime.zarr.staged_metadata.seed_staged_store_metadata",
            side_effect=StagedMetadataError("boom"),
        ),
        pytest.raises(StagedMetadataError, match="boom"),
    ):
        seed_staged_metadata_for_batch(
            ctx=ctx,
            store_uri="file:///tmp/staged.zarr",
            final_target_uri="file:///tmp/final.zarr",
            groups=["F024"],
            resume_existing=True,
            force_reingest=False,
            write_mode="staged",
            logger=logger,
        )


def test_seed_staged_metadata_swallows_other_exceptions() -> None:
    ctx = SimpleNamespace(storage=SimpleNamespace(output=object()))
    logger = MagicMock()

    with patch(
        "firecube.ingestor.runtime.zarr.staged_metadata.seed_staged_store_metadata",
        side_effect=Exception("boom"),
    ):
        seed_staged_metadata_for_batch(
            ctx=ctx,
            store_uri="file:///tmp/staged.zarr",
            final_target_uri="file:///tmp/final.zarr",
            groups=["F024"],
            resume_existing=True,
            force_reingest=False,
            write_mode="staged",
            logger=logger,
        )

    logger.debug.assert_called_with("Staged metadata seeding skipped: %s", ANY)
