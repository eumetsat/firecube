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

"""Behavioral tests (BT1-BT7) gating the storage-abstraction-unification migration.

See .sisyphus/plans/storage-abstraction-unification.md for full BT IDs.
"""

from __future__ import annotations

import importlib
import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Literal, cast
from unittest.mock import MagicMock, patch

import numpy as np
import pytest
import zarr

from firecube.core.config import StorageConfig, derive_target_uri
from firecube.core.controlplane import ChunkManager
from firecube.core.errors import StorageError
from firecube.core.filesystem.store_factory import create_zarr_store
from firecube.core.storage import StorageWriteResult
from firecube.ingestor.api import BaseIngestor, OutputPaths, PluginContext
from firecube.ingestor.templates.direct_zarr import (
    DirectZarrIngestor,
    WriteIntent,
    ZarrArraySpec,
    ZarrGroupSpec,
)
from firecube.ingestor.types.context import (
    IngestContext,
    IngestResult,
    PipelineBatch,
    PipelineResult,
    RuntimeIngestContext,
    StorageContext,
)
from tests.helpers.storage import make_test_binding, make_test_session

ProductTarget = __import__(
    "firecube.core.product.target",
    fromlist=["ProductTarget"],
).ProductTarget
StorageDriverConfig = importlib.import_module(
    "firecube.core.storage.driver_config"
).StorageDriverConfig
StorageSession = importlib.import_module("firecube.core.storage.session").StorageSession
StorageUri = importlib.import_module("firecube.core.storage.uri").StorageUri


class _BT3StagedUploadIngestor(BaseIngestor):
    PRODUCT_NAME = "bt3_staged_upload"
    name = "bt3_staged_upload"

    def discover_source_files(self, ctx: PluginContext) -> list[str]:
        return [str(ctx.source)]

    def _process_batch(self, batch: PipelineBatch, ctx: PluginContext) -> PipelineResult:
        _ = ctx
        return PipelineResult(
            batch=batch, outputs=OutputPaths(primary=batch.data_path), success=True
        )

    def _aggregate_metrics(self, ctx: RuntimeIngestContext, state) -> dict[str, Any]:
        _ = (ctx, state)
        return {}


def _bt3_statuses(chunk_manager: ChunkManager, product: str, run_id: str) -> list[str]:
    control_root_uri = StorageUri.parse(chunk_manager.get_control_root(product))
    control_root = Path(control_root_uri.path)
    run_dir = control_root / "runs" / run_id
    statuses: list[str] = []
    for event_path in sorted(run_dir.glob("events-*.jsonl")):
        for line in event_path.read_text().splitlines():
            payload = json.loads(line)
            record = payload.get("record") or {}
            status = record.get("status")
            if isinstance(status, str):
                statuses.append(status)
    return statuses


def _prepare_bt3_ingest(
    tmp_path: Path,
    *,
    run_id: str,
    product: str,
) -> tuple[ChunkManager, _BT3StagedUploadIngestor, IngestContext]:
    workspace = tmp_path / run_id
    workspace.mkdir(parents=True, exist_ok=True)
    source_path = workspace / "input.bin"
    source_path.write_bytes(b"bt3")
    chunk_manager = ChunkManager(
        binding=make_test_binding(workspace, product=product),
        workspace=workspace,
    )
    ingestor = _BT3StagedUploadIngestor(chunk_manager=chunk_manager)  # type: ignore[abstract]
    ctx = IngestContext(
        source=str(source_path),
        target=product,
        output_format="zarr",
        options={
            "write_mode": "staged",
            "pipeline_parallel": False,
            "pipeline_batch_size": 1,
            "no_progress": True,
        },
        storage=StorageContext(output=make_test_session(workspace, product=product)),
        run_id=run_id,
    )

    return chunk_manager, ingestor, ctx


def _run_bt3_ingest(
    tmp_path: Path,
    *,
    run_id: str,
    product: str,
    upload_tree,
) -> tuple[ChunkManager, IngestResult | None]:
    chunk_manager, ingestor, ctx = _prepare_bt3_ingest(
        tmp_path,
        run_id=run_id,
        product=product,
    )

    with patch.object(StorageSession, "upload_tree", upload_tree):
        result = ingestor.run(ctx)

    return chunk_manager, result


@pytest.mark.integration
@pytest.mark.parametrize(
    ("target_arg", "expected_product_uri", "expected_output_base_uri", "expected_control_root_uri"),
    [
        pytest.param(
            "s3://test-bucket/data/2026/TEST_PRODUCT.zarr",
            "s3://test-bucket/data/2026/TEST_PRODUCT.zarr",
            "s3://test-bucket/data/2026",
            "s3://test-bucket/data/2026/TEST_PRODUCT.zarr/.firecube",
            id="remote-with-prefix",
        ),
        pytest.param(None, None, None, None, id="local-nested"),
    ],
)
def test_remote_prefix_resolves_to_consistent_roots(
    tmp_path: Path,
    target_arg: str | None,
    expected_product_uri: str | None,
    expected_output_base_uri: str | None,
    expected_control_root_uri: str | None,
) -> None:
    if target_arg is None:
        local_target = tmp_path / "data" / "TEST_PRODUCT.zarr"
        target_arg = StorageUri.from_local_path(local_target).to_str()
        expected_product_uri = StorageUri.from_local_path(local_target).to_str()
        expected_output_base_uri = StorageUri.from_local_path(local_target.parent).to_str()
        expected_control_root_uri = StorageUri.from_local_path(local_target / ".firecube").to_str()

    storage_config = StorageConfig(storage_type="s3")
    storage_config.bucket = "test-bucket"  # type: ignore[attr-defined]

    resolved = ProductTarget.resolve(
        target_arg,
        StorageDriverConfig.from_storage_config(storage_config),
        product_name="TEST_PRODUCT",
        plugin_default_format="zarr",
        default_base_uri=StorageUri.parse(derive_target_uri(storage_config)),
    )

    assert expected_product_uri is not None
    assert resolved.product_uri.to_str() == expected_product_uri
    assert resolved.output_base_uri.to_str() == expected_output_base_uri
    assert resolved.control_root_uri.to_str() == expected_control_root_uri
    assert resolved.product_name == "TEST_PRODUCT"
    assert resolved.format == "zarr"


class _BT5DirectZarrIngestor(DirectZarrIngestor):
    PRODUCT_NAME = "bt5_direct_zarr"
    name = "bt5_direct_zarr"

    def zarr_schema(self, ctx: PluginContext) -> list[ZarrGroupSpec]:
        return [
            ZarrGroupSpec(
                group="data_1km",
                arrays=[
                    ZarrArraySpec(
                        name="counts",
                        shape=(0, 100, 100),
                        dtype=np.float32,
                        chunks=(1, 100, 100),
                    ),
                ],
            )
        ]

    def build_write_intents(self, batch, ctx: PluginContext) -> list[WriteIntent]:
        return [
            WriteIntent(
                group="data_1km",
                array="counts",
                ts_index=0,
                data=np.ones((10, 100), dtype=np.float32),
                kind="region",
                y_slice=slice(0, 10),
            )
        ]


@pytest.mark.integration
@pytest.mark.parametrize("storage_driver", ["fsspec", "obstore"])
def test_direct_zarr_receives_storage_config_obstore_branch(
    tmp_path: Path,
    storage_driver: Literal["fsspec", "obstore"],
) -> None:
    ingestor = _BT5DirectZarrIngestor()  # type: ignore[abstract]
    session = make_test_session(tmp_path, product=f"{storage_driver}.zarr", driver=storage_driver)
    ingestor._chunk_manager = ChunkManager(  # type: ignore[attr-defined]
        binding=make_test_binding(
            tmp_path, product=f"{storage_driver}.zarr", driver=storage_driver
        ),
        workspace=tmp_path,
    )
    ingestor.engine_config = SimpleNamespace(  # type: ignore[assignment]
        write_mode="direct", slot_group=None, slot_start=None, slot_end=None
    )

    runtime_ctx = RuntimeIngestContext(
        source=str(tmp_path / "input.nc"),
        target=session.product.product_uri.to_str(),
        output_format="zarr",
        options={"write_mode": "direct", "run_id": "run-001"},
        storage=StorageContext(output=session),
        run_id="run-001",
    )
    runtime_ctx.temp_root = tmp_path
    ctx = PluginContext(runtime_ctx)
    batch = PipelineBatch(batch_id="batch-001", data_path=tmp_path, items=["input.nc"])

    result = ingestor._process_batch(batch, ctx)

    assert result.success is True
    assert result.outputs.zarr == session.product.product_uri.to_str()

    handle = create_zarr_store(
        uri=session.product.product_uri.to_str(),
        storage_config=StorageConfig(storage_type="local", storage_driver=storage_driver),
        mode="r",
    )
    root = zarr.open_group(**handle.zarr_kwargs(), mode="r", zarr_format=3)
    counts = cast("zarr.Array", root["data_1km/counts"])
    assert counts.shape == (1, 100, 100)
    np.testing.assert_array_equal(
        np.asarray(counts[0, :10, :]),
        np.ones((10, 100), dtype=np.float32),
    )


@pytest.mark.integration
def test_staged_upload_failure_no_complete_wal(tmp_path: Path) -> None:
    failed_product = "bt3_failed_product.zarr"
    failed_run_id = "bt3-failed-run"
    failed_chunk_manager, failed_ingestor, failed_ctx = _prepare_bt3_ingest(
        tmp_path,
        run_id=failed_run_id,
        product=failed_product,
    )
    failed_targets: list[str] = []

    def _failing_upload_tree(self, src, dst, *, parallel_workers=4, **kwargs) -> None:
        _ = (self, src, parallel_workers, kwargs)
        failed_targets.append(dst.to_str())
        raise StorageError("simulated staged upload failure")

    with (
        patch.object(StorageSession, "upload_tree", _failing_upload_tree),
        pytest.raises(StorageError, match="simulated staged upload failure"),
    ):
        failed_ingestor.run(failed_ctx)

    assert failed_targets == [
        StorageUri.from_local_path(tmp_path / failed_run_id / failed_product).to_str()
    ]
    failed_statuses = _bt3_statuses(failed_chunk_manager, failed_product, failed_run_id)
    assert failed_statuses[-1] == "failed"
    assert "complete" not in failed_statuses
    failed_runs = failed_chunk_manager.list_runs(product=failed_product, status="failed")
    assert [run.run_id for run in failed_runs] == [failed_run_id]

    success_product = "bt3_success_product.zarr"
    success_run_id = "bt3-success-run"
    success_targets: list[str] = []

    def _successful_upload_tree(
        self,
        src,
        dst,
        *,
        parallel_workers=4,
        **kwargs,
    ) -> StorageWriteResult:
        _ = (self, parallel_workers, kwargs)
        assert Path(StorageUri.parse(src.to_str()).path).exists()
        success_targets.append(dst.to_str())
        return StorageWriteResult(
            path=dst.to_str(),
            bytes_written=0,
            files_written=1,
            duration_s=0.0,
            storage_type="local",
        )

    success_chunk_manager, success_result = _run_bt3_ingest(
        tmp_path,
        run_id=success_run_id,
        product=success_product,
        upload_tree=_successful_upload_tree,
    )

    assert success_result is not None
    assert success_result.registered is True
    assert success_targets == [
        StorageUri.from_local_path(tmp_path / success_run_id / success_product).to_str()
    ]
    success_statuses = _bt3_statuses(success_chunk_manager, success_product, success_run_id)
    assert success_statuses[-1] == "complete"
    assert "failed" not in success_statuses


@pytest.mark.integration
def test_archive_create_restore_honors_endpoint_and_credentials(tmp_path: Path) -> None:
    """BT4: tensogram code paths thread the user's StorageConfig (with non-default
    endpoint + credentials) through the canonical storage abstractions on archive
    create, archive restore, and control-plane restore.

    Pre-W0.6 these sites silently swallowed an ``AttributeError`` and dropped the
    config, causing remote zarr opens/writes to fall back to AMBIENT AWS creds and
    the default endpoint. This test spies on ``create_zarr_store`` and ``open_fs``
    to prove the configured ``StorageConfig`` (NOT ambient defaults) is what reaches
    the seam at every site.
    """
    import xarray as xr

    from firecube.core.tensogram.controlplane_codec import restore_controlplane
    from firecube.core.tensogram.converter import zarr_to_tgm
    from firecube.core.tensogram.restore import tgm_to_zarr

    storage_config = StorageConfig(
        storage_type="s3",
        endpoint_url="https://fake-s3.bt4.example.invalid:9000",
        access_key="BT4_ACCESS_KEY",
        secret_key="BT4_SECRET_KEY",
        region="bt4-region",
    )
    storage_config.bucket = "bt4-test-bucket"  # type: ignore[attr-defined]

    # Part A: zarr_to_tgm (archive create, remote source) -> create_zarr_store
    create_calls_a: list[dict[str, Any]] = []

    def spy_create_zarr_store_a(*, uri: str, storage_config: Any, mode: str = "w") -> Any:
        create_calls_a.append({"uri": uri, "storage_config": storage_config, "mode": mode})
        raise RuntimeError("BT4_PART_A_SPY")

    with (
        patch(
            "firecube.core.filesystem.store_factory.create_zarr_store",
            spy_create_zarr_store_a,
        ),
        pytest.raises(RuntimeError, match="BT4_PART_A_SPY"),
    ):
        zarr_to_tgm(
            source="s3://bt4-test-bucket/source.zarr",
            target=str(tmp_path / "out.tgm"),
            storage_config=storage_config,
        )

    assert create_calls_a, "create_zarr_store was never invoked from zarr_to_tgm"
    assert create_calls_a[0]["uri"] == "s3://bt4-test-bucket/source.zarr"
    assert create_calls_a[0]["mode"] == "r"
    cfg_a = create_calls_a[0]["storage_config"]
    assert cfg_a is not None
    assert cfg_a.endpoint_url == storage_config.endpoint_url
    assert cfg_a.access_key == storage_config.access_key
    assert cfg_a.secret_key == storage_config.secret_key
    assert cfg_a.region == storage_config.region

    # Part B: restore_controlplane (control-plane restore, remote target) -> create_filesystem.
    create_fs_calls: list[dict[str, Any]] = []

    def spy_create_filesystem(binding: Any) -> Any:
        create_fs_calls.append({"binding": binding})
        return MagicMock()

    with patch(
        "firecube.core.filesystem.create_filesystem",
        spy_create_filesystem,
    ):
        restore_controlplane(
            state={"product": "bt4_product", "spans": [], "runs": []},
            target_path="s3://bt4-test-bucket/restored.zarr",
            storage_config=storage_config,
        )

    assert create_fs_calls, "create_filesystem was never invoked from restore_controlplane"
    binding_b = create_fs_calls[0]["binding"]
    assert binding_b.identity.product_uri.to_str() == "s3://bt4-test-bucket/restored.zarr"
    assert binding_b.driver.endpoint_url == storage_config.endpoint_url
    assert binding_b.driver.region == storage_config.region
    creds_b = binding_b.driver.credentials
    assert creds_b is not None
    assert creds_b.access_key == storage_config.access_key
    assert creds_b.secret_key == storage_config.secret_key

    # Part C: tgm_to_zarr (archive restore, remote target) -> create_zarr_store
    src_zarr = tmp_path / "src.zarr"
    ds = xr.Dataset({"X": (("t",), np.array([1.0, 2.0, 3.0], dtype="float32"))})
    ds.to_zarr(str(src_zarr))
    tgm_path = tmp_path / "real.tgm"
    zarr_to_tgm(str(src_zarr), str(tgm_path))

    create_calls_c: list[dict[str, Any]] = []

    def spy_create_zarr_store_c(*, uri: str, storage_config: Any, mode: str = "w") -> Any:
        create_calls_c.append({"uri": uri, "storage_config": storage_config, "mode": mode})
        raise RuntimeError("BT4_PART_C_SPY")

    with (
        patch(
            "firecube.core.filesystem.store_factory.create_zarr_store",
            spy_create_zarr_store_c,
        ),
        pytest.raises(RuntimeError, match="BT4_PART_C_SPY"),
    ):
        tgm_to_zarr(
            source=str(tgm_path),
            target="s3://bt4-test-bucket/restored-via-tgm.zarr",
            storage_config=storage_config,
        )

    assert create_calls_c, "create_zarr_store was never invoked from tgm_to_zarr"
    assert create_calls_c[0]["uri"] == "s3://bt4-test-bucket/restored-via-tgm.zarr"
    cfg_c = create_calls_c[0]["storage_config"]
    assert cfg_c is not None
    assert cfg_c.endpoint_url == storage_config.endpoint_url
    assert cfg_c.access_key == storage_config.access_key
    assert cfg_c.secret_key == storage_config.secret_key
    assert cfg_c.region == storage_config.region


@pytest.mark.integration
def test_obstore_duckdb_remote_hard_error() -> None:
    from firecube.core.product.identity import ProductIdentity
    from firecube.core.storage.binding import StorageBinding

    remote_resolved = ProductTarget.resolve(
        "s3://test-bucket/data.parquet",
        StorageDriverConfig(driver="obstore"),
        product_name="data",
        plugin_default_format="parquet",
    )
    session = StorageSession(
        StorageBinding(
            identity=ProductIdentity(
                product_name=remote_resolved.product_name,
                product_uri=remote_resolved.product_uri,
                control_root_uri=remote_resolved.control_root_uri,
                format=remote_resolved.format,
            ),
            driver=StorageDriverConfig(driver="obstore"),
        )
    )
    con = MagicMock()

    with pytest.raises(
        RuntimeError,
        match=(
            "DuckDB remote parquet is not supported under storage_driver=obstore\\. "
            "Re-run this command with --storage-driver=fsspec\\."
        ),
    ):
        session.duckdb.apply(con)

    local_resolved = ProductTarget.resolve(
        "/tmp/data.parquet",
        StorageDriverConfig(driver="obstore"),
        product_name="data",
        plugin_default_format="parquet",
    )
    local_session = StorageSession(
        StorageBinding(
            identity=ProductIdentity(
                product_name=local_resolved.product_name,
                product_uri=local_resolved.product_uri,
                control_root_uri=local_resolved.control_root_uri,
                format=local_resolved.format,
            ),
            driver=StorageDriverConfig(driver="obstore"),
        )
    )

    assert local_session.duckdb.apply(con) is None
