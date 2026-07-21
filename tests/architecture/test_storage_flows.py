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

from pathlib import Path
from typing import Any, Literal, cast

import numpy as np
import pytest
import xarray as xr
from tests.helpers.storage import make_test_binding, make_test_session

from firecube.core.config import StorageConfig
from firecube.core.controlplane import ChunkManager
from firecube.core.product.identity import ProductIdentity
from firecube.core.storage import StorageWriteResult
from firecube.core.storage.binding import StorageBinding
from firecube.core.storage.driver_config import StorageDriverConfig
from firecube.core.storage.session import StorageSession
from firecube.core.storage.uri import StorageUri
from firecube.ingestor.api import (
    BaseIngestor,
    IngestResult,
    OutputPaths,
    PipelineMetrics,
    ResultMetrics,
)
from firecube.ingestor.runtime.engine import PipelineExecutor
from firecube.ingestor.types.context import (
    PipelineBatch,
    PipelineResult,
    PipelineRunState,
    PluginContext,
    RuntimeIdentity,
    RuntimeIngestContext,
    StorageContext,
)


class _FakeHost:
    name = "storage_flow_probe"


class _DirectProbeIngestor(BaseIngestor):
    PRODUCT_NAME = "direct_probe"
    name = "direct_probe"

    def _process_batch(self, batch: PipelineBatch, ctx: PluginContext) -> PipelineResult:
        _ = ctx
        return PipelineResult(
            batch=batch, outputs=OutputPaths(primary=batch.data_path), success=True
        )

    def _aggregate_metrics(
        self,
        ctx: RuntimeIngestContext,
        state: PipelineRunState,
    ) -> dict[str, Any]:
        _ = (ctx, state)
        return {}


def _storage_config() -> StorageConfig:
    return StorageConfig(storage_type="s3", storage_driver="fsspec")


def _session_for_uri(
    uri: str, *, driver: Literal["fsspec", "obstore"] = "fsspec", format: str = "zarr"
) -> StorageSession:
    product_uri = StorageUri.from_local_path(uri) if "://" not in uri else StorageUri.parse(uri)
    return StorageSession(
        StorageBinding(
            identity=ProductIdentity.from_uri(product_uri, format, product_name="product.zarr"),
            driver=StorageDriverConfig(driver=driver),
        )
    )


def _runtime_ctx(
    *,
    target: str,
    base_uri: str,
    write_mode: str,
) -> RuntimeIngestContext:
    session = _session_for_uri(target if "://" in target else f"{base_uri.rstrip('/')}/{target}")
    return RuntimeIngestContext(
        source="source",
        target=target,
        output_format="zarr",
        storage=StorageContext(output=session),
        options={"write_mode": write_mode, "no_progress": True},
        run_id="storage-flow-run",
        identity=RuntimeIdentity(run_id="storage-flow-run"),
    )


def test_staged_ingest_prefixed_remote_target_preserves_identity(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target = "s3://bucket/data/2026/product.zarr"
    staged_output = tmp_path / "product.zarr"
    staged_output.mkdir()
    (staged_output / "zarr.json").write_text("{}", encoding="utf-8")
    (staged_output / "part.bin").write_bytes(b"abc")
    observed: dict[str, str] = {}

    def _upload_tree(
        self: StorageSession,
        src: StorageUri,
        dst: StorageUri,
        *,
        parallel_workers: int = 4,
        **kwargs: Any,
    ) -> StorageWriteResult:
        _ = kwargs
        observed["upload_source"] = src.to_str()
        observed["upload_target"] = dst.to_str()
        observed["session_product"] = self.product.product_uri.to_str()
        observed["session_control_root"] = self.product.control_root_uri.to_str()
        observed["driver"] = self.driver.driver
        observed["parallel_workers"] = str(parallel_workers)
        return StorageWriteResult(
            path=dst.to_str(),
            bytes_written=5,
            files_written=2,
            duration_s=0.0,
            storage_type="s3",
        )

    monkeypatch.setattr(StorageSession, "upload_tree", _upload_tree)

    result = IngestResult(
        outputs=OutputPaths(primary=str(staged_output)),
        output_format="zarr",
        metrics=ResultMetrics(pipeline=PipelineMetrics(duration_pipeline_s=4.0)),
    )
    updated = PipelineExecutor().complete_output(
        result,
        _runtime_ctx(
            target=target,
            base_uri="s3://bucket/data/2026",
            write_mode="staged",
        ),
        host=cast(Any, _FakeHost()),
    )

    assert observed == {
        "upload_source": StorageUri.from_local_path(staged_output).to_str(),
        "upload_target": target,
        "session_product": target,
        "session_control_root": f"{target}/.firecube",
        "driver": "fsspec",
        "parallel_workers": "4",
    }
    assert updated.storage_result is not None
    assert updated.storage_result.path == target
    assert updated.storage_result.files_written == 2
    assert updated.storage_result.bytes_written == 5
    assert updated.manifest is not None
    assert updated.manifest["stored_at"] == target
    assert updated.output_path == str(staged_output)


def test_direct_ingest_prefixed_remote_target_single_root(tmp_path: Path) -> None:
    target = "s3://bucket/data/2026/product.zarr"
    ctx = _runtime_ctx(
        target=target,
        base_uri="s3://bucket/data/2026",
        write_mode="direct",
    )
    ingestor = cast(Any, _DirectProbeIngestor)(
        chunk_manager=ChunkManager(
            binding=StorageBinding(
                identity=ProductIdentity.from_uri(
                    StorageUri.parse(target), "zarr", product_name="product.zarr"
                ),
                driver=StorageDriverConfig(driver="fsspec"),
            ),
            workspace=tmp_path,
        )
    )

    write_root = ingestor.resolve_output_uri(ctx, write_mode="direct")
    control_root = ingestor._chunk_manager.get_product_root("product.zarr")

    assert write_root == target
    assert control_root == target
    assert ctx.storage is not None
    assert ctx.storage.output is not None
    assert ctx.storage.output.product.control_root_uri.to_str() == f"{target}/.firecube"


def test_poisoned_storage_session_upload_tree_uses_canonical_filesystem(tmp_path: Path) -> None:
    from tests.integration._bt_helpers import bt2_poison

    source = tmp_path / "source.zarr"
    source.mkdir()
    (source / "zarr.json").write_text("{}", encoding="utf-8")
    (source / "part.bin").write_bytes(b"payload")
    target = tmp_path / "target.zarr"
    target.mkdir()
    session = make_test_session(tmp_path, product=target.name)

    with bt2_poison():
        session.upload_tree(StorageUri.from_local_path(source), StorageUri.from_local_path(target))

    assert (target / "zarr.json").read_text(encoding="utf-8") == "{}"
    assert (target / "part.bin").read_bytes() == b"payload"


def test_archive_controlplane_uses_resolved_product_identity(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from firecube.core.tensogram import converter

    target = "s3://bucket/data/2026/product.zarr"
    session = _session_for_uri(target)
    storage_config = _storage_config()
    captured: dict[str, str] = {}

    def _open_dataset(*args: Any, **kwargs: Any) -> xr.Dataset:
        _ = (args, kwargs)
        return xr.Dataset({"counts": (("t",), np.array([1.0], dtype="float32"))})

    def _serialize_controlplane(manager: ChunkManager, product: str, **kwargs: Any) -> Any:
        _ = kwargs
        captured["product"] = product
        captured["base_uri"] = str(manager.base_uri)
        raise RuntimeError("CONTROL_PLANE_CAPTURED")

    monkeypatch.setattr(converter, "_open_group_dataset", _open_dataset)
    monkeypatch.setattr(converter, "_has_controlplane_root", lambda *args, **kwargs: True)
    monkeypatch.setattr(converter, "extract_zarr_array_metadata", lambda *args, **kwargs: {})
    monkeypatch.setattr(converter, "serialize_controlplane", _serialize_controlplane)

    with pytest.raises(RuntimeError, match="CONTROL_PLANE_CAPTURED"):
        converter.zarr_to_tgm(
            target,
            str(tmp_path / "archive.tgm"),
            group="data_1km",
            storage_config=storage_config,
            session=session,
        )

    assert captured == {
        "product": "product.zarr",
        "base_uri": "s3://bucket/data/2026",
    }


def test_upload_workers_propagates_through_engine_to_upload_tree(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Regression: ctx.option('upload_workers') must reach session.upload_tree(parallel_workers=N)."""
    captured: dict[str, int] = {}

    def _spy(
        self: StorageSession,
        src: StorageUri,
        dst: StorageUri,
        *,
        parallel_workers: int = 4,
        **kw: Any,
    ) -> StorageWriteResult:
        _ = (self, src, dst, kw)
        captured["parallel_workers"] = parallel_workers
        return StorageWriteResult(
            path=dst.to_str(),
            bytes_written=0,
            files_written=0,
            duration_s=0.0,
            storage_type="local",
        )

    monkeypatch.setattr(StorageSession, "upload_tree", _spy)

    staged_output = tmp_path / "product.zarr"
    staged_output.mkdir()
    (staged_output / "zarr.json").write_text("{}", encoding="utf-8")

    result = IngestResult(
        outputs=OutputPaths(primary=str(staged_output)),
        output_format="zarr",
        metrics=ResultMetrics(pipeline=PipelineMetrics(duration_pipeline_s=1.0)),
    )
    ctx = _runtime_ctx(
        target=str(tmp_path / "out" / "product.zarr"),
        base_uri=str(tmp_path / "out"),
        write_mode="staged",
    )
    object.__setattr__(
        ctx, "options", {**getattr(ctx, "options", {}), "upload_workers": 8, "no_progress": True}
    )

    updated = PipelineExecutor().complete_output(result, ctx, host=cast(Any, _FakeHost()))
    _ = updated

    assert captured.get("parallel_workers") == 8, f"Expected 8, got {captured}"


def test_bt2_poison_ingest_write_flow(tmp_path: Path) -> None:
    from tests.integration._bt_helpers import bt2_poison

    staged_source = tmp_path / "staged" / "product.zarr"
    staged_source.mkdir(parents=True)
    (staged_source / "zarr.json").write_text("{}", encoding="utf-8")
    (staged_source / "data.bin").write_bytes(b"poison-payload")
    nested = staged_source / "subgroup"
    nested.mkdir()
    (nested / "chunk.bin").write_bytes(b"nested-poison")

    # Target must be a path disjoint from the staged source — otherwise
    # ``_complete_local_storage`` short-circuits via its in-place check and
    # never calls ``session.upload_tree``, which would hide BT2 violations.
    target_root = tmp_path / "out"
    target = target_root / "product.zarr"

    session = make_test_session(target_root, product=target.name)
    ctx = RuntimeIngestContext(
        source="source",
        target=str(target),
        output_format="zarr",
        storage=StorageContext(output=session),
        options={"write_mode": "staged", "no_progress": True},
        run_id="poison-ingest-run",
        identity=RuntimeIdentity(run_id="poison-ingest-run"),
    )
    result = IngestResult(
        outputs=OutputPaths(primary=str(staged_source)),
        output_format="zarr",
        metrics=ResultMetrics(pipeline=PipelineMetrics(duration_pipeline_s=0.1)),
    )

    with bt2_poison():
        updated = PipelineExecutor().complete_output(result, ctx, host=cast(Any, _FakeHost()))

    assert (target / "zarr.json").read_text(encoding="utf-8") == "{}"
    assert (target / "data.bin").read_bytes() == b"poison-payload"
    assert (target / "subgroup" / "chunk.bin").read_bytes() == b"nested-poison"

    expected_bytes = len(b"{}") + len(b"poison-payload") + len(b"nested-poison")
    assert updated.storage_result is not None
    assert updated.storage_result.path == StorageUri.from_local_path(target).to_str()
    assert updated.storage_result.files_written == 3
    assert updated.storage_result.bytes_written == expected_bytes
    assert updated.storage_result.storage_type == "local"
    assert updated.manifest is not None
    assert updated.manifest["stored_at"] == StorageUri.from_local_path(target).to_str()


def _build_local_archive_session(
    product_uri: str,
    target_root: Path,
) -> tuple[StorageConfig, StorageSession]:
    """Build a local-storage StorageSession for archive create/restore tests."""
    session = make_test_session(target_root, product=Path(product_uri).name)
    storage_config = StorageConfig(storage_type="local")
    return storage_config, session


def test_bt2_poison_archive_create(tmp_path: Path) -> None:
    from tests.integration._bt_helpers import bt2_poison

    from firecube.core.tensogram.converter import zarr_to_tgm

    # Build a real (small) zarr source product OUTSIDE the poison context so
    # the archive create flow has a genuine store to read from.
    source_root = tmp_path / "products"
    source_root.mkdir()
    source = source_root / "product.zarr"
    ds = xr.Dataset(
        {"counts": (("t",), np.array([1.0, 2.0, 3.0], dtype="float32"))},
        coords={"t": np.array([0, 1, 2], dtype="int64")},
    )
    ds.to_zarr(str(source), mode="w")

    archive_path = tmp_path / "archive.tgm"
    storage_config, session = _build_local_archive_session(str(source), source_root)

    with bt2_poison():
        result = zarr_to_tgm(
            str(source),
            str(archive_path),
            storage_config=storage_config,
            session=session,
        )

    assert archive_path.exists(), "archive .tgm not produced under bt2_poison"
    assert archive_path.stat().st_size > 0
    assert result["target"] == str(archive_path)
    assert "counts" in result["variables"]


def test_bt2_poison_archive_restore(tmp_path: Path) -> None:
    from tests.integration._bt_helpers import bt2_poison

    from firecube.core.tensogram.converter import zarr_to_tgm
    from firecube.core.tensogram.restore import tgm_to_zarr

    # Stage 1: build source zarr and create archive WITHOUT poison so the
    # restore-only behavior is exercised inside bt2_poison.
    source_root = tmp_path / "products"
    source_root.mkdir()
    source = source_root / "product.zarr"
    ds = xr.Dataset(
        {"counts": (("t",), np.array([1.0, 2.0, 3.0], dtype="float32"))},
        coords={"t": np.array([0, 1, 2], dtype="int64")},
    )
    ds.to_zarr(str(source), mode="w")

    archive_path = tmp_path / "archive.tgm"
    create_storage_config, create_session = _build_local_archive_session(str(source), source_root)
    zarr_to_tgm(
        str(source),
        str(archive_path),
        storage_config=create_storage_config,
        session=create_session,
    )
    assert archive_path.exists()

    # Stage 2: restore under poison into a fresh target root.
    target_root = tmp_path / "restored"
    target_root.mkdir()
    target = target_root / "restored.zarr"
    restore_storage_config, restore_session = _build_local_archive_session(str(target), target_root)

    with bt2_poison():
        restore_result = tgm_to_zarr(
            str(archive_path),
            str(target),
            storage_config=restore_storage_config,
            session=restore_session,
        )

    assert (target / "zarr.json").exists(), "restored zarr.json missing under bt2_poison"
    assert restore_result["target"] == str(target)


def test_bt2_poison_delete_flow(tmp_path: Path) -> None:
    """Abandoning a non-terminal run via the ChunkManager facade must not
    trip BT2 poison: control-plane I/O routes through the approved
    ``firecube.core.filesystem.ops`` module (and downstream control-plane
    helpers) rather than calling ``fsspec.*`` from runtime callers.
    """
    from tests.integration._bt_helpers import bt2_poison

    product = "bt2_delete_product"
    run_id = "run-to-abandon"
    base_uri = str(tmp_path)

    # Set up a non-terminal run under .firecube/ OUTSIDE the poison context
    # so the poisoning only covers the abandon/cleanup flow under test.
    manager = ChunkManager(binding=make_test_binding(Path(base_uri)), workspace=tmp_path)
    try:
        manager.record_run_started(
            product=product,
            run_id=run_id,
            output_path=str(tmp_path / product),
            output_format="zarr",
            size=0,
            meta={"plugin": "test"},
        )

        non_terminal_before = manager.list_runs(product=product, non_terminal=True)
        assert [run.run_id for run in non_terminal_before] == [run_id]
        assert non_terminal_before[0].status == "started"

        with bt2_poison():
            result = manager.abandon_run(
                product=product,
                run_id=run_id,
                reason="bt2-poison-test",
            )

        assert result == {
            "product": product,
            "run_id": run_id,
            "status": "abandoned",
            "abandoned": True,
        }

        runs_after = manager.list_runs(product=product)
        statuses = {run.run_id: run.status for run in runs_after}
        assert statuses == {run_id: "abandoned"}

        assert manager.list_runs(product=product, non_terminal=True) == []
    finally:
        manager.close()


def test_bt2_poison_read_flow(tmp_path: Path) -> None:
    """Reading zarr metadata via ``validate_group_with_fs`` must not trip BT2
    poison: ``firecube.core.zarr.validation`` routes filesystem access through
    the typed ``StorageFilesystem`` adapter rather than calling ``fsspec.*``
    from runtime callers.
    """
    from tests.integration._bt_helpers import bt2_poison

    from firecube.core.zarr.validation import validate_group_with_fs

    source_root = tmp_path / "products"
    source_root.mkdir()
    product = source_root / "product.zarr"
    ds = xr.Dataset(
        {"counts": (("t",), np.array([1.0, 2.0, 3.0], dtype="float32"))},
        coords={"t": np.array([0, 1, 2], dtype="int64")},
    )
    ds.to_zarr(str(product), mode="w")

    session = make_test_session(source_root, product=product.name, driver="fsspec")

    with bt2_poison():
        report = validate_group_with_fs(
            session.fs(),
            session.product.product_uri,
            "counts",
        )

    assert report.product == "product.zarr"
    assert report.group == "counts"
    assert report.shape == [3]
    assert report.extra_chunks == []


# ---------------------------------------------------------------------------
# Obstore-driver variants of the BT2 poison flows above. These ensure the
# obstore I/O path is also free of disallowed ``fsspec.*`` calls when the
# canonical control-plane / read / write seams are exercised. Each variant
# is gated on ``obstore`` being importable so environments without the
# optional dependency simply skip it.
# ---------------------------------------------------------------------------


def test_bt2_poison_ingest_write_flow_obstore(tmp_path: Path) -> None:
    """Obstore variant of :func:`test_bt2_poison_ingest_write_flow`.

    With ``storage_driver="obstore"`` the staged-write seam routes through
    ``ObstoreFilesystem`` (no ``fsspec.*`` calls), so the BT2 poison must
    not fire even though no fsspec call would be allowed from a runtime
    caller.
    """
    from tests.integration._bt_helpers import bt2_poison

    staged_source = tmp_path / "staged" / "product.zarr"
    staged_source.mkdir(parents=True)
    (staged_source / "zarr.json").write_text("{}", encoding="utf-8")
    (staged_source / "data.bin").write_bytes(b"poison-payload")
    nested = staged_source / "subgroup"
    nested.mkdir()
    (nested / "chunk.bin").write_bytes(b"nested-poison")

    target_root = tmp_path / "out"
    target = target_root / "product.zarr"

    session = make_test_session(target_root, product=target.name, driver="obstore")
    ctx = RuntimeIngestContext(
        source="source",
        target=str(target),
        output_format="zarr",
        storage=StorageContext(output=session),
        options={"write_mode": "staged", "no_progress": True},
        run_id="poison-ingest-run-obstore",
        identity=RuntimeIdentity(run_id="poison-ingest-run-obstore"),
    )
    result = IngestResult(
        outputs=OutputPaths(primary=str(staged_source)),
        output_format="zarr",
        metrics=ResultMetrics(pipeline=PipelineMetrics(duration_pipeline_s=0.1)),
    )

    with bt2_poison():
        updated = PipelineExecutor().complete_output(result, ctx, host=cast(Any, _FakeHost()))

    assert (target / "zarr.json").read_text(encoding="utf-8") == "{}"
    assert (target / "data.bin").read_bytes() == b"poison-payload"
    assert (target / "subgroup" / "chunk.bin").read_bytes() == b"nested-poison"

    expected_bytes = len(b"{}") + len(b"poison-payload") + len(b"nested-poison")
    assert updated.storage_result is not None
    assert updated.storage_result.path == StorageUri.from_local_path(target).to_str()
    assert updated.storage_result.files_written == 3
    assert updated.storage_result.bytes_written == expected_bytes
    assert updated.storage_result.storage_type == "local"
    assert updated.manifest is not None
    assert updated.manifest["stored_at"] == StorageUri.from_local_path(target).to_str()


def _build_local_archive_session_obstore(
    product_uri: str,
    target_root: Path,
) -> tuple[StorageConfig, StorageSession]:
    """Same as :func:`_build_local_archive_session` but with the obstore driver."""
    storage_config = StorageConfig(storage_type="local", storage_driver="obstore")
    session = make_test_session(target_root, product=Path(product_uri).name, driver="obstore")
    return storage_config, session


def test_bt2_poison_archive_create_obstore(tmp_path: Path) -> None:
    """Obstore variant of :func:`test_bt2_poison_archive_create`."""
    from tests.integration._bt_helpers import bt2_poison

    from firecube.core.tensogram.converter import zarr_to_tgm

    # Build a real (small) zarr source product OUTSIDE the poison context.
    # Use the default fsspec driver for the source build so we don't depend
    # on the obstore zarr write path here — the test exercises archive
    # creation under poison, which reads the existing store via obstore.
    source_root = tmp_path / "products"
    source_root.mkdir()
    source = source_root / "product.zarr"
    ds = xr.Dataset(
        {"counts": (("t",), np.array([1.0, 2.0, 3.0], dtype="float32"))},
        coords={"t": np.array([0, 1, 2], dtype="int64")},
    )
    ds.to_zarr(str(source), mode="w")

    archive_path = tmp_path / "archive.tgm"
    storage_config, session = _build_local_archive_session_obstore(str(source), source_root)

    with bt2_poison():
        result = zarr_to_tgm(
            str(source),
            str(archive_path),
            storage_config=storage_config,
            session=session,
        )

    assert archive_path.exists(), "archive .tgm not produced under bt2_poison (obstore)"
    assert archive_path.stat().st_size > 0
    assert result["target"] == str(archive_path)
    assert "counts" in result["variables"]


def test_bt2_poison_archive_restore_obstore(tmp_path: Path) -> None:
    """Obstore variant of :func:`test_bt2_poison_archive_restore`."""
    from tests.integration._bt_helpers import bt2_poison

    from firecube.core.tensogram.converter import zarr_to_tgm
    from firecube.core.tensogram.restore import tgm_to_zarr

    # Stage 1: build source zarr and create archive WITHOUT poison so the
    # restore-only behavior is exercised inside bt2_poison.
    source_root = tmp_path / "products"
    source_root.mkdir()
    source = source_root / "product.zarr"
    ds = xr.Dataset(
        {"counts": (("t",), np.array([1.0, 2.0, 3.0], dtype="float32"))},
        coords={"t": np.array([0, 1, 2], dtype="int64")},
    )
    ds.to_zarr(str(source), mode="w")

    archive_path = tmp_path / "archive.tgm"
    create_storage_config, create_session = _build_local_archive_session_obstore(
        str(source), source_root
    )
    zarr_to_tgm(
        str(source),
        str(archive_path),
        storage_config=create_storage_config,
        session=create_session,
    )
    assert archive_path.exists()

    # Stage 2: restore under poison into a fresh target root using obstore.
    target_root = tmp_path / "restored"
    target_root.mkdir()
    target = target_root / "restored.zarr"
    restore_storage_config, restore_session = _build_local_archive_session_obstore(
        str(target), target_root
    )

    with bt2_poison():
        restore_result = tgm_to_zarr(
            str(archive_path),
            str(target),
            storage_config=restore_storage_config,
            session=restore_session,
        )

    assert (target / "zarr.json").exists(), "restored zarr.json missing under bt2_poison (obstore)"
    assert restore_result["target"] == str(target)


def test_bt2_poison_delete_flow_obstore(tmp_path: Path) -> None:
    """Obstore variant of :func:`test_bt2_poison_delete_flow`.

    Even with ``storage_driver="obstore"`` configured on the
    ``ChunkManager``, abandoning a non-terminal run must route through the
    approved control-plane modules and not call ``fsspec.*`` from any
    runtime caller — the obstore path is fully fsspec-free.
    """
    from tests.integration._bt_helpers import bt2_poison

    product = "bt2_delete_product_obstore"
    run_id = "run-to-abandon-obstore"
    base_uri = str(tmp_path)

    manager = ChunkManager(
        binding=make_test_binding(Path(base_uri), driver="obstore"),
        workspace=tmp_path,
    )
    try:
        manager.record_run_started(
            product=product,
            run_id=run_id,
            output_path=str(tmp_path / product),
            output_format="zarr",
            size=0,
            meta={"plugin": "test"},
        )

        non_terminal_before = manager.list_runs(product=product, non_terminal=True)
        assert [run.run_id for run in non_terminal_before] == [run_id]
        assert non_terminal_before[0].status == "started"

        with bt2_poison():
            result = manager.abandon_run(
                product=product,
                run_id=run_id,
                reason="bt2-poison-test-obstore",
            )

        assert result == {
            "product": product,
            "run_id": run_id,
            "status": "abandoned",
            "abandoned": True,
        }

        runs_after = manager.list_runs(product=product)
        statuses = {run.run_id: run.status for run in runs_after}
        assert statuses == {run_id: "abandoned"}

        assert manager.list_runs(product=product, non_terminal=True) == []
    finally:
        manager.close()


def test_bt2_poison_read_flow_obstore(tmp_path: Path) -> None:
    """Obstore variant of :func:`test_bt2_poison_read_flow`.

    With ``storage_driver="obstore"`` the validation read path uses
    ``ObstoreFilesystem`` — which never calls ``fsspec.*`` — so BT2 poison
    must not fire.
    """
    from tests.integration._bt_helpers import bt2_poison

    from firecube.core.zarr.validation import validate_group_with_fs

    source_root = tmp_path / "products"
    source_root.mkdir()
    product = source_root / "product.zarr"
    ds = xr.Dataset(
        {"counts": (("t",), np.array([1.0, 2.0, 3.0], dtype="float32"))},
        coords={"t": np.array([0, 1, 2], dtype="int64")},
    )
    ds.to_zarr(str(product), mode="w")

    session = make_test_session(source_root, product=product.name, driver="obstore")

    with bt2_poison():
        report = validate_group_with_fs(
            session.fs(),
            session.product.product_uri,
            "counts",
        )

    assert report.product == "product.zarr"
    assert report.group == "counts"
    assert report.shape == [3]
    assert report.extra_chunks == []


def test_sessionless_completion_recovers_prefix_from_ctx_target(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Finding 3 regression: programmatic API caller with full prefixed ctx.target but
    bound storage must NOT lose the prefix.
    """
    from firecube.core.storage import StorageWriteResult

    captured: dict[str, str] = {}

    def _spy(self, src, dst, *, parallel_workers: int = 4, **kw):
        captured["dst"] = dst.to_str()
        return StorageWriteResult(
            path=dst.to_str(),
            bytes_written=0,
            files_written=0,
            duration_s=0.0,
            storage_type="s3",
        )

    monkeypatch.setattr(StorageSession, "upload_tree", _spy)

    target = "s3://bucket/data/2026/product.zarr"
    staged = tmp_path / "staged.zarr"
    staged.mkdir()
    (staged / "zarr.json").write_text("{}", encoding="utf-8")

    ctx = _runtime_ctx(
        target=target,
        base_uri="s3://bucket/data/2026",
        write_mode="staged",
    )

    result = IngestResult(
        outputs=OutputPaths(primary=str(staged)),
        output_format="zarr",
        metrics=ResultMetrics(pipeline=PipelineMetrics(duration_pipeline_s=1.0)),
    )
    host: Any = _FakeHost()
    PipelineExecutor().complete_output(result, ctx, host=host)

    # Must preserve full prefix; NOT drop to "s3://bucket/product.zarr"
    assert captured.get("dst") == target, f"Expected {target!r}, got {captured.get('dst')!r}"


def test_local_completion_uses_bound_product_prefix(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """T1 regression: programmatic local caller with relative ctx.target + absolute
    bound product storage must land under its configured product URI, NOT under cwd.
    """
    captured: dict[str, str] = {}

    def _spy(self, src, dst, *, parallel_workers: int = 4, **kw):
        captured["dst"] = dst.to_str()
        return StorageWriteResult(
            path=dst.to_str(),
            bytes_written=0,
            files_written=0,
            duration_s=0.0,
            storage_type="local",
        )

    monkeypatch.setattr(StorageSession, "upload_tree", _spy)

    out_base = tmp_path / "data" / "2026"
    out_base.mkdir(parents=True)
    staged = tmp_path / "staged.zarr"
    staged.mkdir()
    (staged / "zarr.json").write_text("{}", encoding="utf-8")

    ctx = RuntimeIngestContext(
        source="src",
        target=str(out_base / "product.zarr"),
        output_format="zarr",
        storage=StorageContext(output=make_test_session(out_base, product="product.zarr")),
        options={"write_mode": "staged", "no_progress": True},
        run_id="t1-local-prefix",
        identity=RuntimeIdentity(run_id="t1-local-prefix"),
    )

    result = IngestResult(
        outputs=OutputPaths(primary=str(staged)),
        output_format="zarr",
        metrics=ResultMetrics(pipeline=PipelineMetrics(duration_pipeline_s=0.1)),
    )
    host: Any = _FakeHost()
    PipelineExecutor().complete_output(result, ctx, host=host)

    expected = StorageUri.from_local_path(out_base / "product.zarr").to_str()
    assert captured.get("dst") == expected, f"Expected {expected!r}, got {captured.get('dst')!r}"
