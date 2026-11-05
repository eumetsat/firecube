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
    from firecube.core.archive.tensogram import converter

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


# Each BT2 poison flow runs once per driver: the obstore path must be as free of
# disallowed ``fsspec.*`` calls as the fsspec one.
_DRIVERS = ["fsspec", "obstore"]


def _write_counts_zarr(path: Path) -> None:
    ds = xr.Dataset(
        {"counts": (("t",), np.array([1.0, 2.0, 3.0], dtype="float32"))},
        coords={"t": np.array([0, 1, 2], dtype="int64")},
    )
    ds.to_zarr(str(path), mode="w")


def _build_local_archive_session(
    product_uri: str,
    target_root: Path,
    driver: Literal["fsspec", "obstore"],
) -> tuple[StorageConfig, StorageSession]:
    """Build a local-storage StorageSession for archive create/restore tests."""
    session = make_test_session(target_root, product=Path(product_uri).name, driver=driver)
    return StorageConfig(storage_type="local", storage_driver=driver), session


@pytest.mark.parametrize("driver", _DRIVERS)
def test_bt2_poison_ingest_write_flow(tmp_path: Path, driver: Literal["fsspec", "obstore"]) -> None:
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

    session = make_test_session(target_root, product=target.name, driver=driver)
    ctx = RuntimeIngestContext(
        source="source",
        target=str(target),
        output_format="zarr",
        storage=StorageContext(output=session),
        options={"write_mode": "staged", "no_progress": True},
        run_id=f"poison-ingest-run-{driver}",
        identity=RuntimeIdentity(run_id=f"poison-ingest-run-{driver}"),
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


@pytest.mark.parametrize("driver", _DRIVERS)
def test_bt2_poison_archive_create(tmp_path: Path, driver: Literal["fsspec", "obstore"]) -> None:
    from tests.integration._bt_helpers import bt2_poison

    from firecube.core.archive.tensogram.converter import zarr_to_tgm

    # Build a real (small) zarr source product OUTSIDE the poison context with
    # the default fsspec writer, so the archive create flow has a genuine store.
    source_root = tmp_path / "products"
    source_root.mkdir()
    source = source_root / "product.zarr"
    _write_counts_zarr(source)

    archive_path = tmp_path / "archive.tgm"
    storage_config, session = _build_local_archive_session(str(source), source_root, driver)

    with bt2_poison():
        result = zarr_to_tgm(
            str(source),
            str(archive_path),
            storage_config=storage_config,
            session=session,
        )

    assert archive_path.exists(), f"archive .tgm not produced under bt2_poison ({driver})"
    assert archive_path.stat().st_size > 0
    assert result["target"] == str(archive_path)
    assert "counts" in result["variables"]


@pytest.mark.parametrize("driver", _DRIVERS)
def test_bt2_poison_archive_restore(tmp_path: Path, driver: Literal["fsspec", "obstore"]) -> None:
    from tests.integration._bt_helpers import bt2_poison

    from firecube.core.archive.tensogram.converter import zarr_to_tgm
    from firecube.core.archive.tensogram.restore import tgm_to_zarr

    # Stage 1: build source zarr and create archive WITHOUT poison so the
    # restore-only behavior is exercised inside bt2_poison.
    source_root = tmp_path / "products"
    source_root.mkdir()
    source = source_root / "product.zarr"
    _write_counts_zarr(source)

    archive_path = tmp_path / "archive.tgm"
    create_storage_config, create_session = _build_local_archive_session(
        str(source), source_root, driver
    )
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
    restore_storage_config, restore_session = _build_local_archive_session(
        str(target), target_root, driver
    )

    with bt2_poison():
        restore_result = tgm_to_zarr(
            str(archive_path),
            str(target),
            storage_config=restore_storage_config,
            session=restore_session,
        )

    assert (target / "zarr.json").exists(), (
        f"restored zarr.json missing under bt2_poison ({driver})"
    )
    assert restore_result["target"] == str(target)


@pytest.mark.parametrize("driver", _DRIVERS)
def test_zip_restore_writes_through_the_session(tmp_path: Path, monkeypatch, driver) -> None:
    import zarr
    from tests.integration._bt_helpers import bt2_poison

    from firecube.core.archive.zip.archive import zarr_to_zip, zip_to_zarr

    root = tmp_path / "products"
    root.mkdir()
    source, target = root / "product.zarr", tmp_path / "restored.zarr"
    _write_counts_zarr(source)
    xr.Dataset({"inner": (("x",), np.arange(4.0))}, coords={"x": np.arange(4)}).to_zarr(
        str(source), group="nest/deep", mode="a"
    )
    archive = tmp_path / "archive.zip"
    zarr_to_zip(_build_local_archive_session(str(source), root, driver)[1], str(archive))
    session = _build_local_archive_session(str(target), tmp_path, driver)[1]

    def _raw_local_store(*args: Any, **kwargs: Any) -> Any:
        raise RuntimeError("zip restore opened a raw LocalStore, not the session driver")

    monkeypatch.setattr(
        "firecube.core.archive.zip.archive.LocalStore", _raw_local_store, raising=False
    )
    with bt2_poison():
        zip_to_zarr(str(archive), str(target), session=session)  # type: ignore[call-arg]

    def keys(p: Path) -> set[str]:
        return {f.relative_to(p).as_posix() for f in p.rglob("*") if f.is_file()}

    assert keys(target) == {k for k in keys(source) if not k.startswith(".firecube/claims/")}
    differing = [k for k in keys(target) if (target / k).read_bytes() != (source / k).read_bytes()]
    assert differing == [], f"restored bytes differ from the source for {differing}"
    restored = zarr.open_group(str(target), mode="r")
    arrays = dict(zarr.open_group(str(source), mode="r").members(max_depth=None))
    assert len(arrays) >= 4  # counts, t, nest/deep/inner, nest/deep/x
    for name, node in arrays.items():
        if isinstance(node, zarr.Array):
            np.testing.assert_array_equal(restored[name][...], node[...])  # type: ignore[index]


@pytest.mark.parametrize("driver", _DRIVERS)
def test_delta_chain_restore_equals_the_second_release_inventory(tmp_path: Path, driver) -> None:
    import hashlib
    import json
    import shutil

    import zarr

    from firecube.ingestor.runtime.archive import restore_archive
    from firecube.ingestor.runtime.migrate import adopt_product
    from firecube.ingestor.runtime.release import create_release_for_product

    root = tmp_path / "products"
    root.mkdir()
    source = root / "p.zarr"
    _write_counts_zarr(source)
    zarr.open_group(str(source), mode="a").create_array("gone", data=np.arange(3.0))
    group = zarr.open_group(str(source), mode="a")
    group.create_array("grow", data=np.arange(3.0))  # rank 1 now, rank 2 in the second release
    group.create_array("shrink", data=np.ones((3, 2)))  # rank 2 now, rank 1 in the second
    session = _build_local_archive_session(str(source), root, driver)[1]
    adopt_product(session=session)
    archives = [tmp_path / "a1.zip", tmp_path / "a2.zip"]
    create_release_for_product(
        session=session, version="1.0", archive=StorageUri.from_local_path(archives[0])
    )
    shutil.rmtree(source / "gone")
    (source / "counts" / "c" / "0").write_bytes(b"changed chunk")
    zarr.open_group(str(source), mode="a").create_array("new", data=np.arange(3.0))
    shutil.rmtree(source / "grow")  # chunk object grow/c/0 becomes the directory grow/c/0/
    shutil.rmtree(source / "shrink")  # and the directory shrink/c/0/ becomes the object shrink/c/0
    group = zarr.open_group(str(source), mode="a")
    group.create_array("grow", data=np.ones((3, 2)))
    group.create_array("shrink", data=np.arange(3.0))
    second = create_release_for_product(
        session=session, version="1.1", archive=StorageUri.from_local_path(archives[1])
    ).release
    assert second.retention is not None and second.retention["mode"] == "delta"
    inventory = json.loads(
        (
            source / f".firecube/releases/inventories/{second.retention['inventory']}.json"
        ).read_text()
    )
    target = tmp_path / "restored.zarr"
    restore_session = _build_local_archive_session(str(target), tmp_path, driver)[1]
    restore_archive(
        session=restore_session,
        archive=archives[1].as_uri(),
        archive_path=str(archives[1]),
        archive_format="zip",
        target=str(target),
    )

    def digests(p: Path) -> dict[str, str]:
        skipped = (".firecube/claims/", ".firecube/runs/archive-restore-")
        return {
            f.relative_to(p).as_posix(): f"sha256:{hashlib.sha256(f.read_bytes()).hexdigest()}"
            for f in p.rglob("*")
            if f.is_file() and not f.relative_to(p).as_posix().startswith(skipped)
        }

    expected = {o["key"]: o["digest"] for o in inventory["objects"]}
    assert "gone/zarr.json" not in expected and "new/zarr.json" in expected
    assert {"grow/c/0/0", "shrink/c/0"} <= set(expected)
    assert digests(target) == expected


@pytest.mark.parametrize("entry", ["zip_to_zarr", "tgm_to_zarr"])
def test_restore_refuses_a_target_that_is_not_the_session_product(tmp_path: Path, entry) -> None:
    from firecube.core.archive.tensogram.converter import zarr_to_tgm
    from firecube.core.archive.tensogram.restore import tgm_to_zarr
    from firecube.core.archive.zip.archive import zarr_to_zip, zip_to_zarr
    from firecube.core.errors import ConfigurationError

    stores = {n: tmp_path / n / "product.zarr" for n in ("a", "b")}
    for name in stores:
        _write_counts_zarr(stores[name])
        (stores[name] / ".firecube").mkdir()
        (stores[name] / ".firecube" / "marker").write_text(name)
    cfg, session_a = _build_local_archive_session(str(stores["a"]), stores["a"].parent, "fsspec")
    session_b = _build_local_archive_session(str(stores["b"]), stores["b"].parent, "fsspec")[1]
    archive = tmp_path / ("archive.zip" if entry == "zip_to_zarr" else "archive.tgm")
    if entry == "zip_to_zarr":
        zarr_to_zip(session_a, str(archive))
    else:
        zarr_to_tgm(str(stores["a"]), str(archive), storage_config=cfg, session=session_a)

    def snap() -> dict[Path, bytes]:
        return {f: f.read_bytes() for s in stores.values() for f in s.rglob("*") if f.is_file()}

    before = snap()
    restore = zip_to_zarr if entry == "zip_to_zarr" else tgm_to_zarr
    with pytest.raises(ConfigurationError):
        restore(str(archive), str(stores["a"]), session=session_b, overwrite=True)
    assert snap() == before


@pytest.mark.parametrize("driver", _DRIVERS)
def test_bt2_poison_delete_flow(tmp_path: Path, driver: Literal["fsspec", "obstore"]) -> None:
    """Abandoning a non-terminal run via the ChunkManager facade must not
    trip BT2 poison: control-plane I/O routes through the approved
    ``firecube.core.filesystem.ops`` module (and downstream control-plane
    helpers) rather than calling ``fsspec.*`` from runtime callers.
    """
    from tests.integration._bt_helpers import bt2_poison

    product = f"bt2_delete_product_{driver}"
    run_id = f"run-to-abandon-{driver}"

    # Set up a non-terminal run under .firecube/ OUTSIDE the poison context
    # so the poisoning only covers the abandon/cleanup flow under test.
    manager = ChunkManager(binding=make_test_binding(tmp_path, driver=driver), workspace=tmp_path)
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
                reason=f"bt2-poison-test-{driver}",
            )

        assert result == {
            "product": product,
            "run_id": run_id,
            "status": "abandoned",
            "abandoned": True,
            "released_claims": [],
            "skipped_claims": [],
        }

        runs_after = manager.list_runs(product=product)
        statuses = {run.run_id: run.status for run in runs_after}
        assert statuses == {run_id: "abandoned"}

        assert manager.list_runs(product=product, non_terminal=True) == []
    finally:
        manager.close()


@pytest.mark.parametrize("driver", _DRIVERS)
def test_bt2_poison_read_flow(tmp_path: Path, driver: Literal["fsspec", "obstore"]) -> None:
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
    _write_counts_zarr(product)

    session = make_test_session(source_root, product=product.name, driver=driver)

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
