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

import json
import shutil
import threading
from collections.abc import Callable
from contextlib import nullcontext
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import numpy as np
import pytest
import zarr
from zarr.core.buffer import cpu
from zarr.storage import LocalStore

from firecube.core.errors import ConfigurationError, SchemaDriftError
from firecube.core.filesystem.fsspec_backend import FsspecFilesystem
from firecube.core.storage import StorageWriteResult
from firecube.core.storage.completion import StagedPromotionUnits
from firecube.core.storage.session import StorageSession
from firecube.core.storage.uri import StorageUri
from firecube.core.zarr.guard import metadata_authority
from firecube.core.zarr.guard.guarded_write import WriteResult
from firecube.core.zarr.guard.metadata_authority import (
    MetadataAuthority,
    MetadataScope,
    set_attributes,
    update_document,
)
from firecube.core.zarr.guard.unit_ownership import StagedSlotWrite, WriteFootprint
from firecube.ingestor.api import IngestResult, OutputPaths, PipelineMetrics, ResultMetrics
from firecube.ingestor.runtime.engine import PipelineExecutor, _zarr_pre_batch_hook
from firecube.ingestor.runtime.zarr.write_session import WriteSessionStateError, ZarrWriteSession
from firecube.ingestor.templates.direct_zarr import (
    DirectZarrIngestor,
    WriteIntent,
    ZarrArraySpec,
    ZarrGroupSpec,
)
from firecube.ingestor.types.context import (
    ExecutionParams,
    RuntimeIdentity,
    RuntimeIngestContext,
    StorageContext,
)
from firecube.ingestor.types.result_metrics import StorageMetrics
from tests.helpers.storage import begin_write_session, make_test_binding


class _Host:
    name = "completion_probe"


def _ctx(
    tmp_path: Path,
    *,
    product: str = "product.zarr",
    protocol: str = "file",
    authority: str | None = None,
    write_mode: str = "staged",
    upload_workers: int = 4,
) -> RuntimeIngestContext:
    session = StorageSession(
        make_test_binding(
            tmp_path,
            product=product,
            protocol=protocol,
            authority=authority,
        )
    )
    return RuntimeIngestContext(
        source="source",
        target=session.product.product_uri.to_str(),
        output_format="zarr",
        storage=StorageContext(output=session),
        options={"upload_workers": upload_workers},
        execution=ExecutionParams(write_mode=write_mode),
        run_id="completion-run",
        identity=RuntimeIdentity(run_id="completion-run"),
    )


def _result(output_path: str, *, write_mode: str | None = None) -> IngestResult:
    return IngestResult(
        output_format="zarr",
        outputs=OutputPaths(primary=output_path, zarr=output_path),
        metrics=ResultMetrics(
            write_mode=write_mode,
            pipeline=PipelineMetrics(duration_pipeline_s=3.0),
        ),
    )


def test_complete_output_local_staged_in_place_does_not_upload(tmp_path: Path) -> None:
    final_target = tmp_path / "product.zarr"
    final_target.mkdir()
    (final_target / "zarr.json").write_text("{}", encoding="utf-8")

    updated = PipelineExecutor().complete_output(
        _result(str(final_target)),
        _ctx(tmp_path),
        _Host(),  # pyright: ignore[reportArgumentType]
    )

    assert updated.storage_result == StorageWriteResult(
        path=str(final_target.resolve()),
        bytes_written=0,
        files_written=0,
        duration_s=0.0,
        storage_type="local",
    )
    assert updated.write_mode_applied == "staged"
    assert updated.manifest is not None
    assert updated.manifest["stored_at"] == str(final_target.resolve())
    assert updated.manifest["files"] == 0
    assert updated.manifest["bytes"] == 0


def test_complete_output_local_staged_uploads_tree(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = tmp_path / "staged" / "product.zarr"
    source.mkdir(parents=True)
    (source / "zarr.json").write_text("{}", encoding="utf-8")
    observed: dict[str, Any] = {}

    def upload_tree(
        self: StorageSession,
        src: StorageUri,
        dst: StorageUri,
        *,
        parallel_workers: int = 4,
        **kwargs: Any,
    ) -> StorageWriteResult:
        observed["session_target"] = self.product.product_uri.to_str()
        observed["src"] = src.to_str()
        observed["dst"] = dst.to_str()
        observed["parallel_workers"] = parallel_workers
        observed["kwargs"] = kwargs
        return StorageWriteResult(
            path=dst.to_str(),
            bytes_written=11,
            files_written=2,
            duration_s=1.5,
            storage_type="local",
        )

    monkeypatch.setattr(StorageSession, "upload_tree", upload_tree)

    updated = PipelineExecutor().complete_output(
        _result(str(source)),
        _ctx(tmp_path, upload_workers=7),
        _Host(),  # pyright: ignore[reportArgumentType]
    )

    final_target = tmp_path / "product.zarr"
    assert observed == {
        "session_target": StorageUri.from_local_path(final_target).to_str(),
        "src": StorageUri.from_local_path(source.resolve()).to_str(),
        "dst": StorageUri.from_local_path(final_target).to_str(),
        "parallel_workers": 7,
        "kwargs": {},
    }
    assert updated.storage_result is not None
    assert updated.storage_result.path == StorageUri.from_local_path(final_target).to_str()
    assert updated.storage_result.bytes_written == 11
    assert updated.storage_result.files_written == 2


def test_complete_output_s3_direct_uses_storage_summary(tmp_path: Path) -> None:
    result = _result("s3://bucket/product.zarr", write_mode="direct")
    result.metrics.storage = StorageMetrics(
        path="s3://bucket/product.zarr/summary",
        bytes=23,
        files=4,
        duration_s=2.5,
    )

    updated = PipelineExecutor().complete_output(
        result,
        _ctx(tmp_path, protocol="s3", authority="bucket", write_mode="direct"),
        _Host(),  # pyright: ignore[reportArgumentType]
    )

    assert updated.storage_result == StorageWriteResult(
        path="s3://bucket/product.zarr/summary",
        bytes_written=23,
        files_written=4,
        duration_s=2.5,
        storage_type="s3",
    )
    assert updated.write_mode_applied == "direct"
    assert updated.manifest is not None
    assert updated.manifest["stored_at"] == "s3://bucket/product.zarr/summary"


def test_complete_output_s3_staged_uploads_zarr_output(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    primary = tmp_path / "primary"
    primary.mkdir()
    zarr_output = tmp_path / "actual.zarr"
    zarr_output.mkdir()
    (zarr_output / "zarr.json").write_text("{}", encoding="utf-8")
    observed: dict[str, Any] = {}

    def upload_tree(
        self: StorageSession,
        src: StorageUri,
        dst: StorageUri,
        *,
        parallel_workers: int = 4,
        **kwargs: Any,
    ) -> StorageWriteResult:
        observed["session_target"] = self.product.product_uri.to_str()
        observed["src"] = src.to_str()
        observed["dst"] = dst.to_str()
        observed["parallel_workers"] = parallel_workers
        observed["kwargs"] = kwargs
        return StorageWriteResult(
            path=dst.to_str(),
            bytes_written=31,
            files_written=3,
            duration_s=4.0,
            storage_type="s3",
        )

    monkeypatch.setattr(StorageSession, "upload_tree", upload_tree)
    result = IngestResult(
        output_format="zarr",
        outputs=OutputPaths(primary=str(primary), zarr=str(zarr_output)),
        metrics=ResultMetrics(pipeline=PipelineMetrics(duration_pipeline_s=6.0)),
    )

    updated = PipelineExecutor().complete_output(
        result,
        _ctx(tmp_path, protocol="s3", authority="bucket", upload_workers=6),
        _Host(),  # pyright: ignore[reportArgumentType]
    )

    assert observed == {
        "session_target": "s3://bucket/product.zarr",
        "src": StorageUri.from_local_path(zarr_output).to_str(),
        "dst": "s3://bucket/product.zarr",
        "parallel_workers": 6,
        "kwargs": {},
    }
    assert updated.storage_result is not None
    assert updated.storage_result.bytes_written == 31
    assert updated.storage_result.files_written == 3
    assert updated.metrics.storage_handled is True
    assert updated.metrics.pipeline is not None
    assert updated.metrics.pipeline.duration_upload_s == 4.0
    assert updated.metrics.pipeline.duration_total_s == 10.0


# --- staged promotion as an object delta ----------------------------------------------


class _DeltaHost:
    """A staged host that reports its promotion units, as DirectZarr does."""

    name = "completion_probe"

    def __init__(self, units: StagedPromotionUnits | None) -> None:
        self.units = units
        self.calls = 0

    def _staged_promotion_units(self) -> StagedPromotionUnits | None:
        self.calls += 1
        return self.units


def _product(root: Path, values: list[float], *, path: str = "data/values") -> zarr.Array:
    """A product with one ``(len(values), 2)`` float32 array, time chunk 2, fill 0."""
    group = zarr.open_group(str(root), mode="a", zarr_format=3)
    arr = group.require_array(
        path, shape=(len(values), 2), chunks=(2, 2), dtype="float32", fill_value=0.0
    )
    arr[:] = np.repeat(np.asarray(values, dtype=np.float32)[:, None], 2, axis=1)
    return arr


def _stat(root: Path) -> dict[str, tuple[int, int, int]]:
    return {
        p.relative_to(root).as_posix(): (p.stat().st_ino, p.stat().st_mtime_ns, p.stat().st_size)
        for p in sorted(root.rglob("*"))
        if p.is_file()
    }


def _staged_trees(tmp_path: Path) -> tuple[Path, Path]:
    """Target 10..13; workspace rewrites unit 0 to 20, 21 and unit 1 to fill (no object)."""
    target = tmp_path / "product.zarr"
    _product(target, [10.0, 11.0, 12.0, 13.0])
    workspace = tmp_path / "staged" / "product.zarr"
    shutil.copytree(target, workspace)
    arr = _product(workspace, [20.0, 21.0, 0.0, 0.0])
    arr.attrs["run"] = "staged"  # the array document differs: it is published
    assert not (workspace / "data/values/c/1/0").exists()
    return target, workspace


def _units(
    workspace: Path,
    target: Path,
    *,
    written: frozenset[str] = frozenset({"data/values/c/0/0", "data/values/c/1/0"}),
    absent: frozenset[str] = frozenset({"data/values/c/1/0"}),
    publish_metadata: Callable[[str, bytes], None] | None = None,
) -> StagedPromotionUnits:
    def _put(key: str, payload: bytes) -> None:
        (target / key).write_bytes(payload)

    return StagedPromotionUnits(
        workspace=str(workspace),
        target=StorageUri.from_local_path(target).to_str(),
        written=written,
        absent=absent,
        publish_metadata=publish_metadata or _put,
    )


def _refuse_upload_tree(monkeypatch: pytest.MonkeyPatch) -> None:
    def upload_tree(*args: Any, **kwargs: Any) -> StorageWriteResult:
        raise AssertionError("a staged run with promotion units must not upload the tree")

    monkeypatch.setattr(StorageSession, "upload_tree", upload_tree)


@pytest.mark.parametrize(
    ("units_kwargs", "match"),
    [
        pytest.param(
            {"absent": frozenset()},
            r"refused, target untouched: .*data/values/c/1/0",
            id="written-unit-without-absence-evidence",
        ),
        pytest.param(
            {"workspace_override": "elsewhere"},
            r"refused, target untouched: the run's units are relative to",
            id="units-of-another-workspace",
        ),
    ],
)
def test_complete_output_refuses_an_unprovable_delta_before_touching_the_target(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, units_kwargs: dict[str, Any], match: str
) -> None:
    from firecube.core.errors import StorageError

    target, workspace = _staged_trees(tmp_path)
    before = _stat(target)
    other = units_kwargs.pop("workspace_override", None)
    published: list[str] = []
    units = _units(
        tmp_path / other if other else workspace,
        target,
        publish_metadata=lambda key, payload: published.append(key),
        **units_kwargs,
    )
    _refuse_upload_tree(monkeypatch)

    with pytest.raises(StorageError, match=match):
        PipelineExecutor().complete_output(
            _result(str(workspace)),
            _ctx(tmp_path),
            _DeltaHost(units),  # pyright: ignore[reportArgumentType]
        )

    assert _stat(target) == before
    assert published == []


# --- the write session's promotion units ----------------------------------------------


def _session(workspace: Path, target: Path) -> ZarrWriteSession:
    return ZarrWriteSession(
        product="product.zarr",
        run_id="run",
        write_mode="staged",
        window=None,
        slot_group=None,
        acquire_claim=lambda **kwargs: pytest.fail("no claim at promotion"),
        open_root=lambda uri: zarr.open_group(uri, mode="r"),
        open_final_root=lambda ctx: None,
        workspace_uri=str(workspace),
        open_final_store=lambda: (str(target), LocalStore(str(target))),
    )


def _record(
    session: ZarrWriteSession,
    workspace: Path,
    path: str,
    slots: range,
    shape: tuple[int, ...],
    units: tuple[tuple[int, ...], ...],
    absent: tuple[tuple[int, ...], ...],
) -> None:
    """Record slot writes in the ledger and the guarded write's outcome, as a batch does."""
    writes = [StagedSlotWrite(path, slot) for slot in slots]
    session.record_touched(str(workspace), writes)
    session.record_written(str(workspace), writes)
    box = ((slots.start, slots.stop), *((0, n) for n in shape[1:]))
    session.record_write_result(
        WriteResult(WriteFootprint(path, box), units=units, absent=absent, tracked=True),
        store_uri=str(workspace),
    )


def test_session_promotion_units_map_units_to_object_keys(tmp_path: Path) -> None:
    """Chunked, sharded, nested-group and dot-separated keys, relative to the product root."""
    workspace = tmp_path / "ws" / "prefix" / "product.zarr"
    root = zarr.open_group(str(workspace), mode="w", zarr_format=3)
    root.create_array("parent/data/chunked", shape=(4, 2), chunks=(2, 2), dtype="f4")
    root.create_array("parent/data/sharded", shape=(8, 2), chunks=(2, 2), shards=(4, 2), dtype="f4")
    root.create_array(
        "parent/data/dotted",
        shape=(4, 2),
        chunks=(2, 2),
        dtype="f4",
        chunk_key_encoding={"name": "v2", "separator": "."},
    )
    target = tmp_path / "target" / "prefix" / "product.zarr"
    session = _session(workspace, target)
    _record(
        session, workspace, "parent/data/chunked", range(4), (4, 2), ((0, 0), (1, 0)), ((1, 0),)
    )
    _record(session, workspace, "parent/data/sharded", range(4, 8), (8, 2), ((1, 0),), ((1, 0),))
    _record(session, workspace, "parent/data/dotted", range(2), (4, 2), ((0, 0),), ())

    session.verify_promotion()
    units = session.staged_promotion_units()

    assert units is not None
    assert units.workspace == str(workspace)
    assert units.target == str(target)
    assert units.written == {
        "parent/data/chunked/c/0/0",
        "parent/data/chunked/c/1/0",
        "parent/data/sharded/c/1/0",
        "parent/data/dotted/0.0",
    }
    assert units.absent == {"parent/data/chunked/c/1/0", "parent/data/sharded/c/1/0"}


def test_unit_object_key_of_a_root_array_names_the_stored_object(tmp_path: Path) -> None:
    """A Zarr v3 array at the store root has path '', so its key has no leading slash."""
    from firecube.core.zarr.guard.guarded_write import unit_store_key
    from firecube.core.zarr.guard.unit_ownership import _unit_object_key

    arr = zarr.create_array(str(tmp_path / "root.zarr"), shape=(2,), chunks=(2,), dtype="f4")
    arr[:] = 1.0

    key = _unit_object_key(arr, (0,))

    assert key == unit_store_key(arr, (0,))
    assert (tmp_path / "root.zarr" / key).is_file()


def test_session_promotion_units_refuse_a_unit_no_guarded_write_reported(
    tmp_path: Path,
) -> None:
    """A ledger-proved unit the chokepoint never wrote (a stale workspace object) is refused."""
    target, workspace = _staged_trees(tmp_path)
    session = _session(workspace, target)
    _record(session, workspace, "data/values", range(4), (4, 2), ((0, 0),), ())

    session.verify_promotion()
    with pytest.raises(ConfigurationError, match=r"never reported .*data/values/c/1/0"):
        session.staged_promotion_units()


def test_session_promotion_units_need_a_verified_promotion(tmp_path: Path) -> None:
    target, workspace = _staged_trees(tmp_path)
    session = _session(workspace, target)

    with pytest.raises(WriteSessionStateError, match="not verified"):
        session.staged_promotion_units()


def test_session_released_authority_refuses_the_promoted_metadata(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Promotion through the session's publisher; after ``close`` its zarr.json puts are refused."""
    from firecube.core.errors import StorageError

    target, workspace = _staged_trees(tmp_path)
    session = _session(workspace, target)
    _record(session, workspace, "data/values", range(4), (4, 2), ((0, 0), (1, 0)), ((1, 0),))
    session.verify_promotion()
    units = session.staged_promotion_units()
    assert units is not None
    assert units.written == {"data/values/c/0/0", "data/values/c/1/0"}
    assert units.absent == {"data/values/c/1/0"}
    documents = {key: (target / key).read_bytes() for key in _stat(target) if "zarr.json" in key}
    _refuse_upload_tree(monkeypatch)

    session.close()  # releases the authority the publisher is bound to
    with pytest.raises(StorageError, match="was released"):
        PipelineExecutor().complete_output(
            _result(str(workspace)),
            _ctx(tmp_path),
            _DeltaHost(units),  # pyright: ignore[reportArgumentType]
        )

    assert {key: (target / key).read_bytes() for key in documents} == documents


def test_session_publisher_keeps_the_target_attributes_of_an_array(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A workspace shell never drops a target marker; an equal document is not rewritten."""
    target, workspace = _staged_trees(tmp_path)
    marked = zarr.open_array(str(target), path="data/values", mode="r+")
    marked.attrs["firecube_engine_owned"] = True  # lives only in the target
    _product(target, [1.0, 2.0], path="data/other")
    shutil.copytree(target / "data/other", workspace / "data/other")
    other = workspace / "data/other/zarr.json"
    other.write_bytes(json.dumps(json.loads(other.read_bytes())).encode())  # same JSON, new bytes
    before = _stat(target)
    session = _session(workspace, target)
    _record(session, workspace, "data/values", range(4), (4, 2), ((0, 0), (1, 0)), ((1, 0),))
    session.verify_promotion()
    _refuse_upload_tree(monkeypatch)
    data_bytes = (workspace / "data/values/c/0/0").stat().st_size
    document_bytes = (workspace / "data/values/zarr.json").stat().st_size

    updated = PipelineExecutor().complete_output(
        _result(str(workspace)),
        _ctx(tmp_path),
        _DeltaHost(session.staged_promotion_units()),  # pyright: ignore[reportArgumentType]
    )
    session.close()

    values = zarr.open_array(str(target), path="data/values", mode="r")
    assert values.attrs.asdict() == {"firecube_engine_owned": True, "run": "staged"}
    assert np.asarray(values[:, 0]).tolist() == [20.0, 21.0, 0.0, 0.0]
    assert not (target / "data/values/c/1/0").exists()
    assert _stat(target)["data/other/zarr.json"] == before["data/other/zarr.json"]
    result = updated.storage_result
    assert result is not None
    assert (result.files_written, result.bytes_written) == (2, data_bytes + document_bytes)
    assert result.files_deleted == 1


# --- a target grown inside the workspace's terminal unit ------------------------------


def _vector(
    root: Path, values: list[float], *, chunks: int, shards: int | None = None
) -> zarr.Array:
    """A product with one 1-D float32 ``data/values`` array, fill 0, written with *values*."""
    group = zarr.open_group(str(root), mode="a", zarr_format=3)
    arr = group.create_array(
        "data/values",
        shape=(len(values),),
        chunks=(chunks,),
        shards=None if shards is None else (shards,),
        dtype="float32",
        fill_value=0.0,
        overwrite=True,
    )
    arr[:] = np.asarray(values, dtype=np.float32)
    return arr


def _promote_clipped(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    target_values: list[float],
    staged: dict[int, float],
    workspace_len: int,
    chunks: int,
    shards: int | None = None,
) -> tuple[Path, dict[str, tuple[int, int, int]], ConfigurationError | None]:
    """Stage *staged* into a workspace of *workspace_len* slots, then promote it.

    The target was grown to ``len(target_values)`` slots after the workspace
    was seeded. Returns the target, its objects before promotion and the
    refusal, if any.
    """
    target = tmp_path / "product.zarr"
    _vector(target, target_values, chunks=chunks, shards=shards)
    workspace = tmp_path / "staged" / "product.zarr"
    seeded = [0.0] * workspace_len
    for slot, value in staged.items():
        seeded[slot] = value
    _vector(workspace, seeded, chunks=chunks, shards=shards)
    before = _stat(target)
    unit = shards or chunks
    slots = sorted(staged)
    units = tuple(sorted({(slot // unit,) for slot in slots}))
    absent = tuple(u for u in units if not (workspace / "data/values/c" / str(u[0])).exists())
    session = _session(workspace, target)
    _record(
        session,
        workspace,
        "data/values",
        range(slots[0], slots[-1] + 1),
        (workspace_len,),
        units,
        absent,
    )
    session.verify_promotion()
    _refuse_upload_tree(monkeypatch)
    refusal: ConfigurationError | None = None
    try:
        PipelineExecutor().complete_output(
            _result(str(workspace)),
            _ctx(tmp_path),
            _DeltaHost(session.staged_promotion_units()),  # pyright: ignore[reportArgumentType]
        )
    except ConfigurationError as exc:
        refusal = exc
    finally:
        session.close()
    return target, before, refusal


@pytest.mark.parametrize(
    ("staged", "shards", "absent_object"),
    [
        pytest.param({0: 20.0, 1: 21.0}, None, False, id="chunk-put"),
        pytest.param({0: 0.0, 1: 0.0}, None, True, id="chunk-allfill-delete"),
        pytest.param({0: 0.0, 1: 0.0}, 4, True, id="shard-allfill-delete"),
    ],
)
def test_promotion_refuses_a_target_grown_inside_the_terminal_unit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    staged: dict[int, float],
    shards: int | None,
    absent_object: bool,
) -> None:
    """Workspace (2,), target grown to (4,), one four-slot unit: slots 2-3 are not the run's.

    Promoting the unit would replace (or delete) the whole target object and
    erase the target's slots 2 and 3, which the run never wrote.
    """
    target, before, refusal = _promote_clipped(
        tmp_path,
        monkeypatch,
        target_values=[10.0, 11.0, 12.0, 13.0],
        staged=staged,
        workspace_len=2,
        chunks=2 if shards else 4,
        shards=shards,
    )

    values = zarr.open_array(str(target), path="data/values", mode="r")
    assert np.asarray(values[:]).tolist() == [10.0, 11.0, 12.0, 13.0]
    assert _stat(target) == before
    assert refusal is not None
    assert "data/values" in str(refusal) and "data/values/c/0" in str(refusal)
    assert "(2,)" in str(refusal) and "(4,)" in str(refusal)
    workspace_unit = tmp_path / "staged" / "product.zarr" / "data/values/c/0"
    assert workspace_unit.exists() is not absent_object


@pytest.mark.parametrize(
    ("target_values", "staged", "workspace_len", "expected"),
    [
        pytest.param([10.0, 11.0], {0: 20.0, 1: 21.0}, 2, [20.0, 21.0], id="equal-extent-put"),
        pytest.param([10.0, 11.0], {0: 0.0, 1: 0.0}, 2, [0.0, 0.0], id="equal-extent-delete"),
        pytest.param(
            [10.0, 11.0, 12.0, 13.0, 14.0, 15.0, 16.0, 17.0],
            {0: 20.0, 1: 21.0, 2: 22.0, 3: 23.0},
            4,
            [20.0, 21.0, 22.0, 23.0, 14.0, 15.0, 16.0, 17.0],
            id="workspace-ends-on-a-unit-boundary",
        ),
    ],
)
def test_promotion_of_units_the_workspace_extent_does_not_clip(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    target_values: list[float],
    staged: dict[int, float],
    workspace_len: int,
    expected: list[float],
) -> None:
    """A target as long as the workspace, or a unit the workspace holds whole, promotes."""
    target, _, refusal = _promote_clipped(
        tmp_path,
        monkeypatch,
        target_values=target_values,
        staged=staged,
        workspace_len=workspace_len,
        chunks=4,
    )

    assert refusal is None
    values = zarr.open_array(str(target), path="data/values", mode="r")
    assert np.asarray(values[:]).tolist() == expected


# --- metadata publication against an interleaved writer of this process ----------------


class _ReadHookBuffer(cpu.Buffer):
    """A buffer that runs a hook when promotion reads its bytes."""

    on_read: Callable[[], None] | None = None

    def to_bytes(self) -> bytes:
        data = super().to_bytes()
        hook, self.on_read = self.on_read, None
        if hook is not None:
            hook()  # the caller's thread, after the read
        return data


class _ReadHookStore(LocalStore):
    """The target store; the first read of ``data/values/zarr.json`` runs a hook."""

    def __init__(self, root: Path, on_read: Callable[[], None]) -> None:
        super().__init__(root)
        self._on_read: Callable[[], None] | None = on_read

    async def get(self, key: str, prototype: Any = None, byte_range: Any = None) -> Any:
        value = await super().get(key, prototype, byte_range)
        if value is None or key != "data/values/zarr.json" or self._on_read is None:
            return value
        hooked = _ReadHookBuffer.from_bytes(value.to_bytes())
        hooked.on_read, self._on_read = self._on_read, None
        return hooked


def _interleaved_writer(
    target: Path, change: Callable[[MetadataAuthority, LocalStore], object]
) -> tuple[Callable[[], None], Callable[[], None]]:
    """``(on_read, finish)``: a peer thread changes ``data/values`` right after promotion reads it.

    Deterministic: the hook waits until the peer has either committed its
    change or is waiting for the node's lock (two users), so the promotion
    resumes in exactly one of the two orders a lock allows.
    """
    authority = MetadataAuthority.process_local(MetadataScope(str(target.resolve())))
    lock_key = (authority.scope.store, "data/values")
    done = threading.Event()
    errors: list[BaseException] = []

    def peer() -> None:
        try:
            change(authority, LocalStore(target))
        except BaseException as exc:  # reported by finish()
            errors.append(exc)
        finally:
            done.set()

    thread = threading.Thread(target=peer)

    def on_read() -> None:
        thread.start()
        for _ in range(1000):
            entry = metadata_authority._NODE_LOCKS._entries.get(lock_key)
            if done.is_set() or (entry is not None and entry.users >= 2):
                return
            threading.Event().wait(0.01)
        pytest.fail("the interleaved writer neither finished nor waited for the node lock")

    def finish() -> None:
        thread.join(10)
        assert not thread.is_alive() and errors == []

    return on_read, finish


def _grown_workspace_session(
    tmp_path: Path, on_read: Callable[[], None]
) -> tuple[Path, Path, ZarrWriteSession]:
    """Target 10..13 (4 rows); the staged run grows it to 6 rows: 20, 21, fill, fill, 24, 25."""
    target, workspace = _staged_trees(tmp_path)
    grown = zarr.open_array(str(workspace), path="data/values", mode="r+")
    grown.resize((6, 2))
    grown[4:6] = np.asarray([[24.0, 24.0], [25.0, 25.0]], dtype=np.float32)
    session = ZarrWriteSession(
        product="product.zarr",
        run_id="run",
        write_mode="staged",
        window=None,
        slot_group=None,
        acquire_claim=lambda **kwargs: pytest.fail("no claim at promotion"),
        open_root=lambda uri: zarr.open_group(uri, mode="r"),
        open_final_root=lambda ctx: None,
        workspace_uri=str(workspace),
        open_final_store=lambda: (str(target), _ReadHookStore(target, on_read)),
    )
    _record(
        session, workspace, "data/values", range(6), (6, 2), ((0, 0), (1, 0), (2, 0)), ((1, 0),)
    )
    session.verify_promotion()
    return target, workspace, session


def _grow_to_eight_rows(auth: MetadataAuthority, store: LocalStore) -> None:
    def merge(current: bytes | None) -> bytes:
        assert current is not None
        return json.dumps({**json.loads(current), "shape": [8, 2]}).encode()

    update_document(auth, store, "data/values/zarr.json", merge)


@pytest.mark.parametrize(
    ("change", "shape", "attributes"),
    [
        pytest.param(
            _grow_to_eight_rows,
            (8, 2),
            {"run": "staged"},
            id="growth",
        ),
        pytest.param(
            lambda auth, store: set_attributes(auth, store, "data/values", {"stamped": 1}),
            (6, 2),
            {"run": "staged", "stamped": 1},
            id="attribute",
        ),
    ],
)
def test_promotion_never_reverts_a_change_made_after_it_read_the_document(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    change: Callable[[MetadataAuthority, LocalStore], object],
    shape: tuple[int, ...],
    attributes: dict[str, Any],
) -> None:
    """A peer grows the array or stamps an attribute between promotion's read and its put."""
    target = tmp_path / "product.zarr"
    on_read, finish = _interleaved_writer(target, change)
    target, workspace, session = _grown_workspace_session(tmp_path, on_read)
    _refuse_upload_tree(monkeypatch)

    PipelineExecutor().complete_output(
        _result(str(workspace)),
        _ctx(tmp_path),
        _DeltaHost(session.staged_promotion_units()),  # pyright: ignore[reportArgumentType]
    )
    finish()
    session.close()

    values = zarr.open_array(str(target), path="data/values", mode="r")
    assert values.shape == shape
    assert values.attrs.asdict() == attributes
    assert np.asarray(values[:6, 0]).tolist() == [20.0, 21.0, 0.0, 0.0, 24.0, 25.0]


# --- a staged DirectZarr run, written through the real batch path and promoted ---------


class _StagedBatchIngestor(DirectZarrIngestor):
    """A staged DirectZarr host whose next batch writes ``self.intents``."""

    PRODUCT_NAME = "staged_promotion_probe"

    def __init__(self, *, shape: int) -> None:
        chunk_manager = SimpleNamespace(
            storage_config=None, acquire_claim=lambda **_kw: nullcontext()
        )
        super().__init__(name="staged_promotion_probe", chunk_manager=cast(Any, chunk_manager))
        self.shape = shape
        self.intents: list[WriteIntent] = []

    def ingest(self, ctx):  # pragma: no cover - abstract hook not used here
        raise NotImplementedError

    def zarr_schema(self, ctx):
        spec = ZarrArraySpec(
            name="data", shape=(self.shape,), dtype="int64", chunks=(2,), fill_value=-1
        )
        return [ZarrGroupSpec(group="g", arrays=[spec])]

    def build_write_intents(self, batch, ctx):
        return self.intents


def _staged_batch_run(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, *, workspace_values: list[int], shape: int
) -> tuple[_StagedBatchIngestor, Any, Callable[[list[tuple[int, int]]], None], Path, Path]:
    target = tmp_path / "product.zarr"
    workspace = tmp_path / "staged" / "product.zarr"
    for root, values in ((target, [10, 11, 12, 13][: len(workspace_values)]), (workspace, None)):
        array = zarr.open_group(str(root), mode="a", zarr_format=3).create_array(
            "g/data", shape=(len(workspace_values),), chunks=(2,), dtype="int64", fill_value=-1
        )
        array[:] = values if values is not None else workspace_values
    ingestor = _StagedBatchIngestor(shape=shape)
    monkeypatch.setattr(
        ingestor,
        "resolve_output_uri",
        lambda ctx, write_mode: str(workspace if write_mode == "staged" else target),
    )
    ctx = SimpleNamespace(
        run_id="run-1",
        storage=None,
        execution=ExecutionParams(write_mode="staged"),
        option=lambda key, default=None: default,
    )
    session = begin_write_session(ingestor, ctx)

    def run_batch(writes: list[tuple[int, int]]) -> None:
        ingestor.intents = [
            WriteIntent.slot(group="g", array="data", index=slot, data=np.int64(value))
            for slot, value in writes
        ]
        batch = cast(Any, SimpleNamespace(items=["s"], metadata={}))
        result = ingestor._process_batch(batch, cast(Any, ctx))
        assert result.success is True, result.error

    return ingestor, session, run_batch, target, workspace


def _promote(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, session: Any, workspace: Path
) -> None:
    session.verify_promotion()
    _refuse_upload_tree(monkeypatch)
    PipelineExecutor().complete_output(
        _result(str(workspace)),
        _ctx(tmp_path),
        _DeltaHost(session.staged_promotion_units()),  # pyright: ignore[reportArgumentType]
    )


def test_staged_promotion_records_a_negative_index_at_the_slot_it_wrote(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _, session, run_batch, target, workspace = _staged_batch_run(
        tmp_path, monkeypatch, workspace_values=[-1, -1], shape=2
    )
    run_batch([(0, 20), (-1, 99)])
    run_batch([(2, 22)])

    _promote(tmp_path, monkeypatch, session, workspace)
    session.close()

    values = zarr.open_array(str(target), path="g/data", mode="r")
    assert np.asarray(values[:]).tolist() == [20, 99, 22]


def test_staged_promotion_refuses_a_changed_data_chunk_no_write_recorded(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _, session, run_batch, target, workspace = _staged_batch_run(
        tmp_path, monkeypatch, workspace_values=[10, 11, 12, 13], shape=4
    )
    run_batch([(0, 20), (1, 21)])
    unreported = zarr.open_array(str(workspace), path="g/data", mode="r+")
    unreported[2:4] = [30, 31]  # chunk g/data/c/1 changes behind the ledger
    before = _stat(target)

    with pytest.raises(ConfigurationError, match="g/data/c/1"):
        _promote(tmp_path, monkeypatch, session, workspace)
    session.close()

    assert _stat(target) == before


def _seeded_managed_run(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, marker: str, coord_path: str = "g/timestamp"
):
    """A staged run that seeded the target's marked coordinate (slots 0-2 set, 3 NaT)."""
    ingestor, session, run_batch, target, workspace = _staged_batch_run(
        tmp_path, monkeypatch, workspace_values=[-1, -1, -1, -1], shape=4
    )
    ctx = _ctx(tmp_path)
    coord = zarr.open_group(str(target), mode="r+").create_array(
        coord_path, shape=(4,), chunks=(2,), dtype="int64", fill_value=-1
    )
    coord[:3] = [100, 101, 102]
    coord.attrs[marker] = True
    cast(Any, _zarr_pre_batch_hook(ingestor, cast(Any, ctx)))()  # the engine's seeding
    run_batch([(0, 20), (1, 21)])
    return session, target, workspace, coord


@pytest.mark.parametrize("marker", ["firecube_preallocated", "firecube_coord_managed"])
def test_staged_promotion_keeps_a_managed_coordinate_filled_after_seeding(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, marker: str
) -> None:
    session, target, workspace, coord = _seeded_managed_run(tmp_path, monkeypatch, marker)
    coord[3] = 103  # a windowed preallocate fills slot 3 after seeding
    before = _stat(target)

    reads: list[Any] = []
    read_bytes = FsspecFilesystem.read_bytes
    monkeypatch.setattr(
        FsspecFilesystem,
        "read_bytes",
        lambda self, uri, *a, **kw: (reads.append(str(uri)), read_bytes(self, uri, *a, **kw))[1],
    )

    _promote(tmp_path, monkeypatch, session, workspace)
    session.close()

    assert np.asarray(coord[:]).tolist() == [100, 101, 102, 103]
    assert _stat(target)["g/timestamp/c/1"] == before["g/timestamp/c/1"]
    assert not [uri for uri in reads if "g/timestamp/c/" in uri]


def test_staged_promotion_keeps_a_managed_coordinate_of_a_group_the_run_never_wrote(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    session, target, workspace, coord = _seeded_managed_run(
        tmp_path, monkeypatch, "firecube_coord_managed", "h/timestamp"
    )  # the run writes group g only; seeding copied the coordinate of group h
    coord[3] = 103  # a windowed preallocate fills slot 3 of h after seeding
    before = _stat(target)

    _promote(tmp_path, monkeypatch, session, workspace)
    session.close()

    assert np.asarray(coord[:]).tolist() == [100, 101, 102, 103]
    assert _stat(target)["h/timestamp/c/1"] == before["h/timestamp/c/1"]


def test_staged_promotion_refuses_a_coordinate_with_both_markers(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    session, target, workspace, coord = _seeded_managed_run(
        tmp_path, monkeypatch, "firecube_preallocated"
    )
    coord.attrs["firecube_coord_managed"] = True  # an invalid lifecycle state
    before = _stat(target)

    with pytest.raises(SchemaDriftError, match="both"):
        _promote(tmp_path, monkeypatch, session, workspace)
    session.close()

    assert _stat(target) == before
