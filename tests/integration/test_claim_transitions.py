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

"""Claim transitions under forced interleavings, on local targets for both drivers.

The ``scenario_*`` functions are shared with the S3 cases of ``test_obstore_claim_atomicity.py``.
"""

# pyright: reportPrivateUsage=false, reportAttributeAccessIssue=false

from __future__ import annotations

import json
import threading
import time
from collections.abc import Callable, Sequence
from contextlib import suppress
from pathlib import Path
from typing import Any, cast
from unittest.mock import Mock

import numpy as np
import pandas as pd
import pyarrow as pa
import pytest
import xarray as xr
import zarr

from firecube.core.controlplane import ChunkManager
from firecube.core.controlplane.claims import (
    ClaimHandle,
    ClaimLossLatch,
    FilesystemClaimService,
    reporting_claim_loss,
)
from firecube.core.controlplane.types import CLAIMS_DIRNAME, CONTROL_DIRNAME, WriteDomain
from firecube.core.errors import ClaimConflictError, ControlPlaneCorruptionError
from firecube.core.filesystem import FsspecFilesystem, ObstoreFilesystem
from firecube.core.index_spec import IndexSpec, ItemInfo, RegularTimeAxis
from firecube.core.storage.binding import StorageBinding
from firecube.core.storage.uri import StorageUri
from firecube.ingestor.api import (
    DirectZarrIngestor,
    GenericParquetIngestor,
    GenericZarrIngestor,
    PluginContext,
    WriteIntent,
    ZarrArraySpec,
    ZarrGroupSpec,
)
from firecube.ingestor.runtime.zarr import append as append_module
from firecube.ingestor.runtime.zarr.append import append_time_groups
from firecube.ingestor.runtime.zarr.write_session import ZarrWriteSession
from tests.helpers.storage import (
    local_zarr_handle,
    make_local_session,
    make_test_binding,
    make_test_context,
)

pytestmark = [pytest.mark.integration, pytest.mark.concurrency]

PRODUCT = "product.zarr"
HEARTBEAT_S = 1
WAIT_S = 20
READS = {"read_bytes", "read_snapshot"}
WRITES = {"rm", "write_atomic", "replace_atomic", "create_if_absent", "compare_replace"} | {
    "compare_delete"
}


class Gate:
    """Holds the heartbeat thread of one owner at a write operation until the test opens it."""

    def __init__(self, owner: str, *, after_open: bool = False) -> None:
        self.owner, self.after_open, self.stuck = owner, after_open, False
        self.reached, self.done = threading.Event(), threading.Event()
        self._held = threading.Lock()
        self._held.acquire()

    def arrive(self) -> None:
        self.reached.set()
        if not self._held.acquire(timeout=WAIT_S):
            self.stuck = True
            raise AssertionError("gate was never opened; the interleaving was not forced")
        self._held.release()

    def open(self) -> None:
        self.done.clear()
        with suppress(RuntimeError):
            self._held.release()


class _LandsOnClose:
    """A file opened for writing; ``landed`` is called once it is closed."""

    def __init__(self, handle: Any, landed: Callable[[], None]) -> None:
        self._handle, self._landed = handle, landed

    def __enter__(self) -> Any:
        return self._handle.__enter__()

    def __exit__(self, *exc: Any) -> Any:
        try:
            return self._handle.__exit__(*exc)
        finally:
            self._landed()

    def __getattr__(self, name: str) -> Any:
        return getattr(self._handle, name)


class HookedFs:
    """A driver seen at its Python operation boundary: reads, writes and the atomic writer."""

    def __init__(self, real: Any, env: ClaimEnv) -> None:
        self._real, self._env = real, env

    def __getattr__(self, name: str) -> Any:
        attr = getattr(self._real, name)
        if name == "atomic_writer":
            return HookedFs(attr, self._env)
        if not callable(attr) or name not in READS | WRITES | {"open"}:
            return attr

        def call(*args: Any, **kwargs: Any) -> Any:
            mode = args[1] if len(args) > 1 else kwargs.get("mode", "r")
            writing = any(c in mode for c in "wax+") if name == "open" else name in WRITES
            deferred = name == "open" and writing
            return self._env.around(writing, lambda: attr(*args, **kwargs), deferred=deferred)

        return call


class ClaimEnv:
    """One product on one driver, with gates on heartbeat writes and a hook after a read."""

    def __init__(self, binding: StorageBinding, control_for: Callable[[str], StorageUri]) -> None:
        backend = FsspecFilesystem if binding.driver.driver == "fsspec" else ObstoreFilesystem
        self.fs: Any = backend.from_binding(binding)
        self.control_for = control_for
        self.gates: list[Gate] = []
        self.read_callback: Callable[[], None] | None = None

    def service(self, *, hb: int, stale: int) -> FilesystemClaimService:
        def resolve(product: str) -> tuple[StorageUri, StorageUri]:
            return self.control_for(product), self.control_for(product)

        resolve.base_uri = self.control_for("")  # type: ignore[attr-defined]
        hooked: Any = HookedFs(self.fs, self)
        return FilesystemClaimService(
            fs=hooked,
            control_root_resolver=resolve,
            heartbeat_interval_s=hb,
            stale_threshold_s=stale,
        )

    def domain(self, name: str) -> WriteDomain:
        return WriteDomain(product=PRODUCT, category="zarr_region", name=name)

    def hold(self, owner: str, *, after_open: bool = False) -> Gate:
        self.gates.append(Gate(owner, after_open=after_open))
        return self.gates[-1]

    def after_next_read(self, callback: Callable[[], None]) -> list[int]:
        """Run ``callback`` once a service read has returned, before its compare operation starts."""
        fired: list[int] = []
        self.read_callback = lambda: (fired.append(1), callback())[1]
        return fired

    def around(self, writing: bool, operation: Callable[[], Any], *, deferred: bool = False) -> Any:
        name = threading.current_thread().name
        gate = next(
            (g for g in self.gates if writing and name.startswith(f"firecube-claim-{g.owner}")),
            None,
        )
        if gate and not gate.after_open:
            gate.arrive()
        try:
            result = operation()
        except BaseException:
            if gate:
                gate.done.set()
            raise
        if gate and deferred:  # an opened file lands its bytes at close, not at open
            result = _LandsOnClose(result, gate.done.set)
        elif gate:
            gate.done.set()
        if gate and gate.after_open:
            gate.arrive()
        callback = self.read_callback
        if callback and not writing and not name.startswith("firecube-claim-"):
            self.read_callback = None
            callback()
        return result


@pytest.fixture(params=["fsspec", "obstore"])
def local_env(request: pytest.FixtureRequest, tmp_path: Path) -> ClaimEnv:
    binding = make_test_binding(tmp_path, product=PRODUCT, driver=request.param)
    env = ClaimEnv(
        binding, lambda product: StorageUri.from_local_path(tmp_path / product / CONTROL_DIRNAME)
    )
    env.claims_dir = tmp_path / PRODUCT / CONTROL_DIRNAME / CLAIMS_DIRNAME  # type: ignore[attr-defined]
    return env


def owner_of(service: FilesystemClaimService, domain: WriteDomain) -> str | None:
    claim = service.read_claim_by_domain(product=PRODUCT, domain=domain.identifier)
    return None if claim is None else claim.owner_id


def release_all(handles: list[ClaimHandle]) -> None:
    for handle in handles:
        with suppress(Exception):
            handle.release()


def scenario_heartbeat_after_removal(env: Any, *, successor: bool) -> None:
    """A heartbeat held in flight lands after its claim was force-cleared, and maybe retaken."""
    service = env.service(hb=HEARTBEAT_S, stale=120)
    domain = env.domain("late-heartbeat")
    gate = env.hold("A")
    handles: list[ClaimHandle] = []
    try:
        handles.append(service.acquire(product=PRODUCT, domain=domain, owner_id="A"))
        assert gate.reached.wait(WAIT_S), "heartbeat of A never reached its write"
        assert service.clear_claim(product=PRODUCT, domain_id=domain.identifier, force=True)
        if successor:
            handles.append(service.acquire(product=PRODUCT, domain=domain, owner_id="B"))
        gate.open()
        assert gate.done.wait(WAIT_S), "the held write of A never returned"
        owner = owner_of(service, domain)
    finally:
        gate.open()
        release_all(handles)
    assert not gate.stuck
    assert owner == ("B" if successor else None)


def scenario_release_with_inflight_heartbeat(env: Any) -> None:
    """release() runs while a heartbeat write is held. D10: it raises ClaimReleaseError and leaves
    the record alone, or it succeeds and the held write must not bring the claim back."""
    service = env.service(hb=HEARTBEAT_S, stale=1)
    domain = env.domain("release-inflight")
    gate = env.hold("A")
    errors: list[Exception] = []
    handle = service.acquire(product=PRODUCT, domain=domain, owner_id="A")
    try:
        assert gate.reached.wait(WAIT_S), "heartbeat of A never reached its write"

        def release() -> None:
            try:
                handle.release()
            except Exception as exc:
                errors.append(exc)

        releaser = threading.Thread(target=release)
        releaser.start()
        releaser.join(timeout=WAIT_S)
        assert not releaser.is_alive(), "release never returned"
        held = service.read_claim_by_domain(product=PRODUCT, domain=domain.identifier)
        gate.open()
        assert gate.done.wait(WAIT_S), "the held write of A never returned"
        owner = owner_of(service, domain)
    finally:
        gate.open()
        release_all([handle])
    assert not gate.stuck
    if errors:
        assert [type(exc).__name__ for exc in errors] == ["ClaimReleaseError"], errors
        assert held is not None and held.acquired_at == handle.info.acquired_at
        assert owner == "A"
    else:
        assert owner is None, f"release reported success, yet {owner!r} holds the claim"


def scenario_stale_handle_release(env: Any) -> None:
    """A handle whose claim was cleared and retaken releases late."""
    service = env.service(hb=3600, stale=120)
    domain = env.domain("stale-release")
    handles = [service.acquire(product=PRODUCT, domain=domain, owner_id="A")]
    try:
        assert service.clear_claim(product=PRODUCT, domain_id=domain.identifier, force=True)
        handles.append(service.acquire(product=PRODUCT, domain=domain, owner_id="B"))
        with suppress(Exception):  # whether a lost handle's release raises is not specified
            handles[0].release()
        owner = owner_of(service, domain)
    finally:
        release_all(handles)
    assert owner == "B"


def scenario_successor_between_read_and_delete(env: Any, operation: str) -> None:
    """A successor acquires after the sweep read the claim and before it deletes it."""
    service = env.service(hb=3600, stale=0)
    domain = env.domain("read-then-delete")
    handles = [service.acquire(product=PRODUCT, domain=domain, owner_id="A")]
    try:
        listed = service.list_product_claims(product=PRODUCT)[0]

        def successor() -> None:
            service.clear_claim(product=PRODUCT, domain_id=domain.identifier, force=True)
            handles.append(service.acquire(product=PRODUCT, domain=domain, owner_id="B"))

        fired = env.after_next_read(successor)
        if operation == "clear_claim":
            with suppress(ClaimConflictError):
                service.clear_claim(product=PRODUCT, domain_id=domain.identifier)
        else:
            outcome = service.release_claim_if_unchanged(
                product=PRODUCT, claim=listed, stale_only=True
            )
            assert outcome == "changed"
        assert fired, "no claim read happened for the hook to follow"
        owner = owner_of(service, domain)
    finally:
        release_all(handles)
    assert owner == "B"


def test_claim_read_during_heartbeat_write_returns_the_claim(local_env: ClaimEnv) -> None:
    service = local_env.service(hb=HEARTBEAT_S, stale=120)
    domain = local_env.domain("heartbeat-read")
    gate = local_env.hold("holder", after_open=True)
    handle = service.acquire(product=PRODUCT, domain=domain, owner_id="holder")
    try:
        assert gate.reached.wait(WAIT_S), "heartbeat never opened the claim for writing"
        by_domain = owner_of(service, domain)
        listed = [claim.owner_id for claim in service.list_claims(product=PRODUCT)]
    finally:
        gate.open()
        release_all([handle])
    assert (by_domain, listed) == ("holder", ["holder"])


def test_heartbeat_in_flight_at_release_does_not_recreate_claim(local_env: ClaimEnv) -> None:
    scenario_release_with_inflight_heartbeat(local_env)


@pytest.mark.parametrize(
    "successor",
    [
        pytest.param(False, id="cleared_claim_is_not_recreated"),
        pytest.param(True, id="late_heartbeat_keeps_successor_claim"),
    ],
)
def test_late_heartbeat_after_force_clear_leaves_claim_state(
    local_env: ClaimEnv, successor: bool
) -> None:
    scenario_heartbeat_after_removal(local_env, successor=successor)


def test_stale_handle_release_keeps_successor_claim(local_env: ClaimEnv) -> None:
    scenario_stale_handle_release(local_env)


@pytest.mark.parametrize("operation", ["clear_claim", "release_claim_if_unchanged"])
def test_sweep_does_not_delete_successor_acquired_after_its_read(
    local_env: ClaimEnv, operation: str
) -> None:
    scenario_successor_between_read_and_delete(local_env, operation)


@pytest.mark.parametrize(
    "body",
    [
        pytest.param(b"null", id="json_null"),
        pytest.param(b"[]", id="json_list"),
        pytest.param(b"{}", id="empty_object"),
        pytest.param(b'{"owner_id":"x","last_heartbeat_at":"abc"}', id="non_numeric_heartbeat"),
    ],
)
def test_malformed_claim_file_raises_corruption_error(local_env: ClaimEnv, body: bytes) -> None:
    service = local_env.service(hb=3600, stale=120)
    domain = local_env.domain("malformed")
    local_env.claims_dir.mkdir(parents=True)
    (local_env.claims_dir / domain.claim_name).write_bytes(body)
    with pytest.raises(ControlPlaneCorruptionError):
        service.read_claim_by_domain(product=PRODUCT, domain=domain.identifier)
    with pytest.raises(ControlPlaneCorruptionError):
        service.list_product_claims(product=PRODUCT)


def test_failing_claims_listing_raises_and_absent_directory_lists_empty(
    local_env: ClaimEnv, monkeypatch: pytest.MonkeyPatch
) -> None:
    service = local_env.service(hb=3600, stale=120)
    handle = service.acquire(product=PRODUCT, domain=local_env.domain("listed"), owner_id="A")
    try:

        def unreadable(*args: Any, **kwargs: Any) -> Any:
            raise PermissionError("claims directory unreadable")

        with monkeypatch.context() as patch:
            patch.setattr(local_env.fs, "ls", unreadable)
            with pytest.raises((OSError, ControlPlaneCorruptionError)):
                service.list_claims(product=PRODUCT)
        assert service.list_claims(product="never-claimed.zarr") == []
    finally:
        release_all([handle])


def usurp(handle: ClaimHandle, *, beat: bool = True) -> None:
    """Put a successor's incarnation in the record under ``handle``, then drive one heartbeat."""
    data, validator = handle.fs.read_snapshot(handle.claim_path)
    record = json.loads(data) | {
        "owner_id": "successor",
        "incarnation": "successor-incarnation",
        "revision": "successor-revision",
    }
    handle.fs.compare_replace(handle.claim_path, validator, json.dumps(record).encode())
    assert not beat or (handle._beat() is False and handle.lost)


def test_confirm_with_heartbeat_in_flight_gives_up_at_stale_threshold(local_env: ClaimEnv) -> None:
    service = local_env.service(hb=1, stale=1)
    gate, latch = local_env.hold("A"), ClaimLossLatch()
    with reporting_claim_loss(latch):
        handle = service.acquire(product=PRODUCT, domain=local_env.domain("confirm"), owner_id="A")
    try:
        assert gate.reached.wait(WAIT_S), "heartbeat of A never reached its write"
        started = time.monotonic()
        confirmed = handle.confirm()
        elapsed = time.monotonic() - started
    finally:
        gate.open()
        gate.done.wait(WAIT_S)
        release_all([handle])
    assert (confirmed, latch.is_set, handle.lost, elapsed < 5) == (False, False, False, True)


def raised_names(operation: Callable[[], Any]) -> list[str]:
    """Class names of the exception ``operation`` raises and of its cause; ``[]`` if it returns."""
    try:
        operation()
    except Exception as exc:
        return [type(e).__name__ for e in (exc, exc.__cause__) if e is not None]
    return []


@pytest.mark.parametrize("kind", ["slot", "schema"])
def test_run_loss_after_claim_release_refuses_next_direct_batch(
    local_env: ClaimEnv, kind: str
) -> None:
    service = local_env.service(hb=3600, stale=120)
    handles: list[ClaimHandle] = []

    def acquire_claim(**kwargs: Any) -> ClaimHandle:
        handles.append(service.acquire(**kwargs))
        return handles[-1]

    session = ZarrWriteSession(
        product=PRODUCT,
        run_id="run-1",
        write_mode="direct",
        window=None,
        slot_group=None,
        acquire_claim=acquire_claim,
        open_root=lambda _uri: None,
        open_final_root=lambda _ctx: None,
    )
    claim = session.slot_claim("G", 0) if kind == "slot" else session.schema_claim("G")
    with session.admit_write(), claim:
        usurp(handles[-1])  # lost during the admitted batch; released on leaving the scope

    assert "ClaimLostError" in raised_names(session.admit_write().__enter__)


@pytest.mark.parametrize("fail_batch", [False, True], ids=["next_batch", "failed_batch"])
def test_run_loss_in_append_run_refuses_batch_and_keeps_successor_data(
    local_env: ClaimEnv, monkeypatch: pytest.MonkeyPatch, fail_batch: bool
) -> None:
    store = local_env.claims_dir.parents[1]
    service = local_env.service(hb=3600, stale=120)
    handles: list[ClaimHandle] = []
    days = [np.datetime64(f"2024-01-0{day}T00:00:00", "s") for day in range(1, 8)]

    def claim_for_group(_group: str) -> ClaimHandle:
        domain = WriteDomain(product=PRODUCT, category="zarr_append", name="G")
        handles.append(service.acquire(product=PRODUCT, domain=domain, owner_id="run:G"))
        return handles[-1]

    def append(timestamps: list[Any], claim: Any = None) -> dict[str, Any]:
        def dataset(_group: str, batch: Sequence[Any]) -> xr.Dataset:
            value = (("timestamp",), np.zeros(len(batch), dtype=np.float32))
            return xr.Dataset({"value": value}, coords={"timestamp": list(batch)})

        return append_time_groups(
            store=str(store),
            zarr_store=local_zarr_handle(store),
            session=make_local_session(str(store)),
            group_to_timestamps={"G": timestamps},
            dataset_for_batch=dataset,
            arrays_for_group=lambda group: [f"{group}/value"],
            chunk_shape={"timestamp": 2},
            batch_size=2,
            claim_for_group=claim,
        )

    def stored() -> dict[str, list[Any]]:
        group = cast(Any, zarr.open_group(str(store), mode="r", use_consolidated=False))["G"]
        return {n: group[n][:].tolist() for n in ("value", "timestamp", "firecube_timestamp_state")}

    successor: dict[str, list[Any]] = {}
    real_write = append_module.write_dataset_to_zarr

    def write_then_lose(ds: xr.Dataset, **kwargs: Any) -> None:
        real_write(ds, **kwargs)
        if handles[0].lost:
            return
        usurp(handles[0])  # lost inside the admitted batch that has just written
        if fail_batch:
            group = cast(Any, zarr.open_group(str(store), mode="r+", use_consolidated=False))["G"]
            group["value"][5:] = 99.0  # the successor's values
            successor.update(stored())
            raise RuntimeError("batch fails after the loss")

    repair = Mock(wraps=append_module.repair_failed_group)
    if fail_batch:
        append(days[:5])  # five slots already stored
    monkeypatch.setattr(append_module, "write_dataset_to_zarr", write_then_lose)
    monkeypatch.setattr(append_module, "repair_failed_group", repair)
    names = raised_names(lambda: append(days[5:] if fail_batch else days[:4], claim_for_group))

    assert repair.call_count == 0
    if fail_batch:
        assert "RuntimeError" in names  # the deliberate failure was reached
        assert stored() == successor
    else:
        assert len(stored()["timestamp"]) == 2
        assert "ClaimLostError" in names


class IndexedProbe(DirectZarrIngestor):
    PRODUCT_NAME = "run_loss_indexed"

    def discover_source_files(self, ctx: PluginContext) -> list[str]:
        return ["0", "1"]

    def zarr_schema(self, ctx: PluginContext) -> list[ZarrGroupSpec]:
        dims = ("timestamp",)
        spec = ZarrArraySpec(
            name="value", shape=(4,), chunks=(1,), dtype="float32", dimension_names=dims
        )
        return [ZarrGroupSpec(group="G", arrays=[spec])]

    def index_spec(self, ctx: PluginContext) -> IndexSpec:
        axis = RegularTimeAxis(
            coordinate="timestamp",
            epoch="2024-01-01T00:00:00Z",
            cadence_s=86400,
            mode="exact",
            slot_count=4,
        )
        return IndexSpec(name="probe", groups={"G": axis})

    def inspect_item(self, item: Any, ctx: PluginContext) -> ItemInfo:
        return ItemInfo(coordinate=f"2024-01-0{int(item) + 1}T00:00:00Z")

    def build_write_intents(self, batch: Any, ctx: PluginContext) -> list[WriteIntent]:
        slot, data = int(batch.items[0]), np.array([10.0], dtype="float32")
        return [WriteIntent(group="G", array="value", kind="1d", ts_index=slot, data=data)]


class ParquetProbe(GenericParquetIngestor):
    PRODUCT_NAME = name = "run_loss_parquet"

    def discover_source_files(self, ctx: PluginContext) -> list[str]:
        return ["first", "second"]

    def build_dataset(self, group: str, batch: Any, ctx: PluginContext) -> pa.Table:
        return pa.table({"item": batch.items})


@pytest.mark.parametrize(
    ("case", "when"),
    [
        ("resolved_index", "none"),
        ("parquet_path", "none"),
        ("resolved_index", "heartbeat"),
        ("parquet_path", "heartbeat"),
        ("fresh_output", "heartbeat"),
        ("resolved_index", "release"),
        ("parquet_path", "release"),
        ("fresh_output", "release"),
    ],
)
def test_run_loss_of_startup_or_output_claim_records_run_failed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, case: str, when: str
) -> None:
    """``release``: nothing heartbeats after the loss, so only the end of the run can find it."""
    handles: list[ClaimHandle] = []
    lost: list[ClaimHandle] = []
    real_acquire = ChunkManager.acquire_claim

    def acquire_claim(self: ChunkManager, **kwargs: Any) -> ClaimHandle:
        handles.append(real_acquire(self, **kwargs))
        return handles[-1]

    def after(original: Callable[..., Any]) -> Callable[..., Any]:
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            result = original(*args, **kwargs)
            if when != "none" and not lost:
                lost.append(next(h for h in handles[::-1] if f":{case}:" in h.info.domain))
                with lost[0]._call_lock:
                    usurp(lost[0], beat=when == "heartbeat")
            return result

        return wrapper

    owner, name = (
        (ChunkManager, "_apply_resolved_index_precedence")
        if case == "resolved_index"
        else (GenericParquetIngestor, "write_parquet")
    )
    monkeypatch.setattr(ChunkManager, "acquire_claim", acquire_claim)
    monkeypatch.setattr(owner, name, after(getattr(owner, name)))
    options = {"pipeline_workers": 1, "pipeline_batch_size": 1, "write_mode": "direct"}
    if case == "resolved_index":
        product = "output.zarr"
        binding = make_test_binding(tmp_path, product=product)
        host = IndexedProbe(chunk_manager=ChunkManager(binding=binding, workspace=tmp_path / "cm"))
        ctx = make_test_context(tmp_path, source=str(tmp_path), product=product, options=options)
    else:
        product, host = "run_loss_parquet", ParquetProbe()
        ctx = make_test_context(tmp_path, product=product, format="parquet", options=options)
    names = raised_names(lambda: host.run(ctx))

    statuses = sorted(r.status for r in host._chunk_manager.list_runs(product=product))
    written = (tmp_path / "output.zarr" / "G" / "value" / "zarr.json").exists()
    if when == "none":
        assert (names, statuses) == ([], ["complete"])
    else:
        assert "ClaimLostError" in names and statuses == ["failed"]
        assert not (case == "resolved_index" and written), "a batch was admitted after the loss"


@pytest.mark.parametrize("fail_batch", [False, True], ids=["final_batch", "failing_batch"])
def test_run_loss_in_final_batch_records_run_failed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, fail_batch: bool
) -> None:
    product = "output.zarr"
    source = tmp_path / "source"
    source.mkdir()
    (source / "input.nc").touch()
    manager = ChunkManager(
        binding=make_test_binding(tmp_path, product=product), workspace=tmp_path / "cm-work"
    )
    handles: list[ClaimHandle] = []
    real_acquire, real_write = ChunkManager.acquire_claim, append_module.write_dataset_to_zarr

    def acquire_claim(self: ChunkManager, **kwargs: Any) -> ClaimHandle:
        handle = real_acquire(self, **kwargs)
        if kwargs["domain"].category == "zarr_append":
            handles.append(handle)
        return handle

    def write_then_lose(ds: xr.Dataset, **kwargs: Any) -> None:
        real_write(ds, **kwargs)
        if not handles[-1].lost:
            usurp(handles[-1])  # the only batch is written; the claim is lost before the run ends
        if fail_batch:
            raise RuntimeError("batch fails after the loss")

    class Ingestor(GenericZarrIngestor):
        PRODUCT_NAME = "run_loss_final_batch"
        name = "run_loss_final_batch"

        def build_dataset(self, group: str, items: list[object], ctx: PluginContext) -> xr.Dataset:
            coords = {"timestamp": pd.date_range("2024-01-01", periods=2)}
            return xr.Dataset({"data": (("timestamp",), np.zeros(2))}, coords=coords)

    monkeypatch.setattr(ChunkManager, "acquire_claim", acquire_claim)
    repair = Mock(wraps=append_module.repair_failed_group)
    monkeypatch.setattr(append_module, "write_dataset_to_zarr", write_then_lose)
    monkeypatch.setattr(append_module, "repair_failed_group", repair)
    options = {"pipeline_workers": 1, "input_filters": ["*.nc"], "write_mode": "direct"}
    ctx = make_test_context(tmp_path, source=str(source), product=product, options=options)
    names = raised_names(lambda: Ingestor(chunk_manager=manager).run(ctx))

    assert {run.status for run in manager.list_runs(product=product)} == {"failed"}
    assert "ClaimLostError" in names and repair.call_count == 0
    wals = (tmp_path / product / ".firecube").rglob("events-*.jsonl")
    events = [json.loads(line) for wal in wals for line in wal.read_text().splitlines()]
    failed = [e["meta"].get("lost_claim") for e in events if e["event_type"] == "run_failed"]
    assert failed == [handles[-1].info.domain]
