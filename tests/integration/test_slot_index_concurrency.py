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

"""Concurrency tests for ``ChunkManager.ensure_slot_index_model``.

Verifies the contract that multiple ingestor pods racing to declare the same
slot-index model converge on exactly one persisted record and one
``EVENT_SLOT_INDEX_MODEL_RECORDED`` event, with all losers emitting
``EVENT_SLOT_INDEX_MODEL_VERIFIED`` after observing full convergence
(CP record AND zarr root identity-hash attr).

Synchronisation is built on ``threading.Barrier`` (NOT ``time.sleep``) so the
contention window is the same on every machine, slow or fast.  Event accounting
uses a thread-safe counter that wraps ``repo.record_slot_index_model_event``
before threads spawn; this is cleaner than parsing the WAL after the fact
because the WAL writer batches slot events with ``flush=False`` (per the
schema-verification pattern) and a post-test flush would still be racy.

The fourth test is a regression guard for the CP-only race window: if a future
refactor weakens the loser-thread convergence check to "CP record matches",
losers would falsely emit VERIFIED and return when the winner is still
mid-write (CP committed, attrs not yet stamped). This test simulates exactly
that on-disk state and asserts the loser instead hits the retry budget and
raises ``SlotIndexModelClaimTimeoutError`` with ZERO VERIFIED events.
"""

from __future__ import annotations

import json
import threading
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any

import pytest
import zarr
from zarr.storage import LocalStore

from firecube.core.controlplane import ChunkManager
from firecube.core.controlplane.types import (
    EVENT_SLOT_INDEX_MODEL_RECORDED,
    EVENT_SLOT_INDEX_MODEL_VERIFIED,
    SLOT_INDEX_CURRENT_FILENAME,
    SLOT_INDEX_DIRNAME,
    SlotIndexModelRecord,
)
from firecube.core.errors import (
    SlotIndexModelClaimTimeoutError,
    SlotIndexModelConflictError,
)
from firecube.core.product.identity import ProductIdentity
from firecube.core.slot_index import (
    SLOT_INDEX_MODEL_IDENTITY_HASH_ATTR,
    SlotAxis,
    SlotIndexModel,
)
from firecube.core.storage.binding import StorageBinding
from firecube.core.storage.driver_config import StorageDriverConfig
from firecube.core.storage.uri import StorageUri

pytestmark = [pytest.mark.integration, pytest.mark.concurrency]

_PRODUCT = "concprod"
_BARRIER_TIMEOUT_S = 30.0
_JOIN_TIMEOUT_S = 60.0


class _EventCounter:
    """Thread-safe counter for ``record_slot_index_model_event`` invocations.

    Wraps the real repo method so the on-disk WAL still gets the event AND
    the test can read ``event_type`` counts without parsing WAL segments.
    Counters are keyed by ``event_type`` so RECORDED vs VERIFIED can be
    distinguished without scanning the call list.
    """

    def __init__(self, real_method: Callable[..., None]) -> None:
        self._real = real_method
        self._lock = threading.Lock()
        self.counts: dict[str, int] = {}

    def __call__(self, *args: Any, **kwargs: Any) -> None:
        event_type = str(kwargs.get("event_type", ""))
        with self._lock:
            self.counts[event_type] = self.counts.get(event_type, 0) + 1
        self._real(*args, **kwargs)


def _make_manager(tmp_path: Path) -> ChunkManager:
    product_uri = StorageUri.from_local_path(tmp_path / "__firecube_controlplane__")
    binding = StorageBinding(
        identity=ProductIdentity.from_uri(product_uri, "zarr", product_name="control_product"),
        driver=StorageDriverConfig(),
    )
    return ChunkManager(binding=binding, workspace=tmp_path)


def _model(name: str = "concurrency_v1") -> SlotIndexModel:
    return SlotIndexModel(
        name=name,
        epoch="2026-01-01T00:00:00Z",
        groups={"g1": SlotAxis(cadence_s=300, mode="exact")},
    )


def _current_json(tmp_path: Path) -> Path:
    return tmp_path / _PRODUCT / ".firecube" / SLOT_INDEX_DIRNAME / SLOT_INDEX_CURRENT_FILENAME


def _read_root_attrs_hash(tmp_path: Path) -> Any:
    try:
        store = LocalStore(str(tmp_path / _PRODUCT))
        root = zarr.open_group(store=store, mode="r", zarr_format=3)
    except Exception:
        return None
    return root.attrs.get(SLOT_INDEX_MODEL_IDENTITY_HASH_ATTR)


def _start_run(cm: ChunkManager, tmp_path: Path, run_id: str) -> None:
    cm.record_run_started(
        product=_PRODUCT,
        run_id=run_id,
        output_path=str(tmp_path / _PRODUCT),
        output_format="zarr",
        size=0,
        meta={"plugin": "slot_index_concurrency_test"},
    )


def test_concurrent_same_model_fresh_store(tmp_path: Path) -> None:
    cm = _make_manager(tmp_path)
    model = _model()
    counter = _EventCounter(cm.repo.record_slot_index_model_event)
    cm.repo.record_slot_index_model_event = counter  # type: ignore[method-assign]

    n_threads = 5
    barrier = threading.Barrier(n_threads, timeout=_BARRIER_TIMEOUT_S)
    results: list[SlotIndexModelRecord] = []
    results_lock = threading.Lock()
    errors: list[BaseException] = []

    def worker(i: int) -> None:
        run_id = f"r{i}"
        _start_run(cm, tmp_path, run_id)
        barrier.wait(timeout=_BARRIER_TIMEOUT_S)
        try:
            rec = cm.ensure_slot_index_model(
                product=_PRODUCT,
                model=model,
                run_id=run_id,
                max_retries=5,
                initial_backoff_s=0.05,
            )
        except BaseException as exc:
            errors.append(exc)
            return
        with results_lock:
            results.append(rec)

    with ThreadPoolExecutor(max_workers=n_threads) as pool:
        futs = [pool.submit(worker, i) for i in range(n_threads)]
        for f in futs:
            f.result(timeout=_JOIN_TIMEOUT_S)

    assert not errors, f"no thread should have raised; got {errors!r}"
    assert len(results) == n_threads, (
        f"expected {n_threads} records, got {len(results)}: {results!r}"
    )

    hashes = {r.identity_hash for r in results}
    assert hashes == {model.identity_hash}, (
        f"all threads must converge on the same identity_hash; got {hashes!r}"
    )

    assert _current_json(tmp_path).is_file()
    on_disk = SlotIndexModelRecord.from_json_bytes(_current_json(tmp_path).read_bytes())
    assert on_disk.identity_hash == model.identity_hash
    assert _read_root_attrs_hash(tmp_path) == model.identity_hash

    recorded = counter.counts.get(EVENT_SLOT_INDEX_MODEL_RECORDED, 0)
    verified = counter.counts.get(EVENT_SLOT_INDEX_MODEL_VERIFIED, 0)
    assert recorded == 1, (
        f"expected exactly 1 RECORDED event (only the winner writes); got {recorded}"
    )
    assert verified == n_threads - 1, (
        f"expected {n_threads - 1} VERIFIED events (one per loser); got {verified}"
    )


def test_concurrent_different_models_fresh_store(tmp_path: Path) -> None:
    cm = _make_manager(tmp_path)
    counter = _EventCounter(cm.repo.record_slot_index_model_event)
    cm.repo.record_slot_index_model_event = counter  # type: ignore[method-assign]

    n_threads = 5
    models = [_model(name=f"variant_{i}_v1") for i in range(n_threads)]
    assert len({m.identity_hash for m in models}) == n_threads, (
        "each variant must have a distinct identity_hash"
    )

    barrier = threading.Barrier(n_threads, timeout=_BARRIER_TIMEOUT_S)
    winners: list[SlotIndexModelRecord] = []
    winners_lock = threading.Lock()
    conflict_errors: list[SlotIndexModelConflictError] = []
    other_errors: list[BaseException] = []

    def worker(i: int) -> None:
        run_id = f"r{i}"
        _start_run(cm, tmp_path, run_id)
        barrier.wait(timeout=_BARRIER_TIMEOUT_S)
        try:
            rec = cm.ensure_slot_index_model(
                product=_PRODUCT,
                model=models[i],
                run_id=run_id,
                max_retries=5,
                initial_backoff_s=0.05,
            )
        except SlotIndexModelConflictError as exc:
            conflict_errors.append(exc)
            return
        except BaseException as exc:
            other_errors.append(exc)
            return
        with winners_lock:
            winners.append(rec)

    with ThreadPoolExecutor(max_workers=n_threads) as pool:
        futs = [pool.submit(worker, i) for i in range(n_threads)]
        for f in futs:
            f.result(timeout=_JOIN_TIMEOUT_S)

    assert not other_errors, f"only conflict errors expected; got {other_errors!r}"
    assert len(winners) == 1, (
        f"exactly one model variant must win; got {len(winners)} winners "
        f"and {len(conflict_errors)} conflicts"
    )
    assert len(conflict_errors) == n_threads - 1, (
        f"expected {n_threads - 1} conflicts; got {len(conflict_errors)}"
    )

    winner_hash = winners[0].identity_hash
    assert _current_json(tmp_path).is_file()
    on_disk = SlotIndexModelRecord.from_json_bytes(_current_json(tmp_path).read_bytes())
    assert on_disk.identity_hash == winner_hash
    assert _read_root_attrs_hash(tmp_path) == winner_hash


def test_concurrent_same_model_high_contention(tmp_path: Path) -> None:
    cm = _make_manager(tmp_path)
    model = _model(name="high_contention_v1")
    counter = _EventCounter(cm.repo.record_slot_index_model_event)
    cm.repo.record_slot_index_model_event = counter  # type: ignore[method-assign]

    n_threads = 20
    barrier = threading.Barrier(n_threads, timeout=_BARRIER_TIMEOUT_S)
    results: list[SlotIndexModelRecord] = []
    results_lock = threading.Lock()
    errors: list[BaseException] = []

    def worker(i: int) -> None:
        run_id = f"r{i}"
        _start_run(cm, tmp_path, run_id)
        barrier.wait(timeout=_BARRIER_TIMEOUT_S)
        try:
            rec = cm.ensure_slot_index_model(
                product=_PRODUCT,
                model=model,
                run_id=run_id,
                max_retries=10,
                initial_backoff_s=0.02,
            )
        except BaseException as exc:
            errors.append(exc)
            return
        with results_lock:
            results.append(rec)

    with ThreadPoolExecutor(max_workers=n_threads) as pool:
        futs = [pool.submit(worker, i) for i in range(n_threads)]
        for f in futs:
            f.result(timeout=_JOIN_TIMEOUT_S)

    assert not errors, (
        f"with max_retries=10 and 20 threads same model, no thread should raise; got {errors!r}"
    )
    assert len(results) == n_threads
    hashes = {r.identity_hash for r in results}
    assert hashes == {model.identity_hash}

    recorded = counter.counts.get(EVENT_SLOT_INDEX_MODEL_RECORDED, 0)
    verified = counter.counts.get(EVENT_SLOT_INDEX_MODEL_VERIFIED, 0)
    assert recorded == 1, f"expected exactly 1 RECORDED, got {recorded}"
    assert verified == n_threads - 1, f"expected {n_threads - 1} VERIFIED, got {verified}"


def test_loser_never_converges_on_cp_only_state(tmp_path: Path) -> None:
    """Regression: loser must NOT emit VERIFIED on a CP-only match.

    Simulates the race window where the winner has written ``current.json``
    but has not yet stamped the zarr root identity-hash attr. A loser thread
    that treated CP-only match as convergence would return a stale (or even
    correct-by-accident) record and emit VERIFIED, masking the unfinished
    winner's transaction. The contract is: the loser keeps retrying until
    both surfaces (CP + attrs) match, or it exhausts its retry budget.
    """
    cm = _make_manager(tmp_path)
    model = _model(name="cp_only_regression_v1")

    record = SlotIndexModelRecord(
        model=model,
        identity_hash=model.identity_hash,
        schema_version="v1",
        recorded_at="2026-01-01T00:00:00+00:00",
        recorded_by_run_id="winner",
    )
    control_root_uri = cm.get_control_root(_PRODUCT)
    fs, control_root = cm.repo._get_fs(control_root_uri)
    slot_index_dir = control_root.join(SLOT_INDEX_DIRNAME)
    current_json_path = slot_index_dir.join(SLOT_INDEX_CURRENT_FILENAME)
    fs.makedirs(slot_index_dir, exist_ok=True)  # pyright: ignore[reportAttributeAccessIssue]
    with fs.open(current_json_path, "wb") as fh:
        fh.write(record.to_json_bytes())

    assert _current_json(tmp_path).is_file(), "CP must be pre-seeded"
    assert _read_root_attrs_hash(tmp_path) is None, "attrs must be absent"

    counter = _EventCounter(cm.repo.record_slot_index_model_event)
    cm.repo.record_slot_index_model_event = counter  # type: ignore[method-assign]

    claims_dir = tmp_path / _PRODUCT / ".firecube" / "claims"
    claims_dir.mkdir(parents=True, exist_ok=True)
    from firecube.core.controlplane.types import WriteDomain

    domain = WriteDomain(product=_PRODUCT, category="slot_index_model", name="current")
    claim_file = claims_dir / domain.claim_name
    claim_file.write_text(
        json.dumps(
            {
                "product": _PRODUCT,
                "domain": domain.identifier,
                "owner_id": "winner",
                "claim_path": str(claim_file),
                "acquired_at": 9999999999.0,
                "last_heartbeat_at": 9999999999.0,
                "heartbeat_interval_s": 30,
                "stale_threshold_s": 120,
            }
        )
    )

    _start_run(cm, tmp_path, "loser")
    with pytest.raises(SlotIndexModelClaimTimeoutError):
        cm.ensure_slot_index_model(
            product=_PRODUCT,
            model=model,
            run_id="loser",
            max_retries=3,
            initial_backoff_s=0.005,
        )

    verified = counter.counts.get(EVENT_SLOT_INDEX_MODEL_VERIFIED, 0)
    assert verified == 0, (
        f"loser must NOT emit VERIFIED on CP-only state; got {verified} "
        f"VERIFIED events (counter snapshot: {counter.counts!r})"
    )
    recorded = counter.counts.get(EVENT_SLOT_INDEX_MODEL_RECORDED, 0)
    assert recorded == 0, (
        f"loser cannot enter Row 1 (CP exists), so no RECORDED expected; got {recorded}"
    )
