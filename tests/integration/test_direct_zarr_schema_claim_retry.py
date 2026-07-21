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

"""Bounded retry of the per-group schema-setup claim in ``_setup_global_zarr_schema``.

Slot-range parallelism starts many ``firecube ingest`` pods at once on a fresh
store. They all observe ``group_schema_satisfied() == False`` and race for the
exclusive ``zarr_schema_global:<group>:setup`` claim. Without retry exactly one
pod wins and every other pod aborts its entire run with ``ClaimConflictError``.

The fix mirrors :meth:`ChunkManager.ensure_slot_index_model`: bounded retry +
convergence recheck so losers re-observe the schema after the winner finishes
and continue, while a budget-exhausted loser still propagates a hard failure.
"""

from __future__ import annotations

import threading
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import numpy as np
import pytest
import zarr

from firecube.core.controlplane import ChunkManager
from firecube.core.controlplane.types import WriteDomain
from firecube.core.errors import ClaimConflictError
from firecube.ingestor.api import ZarrArraySpec, ZarrGroupSpec
from firecube.ingestor.templates import direct_zarr as direct_zarr_module
from firecube.ingestor.templates.direct_zarr import _setup_global_zarr_schema
from tests.helpers.storage import make_test_binding

_PRODUCT = "product.zarr"
_GROUP = "data"
_JOIN_TIMEOUT_S = 30.0
_BARRIER_TIMEOUT_S = 30.0


def _strategy(store_uri: str) -> SimpleNamespace:
    return SimpleNamespace(
        _store_uri=store_uri,
        _storage_config=None,
        _session=None,
        _coord_names_by_group={},
    )


def _schema() -> list[ZarrGroupSpec]:
    return [
        ZarrGroupSpec(
            group=_GROUP,
            arrays=[
                ZarrArraySpec(
                    name="data",
                    shape=(4, 3, 2),
                    dtype=np.float32,
                    chunks=(2, 3, 2),
                    fill_value=0.0,
                ),
            ],
        )
    ]


def _make_manager(tmp_path: Path, *, workspace_name: str) -> ChunkManager:
    workspace = tmp_path / workspace_name
    workspace.mkdir(exist_ok=True)
    return ChunkManager(
        binding=make_test_binding(tmp_path, product=_PRODUCT),
        workspace=workspace,
    )


@pytest.mark.integration
def test_schema_setup_single_pod_no_retry_needed(tmp_path: Path) -> None:
    """Happy path: single pod, claim acquired first try, arrays created."""
    store_path = tmp_path / _PRODUCT
    manager = _make_manager(tmp_path, workspace_name="workspace-solo")
    try:
        _setup_global_zarr_schema(
            strategy=_strategy(str(store_path)),
            schema=_schema(),
            global_expected={_GROUP: 4},
            product=_PRODUCT,
            run_id="run-solo",
            chunk_manager=manager,
        )
    finally:
        manager.close()

    arr = cast(
        Any,
        zarr.open_group(store=str(store_path), mode="r", zarr_format=3)[f"{_GROUP}/data"],
    )
    assert arr.shape == (4, 3, 2)
    assert manager.list_claims(product=_PRODUCT) == [], (
        "schema-setup claim must be released after happy-path completion"
    )


@pytest.mark.concurrency
@pytest.mark.integration
def test_concurrent_schema_setup_one_winner_others_converge(tmp_path: Path) -> None:
    """5 threads contend on a fresh store; all complete, schema is set up exactly once."""
    n_threads = 5
    barrier = threading.Barrier(n_threads, timeout=_BARRIER_TIMEOUT_S)
    errors: list[BaseException] = []
    errors_lock = threading.Lock()
    store_path = tmp_path / _PRODUCT

    def worker(idx: int) -> None:
        manager = _make_manager(tmp_path, workspace_name=f"workspace-{idx}")
        try:
            barrier.wait(timeout=_BARRIER_TIMEOUT_S)
            _setup_global_zarr_schema(
                strategy=_strategy(str(store_path)),
                schema=_schema(),
                global_expected={_GROUP: 4},
                product=_PRODUCT,
                run_id=f"run-{idx}",
                chunk_manager=manager,
            )
        except BaseException as exc:
            with errors_lock:
                errors.append(exc)
        finally:
            manager.close()

    threads = [
        threading.Thread(target=worker, args=(i,), name=f"schema-setup-{i}")
        for i in range(n_threads)
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=_JOIN_TIMEOUT_S)

    for t in threads:
        assert not t.is_alive(), (
            f"Thread {t.name!r} did not finish within {_JOIN_TIMEOUT_S}s; "
            "possible deadlock in retry loop."
        )

    assert errors == [], (
        f"All {n_threads} pods must converge on schema setup with no errors; got: "
        f"{[type(e).__name__ + ': ' + str(e) for e in errors]!r}"
    )

    arr = cast(
        Any,
        zarr.open_group(store=str(store_path), mode="r", zarr_format=3)[f"{_GROUP}/data"],
    )
    assert arr.shape == (4, 3, 2), (
        f"Final array must match declared shape exactly (no double-init), got {arr.shape!r}"
    )

    verifier = _make_manager(tmp_path, workspace_name="workspace-verify")
    try:
        assert verifier.list_claims(product=_PRODUCT) == [], (
            "No leftover schema-setup claims after all pods converged; found: "
            f"{verifier.list_claims(product=_PRODUCT)!r}"
        )
    finally:
        verifier.close()


@pytest.mark.integration
def test_schema_setup_claim_timeout_propagates(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Pre-seeded foreign claim never frees; retries exhaust and ClaimConflictError propagates."""
    store_path = tmp_path / _PRODUCT
    holder = _make_manager(tmp_path, workspace_name="workspace-holder")
    contender = _make_manager(tmp_path, workspace_name="workspace-contender")
    monkeypatch.setattr(direct_zarr_module.time, "sleep", lambda _s: None)

    domain = WriteDomain(
        product=_PRODUCT,
        category="zarr_schema_global",
        name=f"{_GROUP}:setup",
    )
    handle = holder.acquire_claim(product=_PRODUCT, domain=domain, owner_id="run-holder")
    try:
        with pytest.raises(ClaimConflictError):
            _setup_global_zarr_schema(
                strategy=_strategy(str(store_path)),
                schema=_schema(),
                global_expected={_GROUP: 4},
                product=_PRODUCT,
                run_id="run-contender",
                chunk_manager=contender,
            )
    finally:
        handle.release()
        contender.close()
        holder.close()
