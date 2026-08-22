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

"""Concurrency tests for resolved-index claim convergence."""

from __future__ import annotations

import hashlib
import threading
from pathlib import Path

import pytest

from firecube.core.controlplane import ChunkManager
from firecube.core.controlplane.types import ResolvedIndexRecord, canonical_index_bytes
from tests.helpers.storage import make_test_binding


def _manager(tmp_path: Path) -> ChunkManager:
    return ChunkManager(binding=make_test_binding(tmp_path), workspace=tmp_path)


def _record() -> ResolvedIndexRecord:
    index = {"groups": {"g1": {"axes": {"time": {"kind": "integer", "size": 2}}}}}
    return ResolvedIndexRecord(
        recorded_at="2026-08-20T00:00:00+00:00",
        recorded_by_run_id="run-shared",
        identity_hash=hashlib.sha256(canonical_index_bytes(index)).hexdigest(),
        index=index,
    )


@pytest.mark.integration
@pytest.mark.concurrency
def test_two_workers_with_identical_resolved_index_converge(tmp_path: Path) -> None:
    declared = _record()
    barrier = threading.Barrier(2)
    results: list[tuple[ResolvedIndexRecord, str]] = []
    errors: list[BaseException] = []
    lock = threading.Lock()

    def worker() -> None:
        cm = _manager(tmp_path)
        barrier.wait(timeout=5)
        try:
            result = cm.ensure_resolved_index(
                product="prod1",
                record=declared,
                max_retries=10,
                initial_backoff_s=0.01,
            )
        except BaseException as exc:  # pragma: no cover - reported below
            with lock:
                errors.append(exc)
            return
        with lock:
            results.append(result)

    threads = [threading.Thread(target=worker, name=f"resolved-index-worker-{i}") for i in range(2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=10)

    assert all(not thread.is_alive() for thread in threads)
    assert errors == []
    assert len(results) == 2
    assert {record.identity_hash for record, _outcome in results} == {declared.identity_hash}
    assert {outcome for _record, outcome in results} <= {"created", "matched_existing"}
    assert _manager(tmp_path).get_resolved_index(product="prod1") == declared
