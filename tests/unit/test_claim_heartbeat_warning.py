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
from typing import Any, cast

import pytest

from firecube.core.controlplane.claims import ClaimHandle
from firecube.core.controlplane.types import ClaimInfo
from firecube.core.storage.uri import StorageUri

pytestmark = pytest.mark.unit


class _FailingFilesystem:
    def open(self, path, mode):
        _ = (path, mode)
        raise OSError("boom")

    def rm(self, path, recursive=False):
        _ = (path, recursive)


class _FakeEvent:
    def __init__(self) -> None:
        self.calls = 0

    def wait(self, timeout: float) -> bool:
        _ = timeout
        self.calls += 1
        return self.calls > 1

    def set(self) -> None:
        return None


class _FakeThread:
    def __init__(self, *, target, name, daemon):
        self._target = target
        self.name = name
        self.daemon = daemon
        self._started = False

    def start(self) -> None:
        self._started = True
        self._target()

    def is_alive(self) -> bool:
        return False

    def join(self, timeout: float | None = None) -> None:
        _ = timeout


def test_claim_heartbeat_write_failure_logs_warning(caplog, monkeypatch) -> None:
    monkeypatch.setattr("firecube.core.controlplane.claims.threading.Event", _FakeEvent)
    monkeypatch.setattr("firecube.core.controlplane.claims.threading.Thread", _FakeThread)

    info = ClaimInfo(
        product="product.zarr",
        domain="zarr_append:slot-0",
        owner_id="owner-1",
        claim_path="file:///tmp/product.zarr/.firecube/claims/slot-0.json",
        acquired_at=1.0,
        last_heartbeat_at=1.0,
        heartbeat_interval_s=30,
        stale_threshold_s=120,
    )

    with caplog.at_level(logging.WARNING, logger="firecube.core.controlplane.claims"):
        handle = ClaimHandle(
            fs=cast(Any, _FailingFilesystem()),
            claim_path=StorageUri.parse(info.claim_path),
            info=info,
            heartbeat_interval_s=30,
            stale_threshold_s=120,
        )

    handle.release()

    assert any(
        rec.levelno == logging.WARNING
        and "claim heartbeat write failed for" in rec.message
        and "/tmp/product.zarr/.firecube/claims/slot-0.json" in rec.message
        and "boom" in rec.message
        for rec in caplog.records
    )
