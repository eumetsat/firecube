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

from firecube.core.controlplane.claims import ClaimHandle, ClaimLossLatch
from firecube.core.controlplane.types import ClaimInfo
from firecube.core.filesystem.protocol import ConditionalWrite, PreconditionFailed
from firecube.core.storage.uri import StorageUri

pytestmark = pytest.mark.unit


class _CasFilesystem:
    """One object under compare-and-swap; while ``failing``, a renewal raises, and lands if ``lands``."""

    failing, generation, stored = True, 1, b""

    def __init__(self, *, lands: bool) -> None:
        self.lands = lands

    def capabilities(self):
        return [ConditionalWrite]

    def compare_replace(self, path, validator, data) -> str:
        if validator != f"v{self.generation}":
            raise PreconditionFailed(str(path), "validator is not the stored generation")
        if self.lands or not self.failing:
            self.generation, self.stored = self.generation + 1, data
        if self.failing:
            raise OSError("boom")
        return f"v{self.generation}"

    def read_snapshot(self, path) -> tuple[bytes, str]:
        if not self.lands:
            raise OSError("boom")
        return self.stored, f"v{self.generation}"


class _FakeEvent:
    def __init__(self) -> None:
        self.calls = 0

    def wait(self, timeout: float) -> bool:
        _ = timeout
        self.calls += 1
        return self.calls > 1

    def set(self) -> None:
        return None

    def is_set(self) -> bool:
        return False


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


@pytest.mark.parametrize(
    ("lands", "kept", "next_validator"), [(False, "v1", "v2"), (True, "v2", "v3")]
)
def test_claim_heartbeat_write_failure_logs_warning(
    caplog, monkeypatch, lands, kept, next_validator
) -> None:
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

    fs, latch = _CasFilesystem(lands=lands), ClaimLossLatch()
    with caplog.at_level(logging.WARNING, logger="firecube.core.controlplane.claims"):
        handle = ClaimHandle(
            fs=cast(Any, fs),
            claim_path=StorageUri.parse(info.claim_path),
            info=info,
            heartbeat_interval_s=30,
            stale_threshold_s=120,
            validator="v1",
            latch=latch,
        )

    assert handle.validator == kept  # a committed renewal is adopted, one that failed is not
    fs.failing = False  # so that the next renewal and the release succeed
    assert handle._beat() and handle.validator == next_validator
    handle.release()
    assert not latch.is_set and not handle.lost  # a transient error warns and loses nothing

    assert any(
        rec.levelno == logging.WARNING
        and "claim heartbeat write failed for" in rec.message
        and "/tmp/product.zarr/.firecube/claims/slot-0.json" in rec.message
        and "boom" in rec.message
        for rec in caplog.records
    )
