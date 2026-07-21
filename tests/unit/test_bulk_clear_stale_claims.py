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

"""Tests for ChunkManager.clear_stale_claims — bulk stale-claim clearing.

Covers:
- Dry-run preview (no mutation).
- Fresh-vs-stale segregation on mutation.
- Mid-sweep race where a previously stale claim gets refreshed to fresh.
- Mid-sweep race where another operator deletes a previewed claim.
- Empty-input no-op.
- Repeated-call idempotency.
"""

from __future__ import annotations

import json
import time
from pathlib import Path

import pytest

from firecube.core.controlplane import ChunkManager
from firecube.core.controlplane.types import ClaimInfo, ClearSweepResult, WriteDomain
from firecube.core.storage.uri import StorageUri
from tests.helpers.storage import make_test_binding

pytestmark = pytest.mark.unit


def _manager(tmp_path: Path, *, product: str = "product.zarr") -> ChunkManager:
    workspace = tmp_path / "workspace"
    workspace.mkdir(exist_ok=True)
    return ChunkManager(binding=make_test_binding(tmp_path, product=product), workspace=workspace)


def _claims_dir(tmp_path: Path, product: str) -> Path:
    return tmp_path / product / ".firecube" / "claims"


def _claim_path(tmp_path: Path, product: str, domain: WriteDomain) -> Path:
    return _claims_dir(tmp_path, product) / domain.claim_name


def _write_claim(
    tmp_path: Path,
    *,
    product: str,
    domain: WriteDomain,
    last_heartbeat_at: float,
    stale_threshold_s: int = 120,
) -> Path:
    claims_dir = _claims_dir(tmp_path, product)
    claims_dir.mkdir(parents=True, exist_ok=True)
    claim_path = _claim_path(tmp_path, product, domain)
    payload = {
        "product": product,
        "domain": domain.identifier,
        "owner_id": f"owner:{domain.identifier}",
        "claim_path": StorageUri.from_local_path(claim_path).to_str(),
        "acquired_at": last_heartbeat_at,
        "last_heartbeat_at": last_heartbeat_at,
        "heartbeat_interval_s": 30,
        "stale_threshold_s": stale_threshold_s,
    }
    claim_path.write_text(json.dumps(payload), encoding="utf-8")
    return claim_path


def test_dry_run_previews_without_clearing(tmp_path: Path) -> None:
    manager = _manager(tmp_path)
    product = "product.zarr"
    now = time.time()

    domains = [
        WriteDomain(product=product, category="zarr_append", name=f"stale-{i}") for i in range(3)
    ]
    files = [
        _write_claim(tmp_path, product=product, domain=d, last_heartbeat_at=now - 300)
        for d in domains
    ]

    try:
        result = manager.clear_stale_claims(product=product, dry_run=True)
    finally:
        manager.close()

    assert isinstance(result, ClearSweepResult)
    assert sorted(result.previewed) == sorted(d.identifier for d in domains)
    assert result.cleared == []
    assert result.skipped_fresh == []
    assert result.skipped_missing == []
    for f in files:
        assert f.exists(), f"dry-run must not touch {f}"


def test_clears_only_stale_claims_leaving_fresh_alone(tmp_path: Path) -> None:
    manager = _manager(tmp_path)
    product = "product.zarr"
    now = time.time()

    stale_domains = [
        WriteDomain(product=product, category="zarr_append", name=f"stale-{i}") for i in range(3)
    ]
    fresh_domains = [
        WriteDomain(product=product, category="zarr_append", name=f"fresh-{i}") for i in range(2)
    ]
    stale_files = [
        _write_claim(tmp_path, product=product, domain=d, last_heartbeat_at=now - 300)
        for d in stale_domains
    ]
    fresh_files = [
        _write_claim(tmp_path, product=product, domain=d, last_heartbeat_at=now - 30)
        for d in fresh_domains
    ]

    try:
        result = manager.clear_stale_claims(product=product, dry_run=False)
    finally:
        manager.close()

    assert sorted(result.previewed) == sorted(d.identifier for d in stale_domains)
    assert sorted(result.cleared) == sorted(d.identifier for d in stale_domains)
    assert result.skipped_fresh == []
    assert result.skipped_missing == []
    for f in stale_files:
        assert not f.exists(), f"stale file {f} should have been cleared"
    for f in fresh_files:
        assert f.exists(), f"fresh file {f} must be preserved"


def test_race_claim_becomes_fresh(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A live pod refreshes the heartbeat between preview and mutation → skipped_fresh."""
    manager = _manager(tmp_path)
    product = "product.zarr"
    now = time.time()

    # Iteration order is sorted by domain identifier; "a_race" sorts before
    # "z_other" so d_race is the first mutation-time re-check.
    d_race = WriteDomain(product=product, category="zarr_append", name="a_race")
    d_other = WriteDomain(product=product, category="zarr_append", name="z_other")

    race_file = _write_claim(tmp_path, product=product, domain=d_race, last_heartbeat_at=now - 300)
    other_file = _write_claim(
        tmp_path, product=product, domain=d_other, last_heartbeat_at=now - 300
    )

    original_list_claims = manager.repo.list_claims
    call_count = {"n": 0}

    def racy_list_claims(*, product: str | None = None) -> list[ClaimInfo]:
        call_count["n"] += 1
        # First re-check corresponds to d_race (first in sort order): simulate
        # a live pod refreshing d_race's heartbeat before we see it.
        if call_count["n"] == 1:
            _write_claim(
                tmp_path,
                product=str(product),
                domain=d_race,
                last_heartbeat_at=time.time(),
            )
        return original_list_claims(product=product)

    monkeypatch.setattr(manager.repo, "list_claims", racy_list_claims)

    try:
        result = manager.clear_stale_claims(product=product, dry_run=False)
    finally:
        manager.close()

    assert d_race.identifier in result.previewed
    assert d_other.identifier in result.previewed
    assert d_race.identifier in result.skipped_fresh
    assert d_race.identifier not in result.cleared
    assert d_other.identifier in result.cleared
    assert race_file.exists(), "refreshed claim must not be cleared"
    assert not other_file.exists()


def test_race_claim_deleted_by_another_operator(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Another operator deletes the file between preview and mutation → skipped_missing."""
    manager = _manager(tmp_path)
    product = "product.zarr"
    now = time.time()

    # Iteration order is sorted by domain identifier; "a_gone" sorts before "z_other".
    d_gone = WriteDomain(product=product, category="zarr_append", name="a_gone")
    d_other = WriteDomain(product=product, category="zarr_append", name="z_other")

    gone_file = _write_claim(tmp_path, product=product, domain=d_gone, last_heartbeat_at=now - 300)
    other_file = _write_claim(
        tmp_path, product=product, domain=d_other, last_heartbeat_at=now - 300
    )

    original_list_claims = manager.repo.list_claims
    call_count = {"n": 0}

    def racy_list_claims(*, product: str | None = None) -> list[ClaimInfo]:
        call_count["n"] += 1
        # First re-check corresponds to d_gone: simulate another operator
        # unlinking the file concurrently before this iteration sees it.
        if call_count["n"] == 1 and gone_file.exists():
            gone_file.unlink()
        return original_list_claims(product=product)

    monkeypatch.setattr(manager.repo, "list_claims", racy_list_claims)

    try:
        result = manager.clear_stale_claims(product=product, dry_run=False)
    finally:
        manager.close()

    assert d_gone.identifier in result.previewed
    assert d_other.identifier in result.previewed
    assert d_gone.identifier in result.skipped_missing
    assert d_gone.identifier not in result.cleared
    assert d_other.identifier in result.cleared
    assert not gone_file.exists()
    assert not other_file.exists()


def test_no_stale_claims_returns_empty_result(tmp_path: Path) -> None:
    manager = _manager(tmp_path)
    product = "product.zarr"
    now = time.time()
    fresh = _write_claim(
        tmp_path,
        product=product,
        domain=WriteDomain(product=product, category="zarr_append", name="fresh"),
        last_heartbeat_at=now - 30,
    )

    try:
        result = manager.clear_stale_claims(product=product, dry_run=False)
    finally:
        manager.close()

    assert isinstance(result, ClearSweepResult)
    assert result.previewed == []
    assert result.cleared == []
    assert result.skipped_fresh == []
    assert result.skipped_missing == []
    assert fresh.exists()


def test_repeated_call_is_idempotent(tmp_path: Path) -> None:
    """Second bulk-clear must not error and must not double-count deletions."""
    manager = _manager(tmp_path)
    product = "product.zarr"
    now = time.time()

    stale_domains = [
        WriteDomain(product=product, category="zarr_append", name=f"stale-{i}") for i in range(5)
    ]
    files = [
        _write_claim(tmp_path, product=product, domain=d, last_heartbeat_at=now - 300)
        for d in stale_domains
    ]

    try:
        first = manager.clear_stale_claims(product=product, dry_run=False)
        second = manager.clear_stale_claims(product=product, dry_run=False)
    finally:
        manager.close()

    assert sorted(first.cleared) == sorted(d.identifier for d in stale_domains)
    # After the first sweep the claim files are gone, so the second sweep
    # sees no stale claims to preview or clear — no exceptions, no false positives.
    assert second.previewed == []
    assert second.cleared == []
    assert second.skipped_fresh == []
    assert second.skipped_missing == []
    for f in files:
        assert not f.exists()
