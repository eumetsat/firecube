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
- Concurrent-operator idempotency: two overlapping --all-stale sweeps against
  the same product must not double-clear any claim.
- Enumeration-cost regression (CountingFilesystem): sweep must bound the number
  of claims-directory listings independent of stale count S.
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

import pytest

from firecube.core.controlplane import ChunkManager
from firecube.core.controlplane.claims import FilesystemClaimService
from firecube.core.controlplane.types import ClaimInfo, ClearSweepResult, WriteDomain
from firecube.core.storage.uri import StorageUri
from tests.helpers.storage import make_test_binding
from tests.unit._helpers.counting_fs import CountingFilesystem, make_counting_local_fs


def _manager(tmp_path: Path, *, product: str = "product.zarr") -> ChunkManager:
    workspace = tmp_path / "workspace"
    workspace.mkdir(exist_ok=True)
    return ChunkManager(binding=make_test_binding(tmp_path, product=product), workspace=workspace)


class _PathCountingFilesystem(CountingFilesystem):
    """CountingFilesystem variant that records which paths were listed."""

    def __init__(self, fs: Any) -> None:
        super().__init__(fs)
        self.ls_paths: list[str] = []

    def ls(self, uri: StorageUri, detail: bool = False) -> list[Any]:
        self.ls_paths.append(uri.path)
        return super().ls(uri, detail=detail)


def _counting_manager(
    tmp_path: Path, *, product: str = "product.zarr"
) -> tuple[ChunkManager, _PathCountingFilesystem]:
    workspace = tmp_path / "workspace"
    workspace.mkdir(exist_ok=True)
    _wrapped, real_fs = make_counting_local_fs(tmp_path)
    counting_fs = _PathCountingFilesystem(real_fs)
    manager = ChunkManager(
        binding=make_test_binding(tmp_path, product=product),
        workspace=workspace,
        filesystem=counting_fs,
    )
    return manager, counting_fs


def _claims_ls_count(counting_fs: _PathCountingFilesystem) -> int:
    """Count list calls targeting the ``.firecube/claims`` directory."""
    return sum(1 for path in counting_fs.ls_paths if path.endswith("/.firecube/claims"))


pytestmark = pytest.mark.unit


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

    original_read_claim_by_domain = FilesystemClaimService.read_claim_by_domain
    call_count = {"n": 0}

    def racy_read_claim_by_domain(
        self: FilesystemClaimService, *, product: str, domain: str
    ) -> ClaimInfo | None:
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
        return original_read_claim_by_domain(self, product=product, domain=domain)

    monkeypatch.setattr(FilesystemClaimService, "read_claim_by_domain", racy_read_claim_by_domain)

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

    original_read_claim_by_domain = FilesystemClaimService.read_claim_by_domain
    call_count = {"n": 0}

    def racy_read_claim_by_domain(
        self: FilesystemClaimService, *, product: str, domain: str
    ) -> ClaimInfo | None:
        call_count["n"] += 1
        # First re-check corresponds to d_gone: simulate another operator
        # unlinking the file concurrently before this iteration sees it.
        if call_count["n"] == 1 and gone_file.exists():
            gone_file.unlink()
        return original_read_claim_by_domain(self, product=product, domain=domain)

    monkeypatch.setattr(FilesystemClaimService, "read_claim_by_domain", racy_read_claim_by_domain)

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


def test_concurrent_all_stale_clears_are_idempotent(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Two overlapping --all-stale claim sweeps must not double-clear any claim.

    Simulates two operators (two ``ChunkManager`` instances against the same
    on-disk control-plane root) running ``clear_stale_claims`` concurrently.
    Uses deterministic monkeypatch interleaving instead of threads: when
    operator A is about to call ``clear_claim`` on the 5th stale claim,
    operator B's full sweep runs first, then A resumes.

    Operator B's ``list_stale_claims`` is captured up-front so its preview
    holds all 10 stale claims — as it would if both operators listed at nearly
    the same wall time in a real race. The mutation-time re-check inside each
    sweep is what must actually keep the sweeps idempotent.

    Invariants proven:
      - Neither operator raises.
      - Every stale claim file is removed exactly once — the union of
        ``cleared`` across both operators covers all stale domains with no
        overlap.
      - Claims the other operator already removed surface as
        ``skipped_missing`` on the losing side (no exception, no silent
        double-count into ``cleared``).
      - No stale claim file remains on disk.
    """
    manager_a = _manager(tmp_path)
    manager_b = _manager(tmp_path)
    product = "product.zarr"
    now = time.time()

    n_stale = 10
    stale_domains = [
        WriteDomain(product=product, category="zarr_append", name=f"stale-{i}")
        for i in range(n_stale)
    ]
    stale_files = [
        _write_claim(tmp_path, product=product, domain=d, last_heartbeat_at=now - 300)
        for d in stale_domains
    ]

    cached_b_preview = manager_b.list_stale_claims(product=product)
    assert len(cached_b_preview) == n_stale
    assert sorted(c.domain for c in cached_b_preview) == sorted(d.identifier for d in stale_domains)

    def cached_list_stale_claims_for_b(*, product: str) -> list[ClaimInfo]:
        return list(cached_b_preview)

    monkeypatch.setattr(manager_b.repo, "list_stale_claims", cached_list_stale_claims_for_b)

    original_a_clear_claim = manager_a.repo.clear_claim
    a_call_count = {"n": 0}
    b_result_holder: dict[str, Any] = {}
    trigger_at = 5

    def racy_a_clear_claim(*, product: str, domain_id: str, force: bool = False) -> bool:
        a_call_count["n"] += 1
        if a_call_count["n"] == trigger_at and "result" not in b_result_holder:
            b_result_holder["result"] = manager_b.clear_stale_claims(product=product, dry_run=False)
        return original_a_clear_claim(product=product, domain_id=domain_id, force=force)

    monkeypatch.setattr(manager_a.repo, "clear_claim", racy_a_clear_claim)

    try:
        result_a = manager_a.clear_stale_claims(product=product, dry_run=False)
    finally:
        manager_a.close()
        manager_b.close()

    assert "result" in b_result_holder, "interleave did not fire; test setup is broken"
    result_b = b_result_holder["result"]

    assert isinstance(result_a, ClearSweepResult)
    assert isinstance(result_b, ClearSweepResult)

    a_pre_interleave = sorted(stale_domains[i].identifier for i in range(trigger_at - 1))
    a_post_interleave = sorted(stale_domains[i].identifier for i in range(trigger_at - 1, n_stale))

    assert sorted(result_a.cleared) == a_pre_interleave
    assert sorted(result_a.skipped_missing) == a_post_interleave
    assert result_a.skipped_fresh == []

    assert sorted(result_b.cleared) == a_post_interleave
    assert sorted(result_b.skipped_missing) == a_pre_interleave
    assert result_b.skipped_fresh == []

    assert set(result_a.cleared).isdisjoint(result_b.cleared), (
        "concurrent sweeps must not both claim the same clear"
    )
    all_domain_ids = {d.identifier for d in stale_domains}
    assert set(result_a.cleared) | set(result_b.cleared) == all_domain_ids

    for f in stale_files:
        assert not f.exists(), f"stale claim file {f} must be removed after both sweeps"


def test_enumeration_bounded_by_stale_claim_count(tmp_path: Path) -> None:
    """clear_stale_claims must not re-list the claims directory once per stale item.

    Both the mutation loop's re-check and the delete itself derive the claim
    path directly, so the number of claims-directory listings stays bounded by
    the preview regardless of how many claims turn out to be stale. Re-listing
    per candidate would make a sweep cost O(N x S).
    """
    manager, counting_fs = _counting_manager(tmp_path)
    product = "product.zarr"
    now = time.time()

    stale_domains = [
        WriteDomain(product=product, category="zarr_append", name=f"stale-{i}") for i in range(10)
    ]
    fresh_domains = [
        WriteDomain(product=product, category="zarr_append", name=f"fresh-{i}") for i in range(10)
    ]
    for d in stale_domains:
        _write_claim(tmp_path, product=product, domain=d, last_heartbeat_at=now - 300)
    for d in fresh_domains:
        _write_claim(tmp_path, product=product, domain=d, last_heartbeat_at=now - 30)

    counting_fs.reset()
    counting_fs.ls_paths.clear()

    try:
        result = manager.clear_stale_claims(product=product, dry_run=False)
    finally:
        manager.close()

    observed = _claims_ls_count(counting_fs)
    assert observed <= 2, (
        f"clear_stale_claims listed claims_dir {observed} times for N=20, S=10; "
        "must be bounded (<= 2) independent of stale count. "
        f"Recorded ls paths on claims_dir: "
        f"{[p for p in counting_fs.ls_paths if p.endswith('/.firecube/claims')]}"
    )
    assert len(result.cleared) == len(stale_domains)


def test_enumeration_bounded_scales_with_N_not_S_claims(tmp_path: Path) -> None:
    """Enumeration cost is constant across differing stale counts.

    Runs (N=50, S=5) and (N=20, S=15) and asserts the number of
    claims-directory listings is bounded in each and varies by at most 1
    between the two, pinning the cost to N rather than S.
    """
    product = "product.zarr"
    now = time.time()

    def _run_case(root: Path, *, stale: int, fresh: int) -> tuple[int, int]:
        root.mkdir()
        mgr, counting = _counting_manager(root, product=product)
        for i in range(stale):
            _write_claim(
                root,
                product=product,
                domain=WriteDomain(product=product, category="zarr_append", name=f"stale-{i}"),
                last_heartbeat_at=now - 300,
            )
        for i in range(fresh):
            _write_claim(
                root,
                product=product,
                domain=WriteDomain(product=product, category="zarr_append", name=f"fresh-{i}"),
                last_heartbeat_at=now - 30,
            )
        counting.reset()
        counting.ls_paths.clear()
        try:
            result = mgr.clear_stale_claims(product=product, dry_run=False)
        finally:
            mgr.close()
        return _claims_ls_count(counting), len(result.cleared)

    ls_a, cleared_a = _run_case(tmp_path / "case_a", stale=5, fresh=45)
    ls_b, cleared_b = _run_case(tmp_path / "case_b", stale=15, fresh=5)

    assert cleared_a == 5
    assert cleared_b == 15
    assert ls_a <= 2, f"case A (N=50, S=5): {ls_a} claims_dir listings"
    assert ls_b <= 2, f"case B (N=20, S=15): {ls_b} claims_dir listings"
    assert abs(ls_a - ls_b) <= 1, (
        f"listing count scales with stale count S: A(S=5)={ls_a}, B(S=15)={ls_b}"
    )
