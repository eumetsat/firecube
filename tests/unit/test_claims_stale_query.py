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
import time
from pathlib import Path

import pytest

from firecube.core.controlplane import ChunkManager
from firecube.core.controlplane.types import WriteDomain
from firecube.core.errors import ControlPlaneCorruptionError
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


def test_list_stale_claims_returns_empty_list_when_claims_dir_is_empty(tmp_path: Path) -> None:
    manager = _manager(tmp_path)
    product = "product.zarr"

    try:
        assert manager.list_stale_claims(product=product) == []
    finally:
        manager.close()


def test_list_stale_claims_returns_only_stale_claims(tmp_path: Path) -> None:
    manager = _manager(tmp_path)
    product = "product.zarr"
    now = time.time()

    _write_claim(
        tmp_path,
        product=product,
        domain=WriteDomain(product=product, category="zarr_append", name="stale-a"),
        last_heartbeat_at=now - 300,
    )
    _write_claim(
        tmp_path,
        product=product,
        domain=WriteDomain(product=product, category="zarr_append", name="stale-b"),
        last_heartbeat_at=now - 300,
    )
    _write_claim(
        tmp_path,
        product=product,
        domain=WriteDomain(product=product, category="zarr_append", name="fresh"),
        last_heartbeat_at=now - 30,
    )

    try:
        stale_claims = manager.list_stale_claims(product=product)

        assert len(stale_claims) == 2
        assert {info.domain for info in stale_claims} == {
            WriteDomain(product=product, category="zarr_append", name="stale-a").identifier,
            WriteDomain(product=product, category="zarr_append", name="stale-b").identifier,
        }
    finally:
        manager.close()


def test_list_stale_claims_excludes_boundary_fresh_claim(tmp_path: Path) -> None:
    manager = _manager(tmp_path)
    product = "product.zarr"
    now = time.time()

    _write_claim(
        tmp_path,
        product=product,
        domain=WriteDomain(product=product, category="zarr_append", name="boundary"),
        last_heartbeat_at=now - 119,
    )

    try:
        assert manager.list_stale_claims(product=product) == []
    finally:
        manager.close()


def test_list_stale_claims_raises_on_malformed_json(tmp_path: Path) -> None:
    manager = _manager(tmp_path)
    product = "product.zarr"
    claims_dir = _claims_dir(tmp_path, product)
    claims_dir.mkdir(parents=True, exist_ok=True)
    malformed = _claim_path(
        tmp_path,
        product,
        WriteDomain(product=product, category="zarr_append", name="broken"),
    )
    malformed.write_text("{not-json", encoding="utf-8")

    try:
        with pytest.raises(ControlPlaneCorruptionError):
            manager.list_stale_claims(product=product)
    finally:
        manager.close()
