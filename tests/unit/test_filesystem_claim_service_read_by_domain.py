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

"""Tests for targeted filesystem claim reads by write-domain identifier."""

from __future__ import annotations

import json
import time
from pathlib import Path

import pytest

from firecube.core.controlplane.claims import FilesystemClaimService
from firecube.core.controlplane.types import CLAIMS_DIRNAME, CONTROL_DIRNAME, WriteDomain
from firecube.core.errors import ControlPlaneCorruptionError
from firecube.core.filesystem import FsspecFilesystem
from firecube.core.storage.uri import StorageUri
from tests.helpers.storage import make_test_binding

pytestmark = pytest.mark.unit


class _Resolver:
    base_uri: StorageUri

    def __init__(self, root: Path) -> None:
        self._root = root
        self.base_uri = StorageUri.from_local_path(root)

    def __call__(self, product: str) -> tuple[StorageUri, StorageUri]:
        control_path = StorageUri.from_local_path(self._root / product / CONTROL_DIRNAME)
        return control_path, control_path


def _service(tmp_path: Path, *, product: str) -> FilesystemClaimService:
    binding = make_test_binding(tmp_path, product=product)
    fs = FsspecFilesystem.from_binding(binding)
    return FilesystemClaimService(fs=fs, control_root_resolver=_Resolver(tmp_path))


def _claim_path(tmp_path: Path, *, product: str, domain: WriteDomain) -> Path:
    return tmp_path / product / CONTROL_DIRNAME / CLAIMS_DIRNAME / domain.claim_name


def _write_claim(tmp_path: Path, *, product: str, domain: WriteDomain) -> Path:
    claim_path = _claim_path(tmp_path, product=product, domain=domain)
    claim_path.parent.mkdir(parents=True, exist_ok=True)
    now = time.time()
    payload = {
        "product": product,
        "domain": domain.identifier,
        "owner_id": "owner-1",
        "claim_path": StorageUri.from_local_path(claim_path).to_str(),
        "acquired_at": now,
        "last_heartbeat_at": now,
        "heartbeat_interval_s": 30,
        "stale_threshold_s": 120,
    }
    claim_path.write_text(json.dumps(payload), encoding="utf-8")
    return claim_path


def test_read_claim_by_domain_returns_claim_info_for_existing_domain(tmp_path: Path) -> None:
    product = "product.zarr"
    domain = WriteDomain(product=product, category="zarr_append", name="slot-0")
    claim_path = _write_claim(tmp_path, product=product, domain=domain)
    service = _service(tmp_path, product=product)

    info = service.read_claim_by_domain(product=product, domain=domain.identifier)

    assert info is not None
    assert info.product == product
    assert info.domain == domain.identifier
    assert info.owner_id == "owner-1"
    assert info.claim_path == StorageUri.from_local_path(claim_path).to_str()
    assert info.heartbeat_interval_s == 30
    assert info.stale_threshold_s == 120


def test_read_claim_by_domain_returns_none_for_missing_domain(tmp_path: Path) -> None:
    product = "product.zarr"
    domain = WriteDomain(product=product, category="zarr_append", name="missing")
    service = _service(tmp_path, product=product)

    assert service.read_claim_by_domain(product=product, domain=domain.identifier) is None


def test_read_claim_by_domain_raises_on_malformed_json(tmp_path: Path) -> None:
    product = "product.zarr"
    domain = WriteDomain(product=product, category="zarr_append", name="bad-json")
    claim_path = _claim_path(tmp_path, product=product, domain=domain)
    claim_path.parent.mkdir(parents=True, exist_ok=True)
    claim_path.write_text("{not-json", encoding="utf-8")
    service = _service(tmp_path, product=product)

    with pytest.raises(ControlPlaneCorruptionError, match="Corrupt claim file"):
        service.read_claim_by_domain(product=product, domain=domain.identifier)
