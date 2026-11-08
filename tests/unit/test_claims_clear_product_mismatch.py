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
import os
import time
from pathlib import Path
from typing import Any

import pytest
from click.testing import CliRunner

from firecube.cli.main import cli
from firecube.core.controlplane import ChunkManager
from firecube.core.controlplane.types import WriteDomain
from firecube.core.errors import ProductLabelMismatchError
from firecube.core.storage.uri import StorageUri
from tests.helpers.rows import table_row
from tests.helpers.storage import make_test_binding

pytestmark = pytest.mark.unit


LOGICAL_PRODUCT = "logical_name"
STORE_PRODUCT = "logical_name_20260501.zarr"
DOMAIN = WriteDomain(product=LOGICAL_PRODUCT, category="coord_materialization", name="all")


def _manager(tmp_path: Path, *, product: str = STORE_PRODUCT) -> ChunkManager:
    workspace = tmp_path / "workspace"
    workspace.mkdir(exist_ok=True)
    binding = make_test_binding(tmp_path, product=product, label=LOGICAL_PRODUCT)
    return ChunkManager(binding=binding, workspace=workspace)


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


def _stale_claim(tmp_path: Path) -> Path:
    return _write_claim(
        tmp_path,
        product=STORE_PRODUCT,
        domain=DOMAIN,
        last_heartbeat_at=time.time() - 400.0,
    )


def _cli_env() -> dict[str, str]:
    env = {
        key: value
        for key, value in os.environ.items()
        if key
        not in {
            "AWS_ACCESS_KEY_ID",
            "AWS_SECRET_ACCESS_KEY",
            "AWS_SESSION_TOKEN",
            "AWS_SECURITY_TOKEN",
            "AWS_DEFAULT_REGION",
            "FIRECUBE_ACCESS_KEY",
            "FIRECUBE_SECRET_KEY",
            "FIRECUBE_S3_ANONYMOUS",
        }
    }
    env["FIRECUBE_S3_ANONYMOUS"] = "false"
    return env


def test_claim_clear_product_name_matching(subtests: Any, tmp_path: Path) -> None:
    with table_row(
        subtests, "clear_claim_takes_the_recorded_label_and_refuses_the_uri", tmp_path
    ) as (
        workdir,
        _,
    ):
        claim_path = _stale_claim(workdir)
        manager = _manager(workdir)

        try:
            with pytest.raises(ProductLabelMismatchError):
                manager.clear_claim(product=STORE_PRODUCT, domain_id=DOMAIN.identifier, force=False)
            assert claim_path.exists(), "a refused label must clear nothing"
            cleared = manager.clear_claim(
                product=LOGICAL_PRODUCT,
                domain_id=DOMAIN.identifier,
                force=False,
            )
        finally:
            manager.close()

        assert cleared is True
        assert not claim_path.exists()

    with table_row(
        subtests, "read_claim_by_domain_takes_the_recorded_label_and_refuses_the_uri", tmp_path
    ) as (workdir, _):
        _stale_claim(workdir)
        manager = _manager(workdir)

        try:
            manager.list_claims(product=LOGICAL_PRODUCT)
            assert manager.repo.claims is not None
            with pytest.raises(ProductLabelMismatchError):
                manager.repo.claims.read_claim_by_domain(
                    product=STORE_PRODUCT, domain=DOMAIN.identifier
                )
            claim = manager.repo.claims.read_claim_by_domain(
                product=LOGICAL_PRODUCT,
                domain=DOMAIN.identifier,
            )
        finally:
            manager.close()

        assert claim is not None
        assert claim.product == LOGICAL_PRODUCT
        assert claim.domain == DOMAIN.identifier

    with table_row(subtests, "clear_claim_still_rejects_malformed_domain", tmp_path) as (
        workdir,
        _,
    ):
        manager = _manager(workdir)

        try:
            with pytest.raises(ValueError):
                manager.clear_claim(
                    product=STORE_PRODUCT, domain_id="malformed_no_colons", force=False
                )
        finally:
            manager.close()

    with table_row(
        subtests,
        "cli_all_stale_clears_claim_when_target_name_differs_from_recorded_label",
        tmp_path,
    ) as (workdir, _):
        claim_path = _stale_claim(workdir)
        store_uri = StorageUri.from_local_path(workdir / STORE_PRODUCT).to_str()

        result = CliRunner().invoke(
            cli,
            [
                "chunks",
                "claims",
                "clear",
                "--target",
                store_uri,
                "--workspace",
                str(workdir / "workspace"),
                "--all-stale",
                "--yes-i-really-mean-it",
            ],
            env=_cli_env(),
        )

        assert result.exit_code == 0, result.output
        assert "Cleared stale claims:" in result.output
        assert DOMAIN.identifier in result.output
        assert not claim_path.exists()
