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

from click.testing import CliRunner

from firecube.cli.chunks._claims import claims_group
from firecube.cli.chunks._runs import runs_group
from firecube.cli.chunks._snapshots import snapshots_group
from firecube.core.controlplane import ChunkManager
from firecube.core.controlplane.types import ClaimInfo, RunInfo
from tests.helpers.storage import make_test_binding


class _FakeClaimsManager:
    def __init__(self) -> None:
        self.cleared: list[tuple[str, str, bool]] = []

    def list_claims(self, *, product=None):
        _ = product
        return [
            ClaimInfo(
                product="TEST_PRODUCT.zarr",
                domain="TEST_PRODUCT.zarr:zarr_group:F024",
                owner_id="run-001",
                claim_path="s3://bucket/TEST_PRODUCT.zarr/.firecube/claims/abc.json",
                acquired_at=time.time(),
                last_heartbeat_at=time.time(),
                heartbeat_interval_s=30,
                stale_threshold_s=120,
            )
        ]

    def clear_claim(self, *, product, domain_id, force=False):
        self.cleared.append((product, domain_id, force))
        return True


class _FakeSnapshotManager:
    def rebuild_snapshot(self, product):
        return {"product": product, "generation": "123", "records": 4}


class _FakeRunsManager:
    def __init__(self) -> None:
        self.abandoned: list[tuple[str, str, str]] = []

    def list_runs(self, *, product, status=None, non_terminal=False):
        _ = status
        _ = non_terminal
        return [
            RunInfo(
                product=product,
                run_id="run-001",
                status="started",
                run_dir="/tmp/run-001",
                run_uri="file:///tmp/run-001",
                started_at=time.time(),
                updated_at=time.time(),
                completed_at=None,
                events=2,
                parts=1,
            )
        ]

    def abandon_run(self, *, product, run_id, reason, meta=None):
        _ = meta
        self.abandoned.append((product, run_id, reason))
        return {"product": product, "run_id": run_id, "status": "abandoned", "abandoned": True}


def test_chunks_claims_list_json(monkeypatch):
    runner = CliRunner()
    monkeypatch.setattr(
        "firecube.cli.chunks._claims.resolve_manager",
        lambda *args, **kwargs: _FakeClaimsManager(),
    )

    result = runner.invoke(claims_group, ["list", "--format", "json"], obj={})

    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload[0]["domain"] == "TEST_PRODUCT.zarr:zarr_group:F024"
    assert payload[0]["stale"] is False


def test_chunks_claims_clear(monkeypatch):
    runner = CliRunner()
    manager = _FakeClaimsManager()
    product_uri = "file:///tmp/TEST_PRODUCT.zarr"
    monkeypatch.setattr(
        "firecube.cli.chunks._claims.resolve_manager",
        lambda *args, **kwargs: manager,
    )

    result = runner.invoke(
        claims_group,
        [
            "clear",
            "--product-name",
            product_uri,
            "--domain",
            "TEST_PRODUCT.zarr:zarr_group:F024",
            "--force",
            "--yes-i-really-mean-it",
        ],
        obj={},
    )

    assert result.exit_code == 0
    assert manager.cleared == [("TEST_PRODUCT.zarr", "TEST_PRODUCT.zarr:zarr_group:F024", True)]


def test_chunks_snapshots_rebuild_json(monkeypatch):
    runner = CliRunner()
    product_uri = "file:///tmp/TEST_PRODUCT.zarr"
    monkeypatch.setattr(
        "firecube.cli.chunks._snapshots.resolve_manager",
        lambda *args, **kwargs: _FakeSnapshotManager(),
    )

    result = runner.invoke(
        snapshots_group,
        ["rebuild", "--product-name", product_uri, "--format", "json"],
        obj={},
    )

    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["generation"] == "123"


def test_chunks_runs_list_json(monkeypatch):
    runner = CliRunner()
    product_uri = "file:///tmp/TEST_PRODUCT.zarr"
    monkeypatch.setattr(
        "firecube.cli.chunks._runs.resolve_manager",
        lambda *args, **kwargs: _FakeRunsManager(),
    )

    result = runner.invoke(
        runs_group,
        ["list", "--product-name", product_uri, "--format", "json"],
        obj={},
    )

    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload[0]["run_id"] == "run-001"
    assert payload[0]["stale"] is False


def test_chunks_runs_list_help_shows_status():
    runner = CliRunner()

    result = runner.invoke(runs_group, ["list", "--help"], obj={})

    assert result.exit_code == 0
    assert "--status" in result.output


def test_chunks_runs_list_filters_status_with_real_manager(tmp_path, monkeypatch):
    runner = CliRunner()
    manager = ChunkManager(binding=make_test_binding(tmp_path), workspace=tmp_path)
    product = "TEST_PRODUCT.zarr"

    manager.record_run_started(
        product=product,
        run_id="run-started",
        output_path=str(tmp_path / product),
        output_format="zarr",
        size=0,
        meta={"plugin": "test"},
    )
    manager.record_run_started(
        product=product,
        run_id="run-complete",
        output_path=str(tmp_path / product),
        output_format="zarr",
        size=0,
        meta={"plugin": "test"},
    )
    manager.record_run_terminal(
        product=product,
        run_id="run-complete",
        output_path=str(tmp_path / product),
        output_format="zarr",
        size=1,
        meta={"plugin": "test"},
        status="complete",
    )
    monkeypatch.setattr(
        "firecube.cli.chunks._runs.resolve_manager", lambda *args, **kwargs: manager
    )

    product_uri = (tmp_path / product).as_uri()
    result = runner.invoke(
        runs_group, ["list", "--product-name", product_uri, "--status", "started"], obj={}
    )

    assert result.exit_code == 0
    assert "run-started" in result.output
    assert "run-complete" not in result.output


def test_chunks_runs_abandon(monkeypatch):
    runner = CliRunner()
    manager = _FakeRunsManager()
    product_uri = "file:///tmp/TEST_PRODUCT.zarr"
    monkeypatch.setattr(
        "firecube.cli.chunks._runs.resolve_manager",
        lambda *args, **kwargs: manager,
    )

    result = runner.invoke(
        runs_group,
        [
            "abandon",
            "--product-name",
            product_uri,
            "--run-id",
            "run-001",
            "--reason",
            "stale",
            "--yes-i-really-mean-it",
        ],
        obj={},
    )

    assert result.exit_code == 0
    assert manager.abandoned == [("TEST_PRODUCT.zarr", "run-001", "stale")]
