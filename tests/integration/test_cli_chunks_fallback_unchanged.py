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

"""T15: storage-config fallback stays GREEN for non-target chunks subcommands.

T8 added an early-return branch in
``firecube.cli.chunks._manager.resolve_manager`` for the case where both
``--target`` and ``--product-name`` are provided. The historical
storage-config fallback path (no ``--target``) MUST remain unchanged.

These tests pin that contract for the five chunks subcommands that do NOT
expose ``--target``:

- ``chunks list``
- ``chunks delete``
- ``chunks snapshots status``
- ``chunks claims list``

Each test configures storage via a ``[storage]`` section in a temporary
TOML, optionally pre-populates control-plane state under the resolved
root using a binding pointing at the same on-disk location the fallback
will pick, and invokes the subcommand without ``--target``. Assertions
check a Firecube-visible effect (output content, control-plane state) —
never just the exit code.

Why share the resolved root? The fallback synthesises a binding at
``<target_path>/__firecube_controlplane__`` whose ``_base_uri`` is
``<target_path>``. ``_ControlRootResolver`` then routes any non-synthetic
product through ``ensure_product_uri(<target_path>, product)``, which
puts the per-product control root at ``<target_path>/<product>/.firecube``.
The test pre-population uses ``make_test_binding(<target_path>,
product=<product>)``, which lands at the same control root.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from click.testing import CliRunner

from firecube.cli.main import cli
from firecube.core.controlplane import ChunkManager
from firecube.core.controlplane.types import SpanCoverage
from tests.helpers.storage import make_test_binding

pytestmark = pytest.mark.integration


_CONFIG_TEMPLATE = """\
[storage]
type = "local"
target_path = "{target_path}"
"""


def _write_config(tmp_path: Path) -> Path:
    """Write a TOML config that points the fallback at ``tmp_path``."""
    config_file = tmp_path / "firecube-test.toml"
    config_file.write_text(
        _CONFIG_TEMPLATE.format(target_path=tmp_path),
        encoding="utf-8",
    )
    return config_file


def _clean_storage_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Strip storage env vars that would override the config file."""
    for name in (
        "FIRECUBE_STORAGE_TYPE",
        "FIRECUBE_TARGET_PATH",
        "FIRECUBE_BUCKET",
        "FIRECUBE_CONFIG",
        "FIRECUBE_ENDPOINT_URL",
        "FIRECUBE_ACCESS_KEY",
        "FIRECUBE_SECRET_KEY",
    ):
        monkeypatch.delenv(name, raising=False)


def _populate_completed_span(tmp_path: Path, *, product: str, workspace: Path) -> str:
    """Record a complete span for ``product`` at the fallback control-plane root.

    Both the CLI fallback (``target_path = tmp_path``) and this binding
    resolve to ``tmp_path/<product>/.firecube/`` — so anything written
    here is what the CLI will see.
    """
    binding = make_test_binding(tmp_path, product=product)
    manager = ChunkManager(binding=binding, workspace=workspace)
    run_id = "fallback-run-001"
    try:
        manager.record_run_started(
            product=product,
            run_id=run_id,
            output_path=str(tmp_path / product),
            output_format="zarr",
            size=0,
            meta={"plugin": "test"},
        )
        manager.record_span(
            product=product,
            run_id=run_id,
            batch_id="batch-1",
            group="F024",
            status="active",
            coverage=SpanCoverage(
                group="F024",
                arrays=["F024/FWI"],
                time_index_ranges=[[0, 0]],
                aligned=True,
            ),
            meta={
                "group": "F024",
                "time_min": "2024-01-01T00:00:00Z",
                "time_max": "2024-01-01T00:00:00Z",
            },
        )
        manager.record_run_terminal(
            product=product,
            run_id=run_id,
            output_path=str(tmp_path / product),
            output_format="zarr",
            size=0,
            meta={"plugin": "test"},
            status="complete",
        )
    finally:
        manager.close()
    return run_id


def test_chunks_list_fallback_finds_chunks(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """``chunks list`` without ``--target`` reads the storage-config target."""
    product = "my_product"
    workspace = tmp_path / "ws"
    workspace.mkdir()
    _clean_storage_env(monkeypatch)
    config_file = _write_config(tmp_path)
    _populate_completed_span(tmp_path, product=product, workspace=workspace)

    result = CliRunner().invoke(
        cli,
        [
            "--config-file",
            str(config_file),
            "chunks",
            "--workspace",
            str(workspace),
            "list",
            "--product-name",
            product,
            "--format",
            "json",
        ],
    )

    assert result.exit_code == 0, result.output
    payload = json.loads(result.stdout)
    assert isinstance(payload, list) and payload, (
        f"fallback must surface the pre-populated chunk, got: {payload!r}"
    )
    assert all(entry["product"] == product for entry in payload), payload


def test_chunks_delete_dry_run_fallback(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """``chunks delete --dry-run`` resolves chunks via the storage-config target."""
    product = "my_product"
    workspace = tmp_path / "ws"
    workspace.mkdir()
    _clean_storage_env(monkeypatch)
    config_file = _write_config(tmp_path)
    _populate_completed_span(tmp_path, product=product, workspace=workspace)

    runs_dir = tmp_path / product / ".firecube" / "runs"
    assert runs_dir.is_dir() and any(runs_dir.iterdir()), (
        "pre-condition: populator must create at least one run directory"
    )

    result = CliRunner().invoke(
        cli,
        [
            "--config-file",
            str(config_file),
            "chunks",
            "--workspace",
            str(workspace),
            "delete",
            "--product-name",
            product,
            "--dry-run",
            "--yes-i-really-mean-it",
        ],
    )

    assert result.exit_code == 0, result.output
    assert "DRY RUN - Would delete:" in result.output, result.output
    # dry-run must NOT mutate the pre-populated control plane
    assert runs_dir.is_dir() and any(runs_dir.iterdir()), (
        "dry-run must not delete pre-existing runs"
    )


def test_chunks_snapshots_status_fallback(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """``chunks snapshots status`` reports against the storage-config target."""
    product = "my_product"
    workspace = tmp_path / "ws"
    workspace.mkdir()
    _clean_storage_env(monkeypatch)
    config_file = _write_config(tmp_path)

    result = CliRunner().invoke(
        cli,
        [
            "--config-file",
            str(config_file),
            "chunks",
            "--workspace",
            str(workspace),
            "snapshots",
            "status",
            "--product-name",
            product,
            "--format",
            "json",
        ],
    )

    assert result.exit_code == 0, result.output
    payload = json.loads(result.stdout)
    assert payload == {"exists": False}, (
        f"fallback against an empty product must report no snapshot, got: {payload!r}"
    )


def test_chunks_claims_list_fallback(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """``chunks claims list`` enumerates claims at the storage-config target."""
    product = "my_product"
    workspace = tmp_path / "ws"
    workspace.mkdir()
    _clean_storage_env(monkeypatch)
    config_file = _write_config(tmp_path)

    result = CliRunner().invoke(
        cli,
        [
            "--config-file",
            str(config_file),
            "chunks",
            "--workspace",
            str(workspace),
            "claims",
            "list",
            "--product-name",
            product,
            "--format",
            "json",
        ],
    )

    assert result.exit_code == 0, result.output
    payload = json.loads(result.stdout)
    assert payload == [], (
        f"fallback against an empty product must report no claims, got: {payload!r}"
    )
