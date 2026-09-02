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

"""Integration tests for the run-lifecycle invariants of ``firecube zarr preallocate``.

``ensure_slot_index_model`` creates a non-terminal ``run.json``
(``status="started"``). Before this fix, the command never drove that run to a
terminal state, so ``ChunkManager.list_runs(non_terminal=True)`` kept
returning the stuck ``("preallocate", "started")`` entry. ``ResumeGuard`` then
blocked the next slot-range ingest with ``ResumeConflictError``.

These tests pin the fix:

* On success, the preallocate run is recorded ``status="complete"`` AND the
  underlying ``ChunkManager`` is closed (WAL events flushed).
* On a failure that happens AFTER ``ensure_slot_index_model`` succeeds, the
  run is recorded ``status="failed"`` (not left ``started``).
* As a regression check, ``ResumeGuard._check_non_terminal_runs`` does not
  raise on a slot-range run after a successful preallocate.
"""

from __future__ import annotations

import importlib
import logging
from collections.abc import Iterator
from pathlib import Path

import pytest
from click.testing import CliRunner

from firecube.cli.main import cli
from firecube.core.controlplane import ChunkManager
from firecube.core.product.identity import ProductIdentity
from firecube.core.storage.binding import StorageBinding
from firecube.core.storage.driver_config import StorageDriverConfig
from firecube.core.storage.uri import StorageUri
from firecube.ingestor.errors import ConfigurationError
from firecube.ingestor.registry import loader as _loader
from firecube.ingestor.runtime.resume_guard import ResumeGuard

pytestmark = pytest.mark.integration

PRODUCT_NAME = "direct_zarr_capable_test_product"
PLUGIN_NAME = "direct_zarr_capable_test_plugin"


@pytest.fixture(autouse=True)
def reset_plugin_registry() -> Iterator[None]:
    original_loaded = _loader._LOADED
    original_registry = dict(_loader.AVAILABLE_INGESTORS)
    _loader._LOADED = False
    _loader.AVAILABLE_INGESTORS.clear()
    importlib.reload(importlib.import_module(PLUGIN_NAME))
    yield
    _loader._LOADED = original_loaded
    _loader.AVAILABLE_INGESTORS.clear()
    _loader.AVAILABLE_INGESTORS.update(original_registry)


def _cli_args(target_dir: Path) -> list[str]:
    return [
        "zarr",
        "preallocate",
        PLUGIN_NAME,
        "--target",
        f"file://{target_dir}",
        "--product-name",
        PRODUCT_NAME,
        "--storage-type",
        "local",
        "--storage-driver",
        "fsspec",
        "--write-mode",
        "direct",
    ]


def _open_manager(target_dir: Path) -> ChunkManager:
    product_uri = StorageUri.from_local_path(target_dir)
    binding = StorageBinding(
        identity=ProductIdentity.from_uri(product_uri, "zarr", product_name=PRODUCT_NAME),
        driver=StorageDriverConfig(),
    )
    return ChunkManager(binding=binding)


def test_preallocate_records_terminal_run_on_success(tmp_path: Path) -> None:
    target_dir = tmp_path / "out.zarr"

    result = CliRunner().invoke(cli, _cli_args(target_dir))

    assert result.exit_code == 0, result.output

    cm = _open_manager(target_dir)
    try:
        non_terminal = cm.list_runs(product=PRODUCT_NAME, non_terminal=True)
        assert non_terminal == [], (
            f"preallocate left non-terminal run(s) for {PRODUCT_NAME}: "
            f"{[(r.run_id, r.status) for r in non_terminal]}"
        )

        all_runs = cm.list_runs(product=PRODUCT_NAME)
        preallocate_runs = [r for r in all_runs if r.run_id.startswith("preallocate")]
        assert preallocate_runs, f"no preallocate run was recorded; runs={all_runs!r}"
        assert all(r.is_terminal for r in preallocate_runs), (
            f"preallocate runs are not terminal: {[(r.run_id, r.status) for r in preallocate_runs]}"
        )
        assert any(r.status == "complete" for r in preallocate_runs), (
            f"no preallocate run with status='complete'; "
            f"statuses={[r.status for r in preallocate_runs]}"
        )
    finally:
        cm.close()


def test_preallocate_records_failed_run_on_post_slot_index_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A failure AFTER ensure_slot_index_model must terminate the run, not leak it."""
    target_dir = tmp_path / "out.zarr"
    import direct_zarr_capable_test_plugin as plugin_module

    def explode(self, ctx):  # type: ignore[no-untyped-def]
        raise ConfigurationError("induced post-slot-index failure")

    monkeypatch.setattr(
        plugin_module.DirectZarrCapableTestIngestor,
        "zarr_schema",
        explode,
    )

    result = CliRunner().invoke(cli, _cli_args(target_dir))

    assert result.exit_code != 0, result.output

    cm = _open_manager(target_dir)
    try:
        non_terminal = cm.list_runs(product=PRODUCT_NAME, non_terminal=True)
        assert non_terminal == [], (
            f"preallocate failure left non-terminal run(s) for {PRODUCT_NAME}: "
            f"{[(r.run_id, r.status) for r in non_terminal]}"
        )

        all_runs = cm.list_runs(product=PRODUCT_NAME)
        preallocate_runs = [r for r in all_runs if r.run_id.startswith("preallocate")]
        assert preallocate_runs, f"no preallocate run was recorded; runs={all_runs!r}"
        assert any(r.status == "failed" for r in preallocate_runs), (
            f"no preallocate run with status='failed'; "
            f"statuses={[r.status for r in preallocate_runs]}"
        )
    finally:
        cm.close()


def test_preallocate_does_not_block_subsequent_slot_range_run(tmp_path: Path) -> None:
    """ResumeGuard must not see a non-terminal preallocate run after success."""
    target_dir = tmp_path / "out.zarr"

    result = CliRunner().invoke(cli, _cli_args(target_dir))
    assert result.exit_code == 0, result.output

    cm = _open_manager(target_dir)
    try:
        guard = ResumeGuard(
            plugin_name=PLUGIN_NAME,
            chunk_manager=cm,
            log=logging.getLogger("test_preallocate_lifecycle"),
            slice_meta_keys=(),
        )

        guard._check_non_terminal_runs(
            product=PRODUCT_NAME,
            force_reingest=False,
            resume_existing=False,
            slot_range=(0, 100),
            new_slot_group=None,
        )
    finally:
        cm.close()
