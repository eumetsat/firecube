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
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest

from firecube.core.controlplane.manager import ChunkManager
from firecube.core.controlplane.types import SpanCoverage
from firecube.core.storage.uri import StorageUri
from firecube.ingestor.errors import ResumeConflictError
from firecube.ingestor.runtime.resume_guard import ResumeGuard
from tests.helpers.storage import make_test_binding
from tests.unit._helpers.counting_fs import CountingFilesystem, make_counting_local_fs

PRODUCT = "product.zarr"
PLUGIN = "op-count-plugin"


class PathCountingFilesystem(CountingFilesystem):
    """CountingFilesystem variant that records which paths were listed."""

    def __init__(self, fs: Any) -> None:
        super().__init__(fs)
        self.ls_paths: list[str] = []

    def ls(self, uri: StorageUri, detail: bool = False) -> list[Any]:
        self.ls_paths.append(uri.path)
        return super().ls(uri, detail=detail)


def _make_ctx(**options: Any) -> MagicMock:
    ctx = MagicMock()
    ctx.force_reingest = bool(options.pop("force_reingest", False))
    ctx.option.side_effect = lambda name, default=None: options.get(name, default)
    return ctx


def _make_manager(tmp_path: Path) -> tuple[ChunkManager, PathCountingFilesystem]:
    _counting_fs, real_fs = make_counting_local_fs(tmp_path)
    counting_fs = PathCountingFilesystem(real_fs)
    manager = ChunkManager(
        binding=make_test_binding(tmp_path, product=PRODUCT),
        workspace=tmp_path,
        filesystem=counting_fs,
    )
    return manager, counting_fs


def _make_guard(manager: ChunkManager) -> ResumeGuard:
    return ResumeGuard(
        plugin_name=PLUGIN,
        chunk_manager=manager,
        log=logging.getLogger(__name__),
        slice_meta_keys=(),
    )


def _seed_completed_runs(manager: ChunkManager, *, count: int = 10) -> None:
    for index in range(count):
        run_id = f"run-{index:02d}"
        meta = {"plugin": PLUGIN, "sequence": index}
        manager.record_run_started(
            product=PRODUCT,
            run_id=run_id,
            output_path=f"file:///tmp/{run_id}",
            output_format="zarr",
            size=1,
            meta=meta,
        )
        if index == 0:
            manager.record_span(
                PRODUCT,
                run_id,
                "batch-0",
                "group-a",
                "active",
                coverage=SpanCoverage(
                    group="group-a",
                    arrays=["data"],
                    time_index_ranges=[[0, 9]],
                ),
                meta={"plugin": PLUGIN},
            )
        manager.record_run_terminal(
            product=PRODUCT,
            run_id=run_id,
            output_path=f"file:///tmp/{run_id}",
            output_format="zarr",
            size=1,
            meta=meta,
            status="complete",
        )


def _runs_ls_count(counting_fs: PathCountingFilesystem) -> int:
    return sum(1 for path in counting_fs.ls_paths if path.endswith("/.firecube/runs"))


@pytest.mark.unit
def test_enforce_lists_runs_directory_once_across_scan_passes(tmp_path: Path) -> None:
    manager, counting_fs = _make_manager(tmp_path)
    _seed_completed_runs(manager)
    counting_fs.reset()
    counting_fs.ls_paths.clear()

    _make_guard(manager).enforce(
        ctx=_make_ctx(),
        product=PRODUCT,
        slot_range=(10, 20),
        slot_group="group-a",
    )

    assert _runs_ls_count(counting_fs) == 1


@pytest.mark.unit
def test_cache_restores_on_exception(tmp_path: Path) -> None:
    manager, _counting_fs = _make_manager(tmp_path)
    guard = _make_guard(manager)
    manager.record_run_started(
        product=PRODUCT,
        run_id="active-run",
        output_path="file:///tmp/active-run",
        output_format="zarr",
        size=1,
        meta={"plugin": PLUGIN},
    )

    with manager.repo.run_entries_cache_scope():
        outer_cache = manager.repo._run_entries_cache

        with pytest.raises(ResumeConflictError, match="Non-terminal run"):
            guard.enforce(ctx=_make_ctx(), product=PRODUCT)

        assert manager.repo._run_entries_cache is outer_cache

    assert manager.repo._run_entries_cache is None


@pytest.mark.unit
def test_nested_enforce_scope_restores_outer_cache(tmp_path: Path) -> None:
    manager, _counting_fs = _make_manager(tmp_path)
    guard = _make_guard(manager)

    with manager.repo.run_entries_cache_scope():
        outer_cache = manager.repo._run_entries_cache

        guard.enforce(ctx=_make_ctx(), product=PRODUCT)

        assert manager.repo._run_entries_cache is outer_cache

    assert manager.repo._run_entries_cache is None
