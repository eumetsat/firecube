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

"""Enumeration-cost regression tests for ``list_time_coord_consolidations``.

The seal event is written to a single canonical run directory
(``.firecube/runs/time-coord-consolidation/``). Reading it back must NOT scan
every run entry under ``.firecube/runs/``: the cost of enforcing the seal on a
hot-path (resume-guard) call has to stay independent of how many completed
ingest runs happen to be recorded in the control plane.

All three tests below observe filesystem traffic through a path-recording
subclass of the shared :class:`CountingFilesystem`. The subclass extends the
base with per-path lists for ``ls``/``open``/``exists`` and augments
``reset()`` so seeding traffic can be cleared before the API-under-test call.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from firecube.core.controlplane import ChunkManager
from firecube.core.controlplane.events import ConsolidatedTimeCoord
from firecube.core.storage.uri import StorageUri
from tests.helpers.storage import make_test_binding
from tests.unit._helpers.counting_fs import CountingFilesystem, make_counting_local_fs

pytestmark = pytest.mark.unit

PRODUCT = "product.zarr"
PLUGIN = "seal-scan-plugin"
SEAL_RUN_ID = "time-coord-consolidation"


class _PathCountingFilesystem(CountingFilesystem):
    """CountingFilesystem variant that records per-path ``ls``/``open``/``exists``.

    The base class only tallies per-operation counters, which is not enough to
    prove that ``list_time_coord_consolidations`` never touches ordinary run
    directories. Recording paths lets each test assert both a total-cost bound
    and structural exclusivity ("ordinary run directories saw zero traffic").

    ``reset()`` is extended so seed-time filesystem activity does not pollute
    assertions on the subsequent API-under-test call.
    """

    def __init__(self, fs: Any) -> None:
        super().__init__(fs)
        self.ls_paths: list[str] = []
        self.open_paths: list[str] = []
        self.exists_paths: list[str] = []

    def reset(self) -> None:
        super().reset()
        self.ls_paths.clear()
        self.open_paths.clear()
        self.exists_paths.clear()

    def ls(self, uri: StorageUri, detail: bool = False) -> list[Any]:
        self.ls_paths.append(uri.path)
        return super().ls(uri, detail=detail)

    def exists(self, uri: StorageUri) -> bool:
        self.exists_paths.append(uri.path)
        return super().exists(uri)

    def open(self, uri: StorageUri, mode: str = "rb") -> Any:
        self.open_paths.append(uri.path)
        return super().open(uri, mode)


def _counting_manager(
    tmp_path: Path, *, product: str = PRODUCT
) -> tuple[ChunkManager, _PathCountingFilesystem]:
    """Build a ChunkManager backed by a path-recording counting filesystem."""

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


def _seed_completed_runs(manager: ChunkManager, *, run_count: int) -> None:
    """Record N completed runs via the canonical manager API.

    Uses the same idiom as ``tests/unit/test_resume_guard_telemetry.py`` so the
    on-disk layout is whatever the current writer produces — no hand-crafted
    directory structure and no fictitious slug format.
    """

    for index in range(run_count):
        run_id = f"run-{index:03d}"
        meta = {"plugin": PLUGIN, "sequence": index}
        manager.record_run_started(
            product=PRODUCT,
            run_id=run_id,
            output_path=f"file:///tmp/{run_id}",
            output_format="zarr",
            size=1,
            meta=meta,
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


def _ordinary_run_paths(
    paths: list[str], *, product: str = PRODUCT, seal_run_id: str = SEAL_RUN_ID
) -> list[str]:
    """Return the subset of recorded paths that reference NON-seal run directories.

    A path belongs to an ordinary run iff it lives under
    ``<product>/.firecube/runs/<something>`` where ``<something>`` is not the
    seal run id. Paths at the ``.firecube/runs/`` root itself (without a run
    segment) are not counted here — the tests assert on those separately.
    Paths are compared by substring so an arbitrary tmp-dir prefix
    (``/tmp/pytest-.../``) does not defeat the seal-vs-ordinary check.
    """

    runs_marker = f"/{product}/.firecube/runs/"
    offenders: list[str] = []
    for path in paths:
        idx = path.find(runs_marker)
        if idx < 0:
            continue
        tail = path[idx + len(runs_marker) :]
        if not tail:
            continue
        run_segment = tail.split("/", 1)[0]
        if run_segment == seal_run_id:
            continue
        offenders.append(path)
    return offenders


def _not_under_seal_dir(
    paths: list[str], *, product: str = PRODUCT, seal_run_id: str = SEAL_RUN_ID
) -> list[str]:
    """Return the subset of recorded paths that are NOT under the canonical seal dir.

    A path is "under the canonical seal directory" iff it equals
    ``<...>/<product>/.firecube/runs/<seal_run_id>`` or lives beneath it
    (``<...>/<product>/.firecube/runs/<seal_run_id>/...``). Any other path is a
    violation of the seal-only-read invariant enforced by
    ``list_time_coord_consolidations``.

    The check is substring-based so an arbitrary tmp-dir prefix
    (``/tmp/pytest-.../``) does not defeat it, and the tail after the marker is
    checked to avoid false positives from run ids that merely start with the
    seal id (e.g. a hypothetical ``time-coord-consolidation-other``).
    """

    seal_dir = f"/{product}/.firecube/runs/{seal_run_id}"
    offenders: list[str] = []
    for path in paths:
        idx = path.find(seal_dir)
        if idx < 0:
            offenders.append(path)
            continue
        tail_start = idx + len(seal_dir)
        if tail_start >= len(path) or path[tail_start] == "/":
            continue
        offenders.append(path)
    return offenders


def test_list_time_coord_consolidations_returns_empty_when_no_seal_run_dir_exists(
    tmp_path: Path,
) -> None:
    """No seal run means an empty result and NO enumeration of ordinary runs.

    Even with N ordinary runs already recorded, the seal check must probe only
    the canonical seal directory. It must never list ``.firecube/runs/`` and
    must never list, open, or exists-probe any ordinary run directory.
    """

    manager, counting_fs = _counting_manager(tmp_path)
    try:
        _seed_completed_runs(manager, run_count=3)
        counting_fs.reset()

        result = manager.list_time_coord_consolidations(product=PRODUCT)
    finally:
        manager.close()

    assert result == []

    assert counting_fs.open_paths == [], (
        f"list_time_coord_consolidations must not open any path when no seal run "
        f"dir exists; recorded open paths: {counting_fs.open_paths}"
    )

    assert _not_under_seal_dir(counting_fs.ls_paths) == [], (
        f"list_time_coord_consolidations recorded ls calls outside the canonical "
        f"seal dir; offending paths: {_not_under_seal_dir(counting_fs.ls_paths)}"
    )

    assert _not_under_seal_dir(counting_fs.exists_paths) == [], (
        f"list_time_coord_consolidations recorded exists probes outside the "
        f"canonical seal dir; offending paths: "
        f"{_not_under_seal_dir(counting_fs.exists_paths)}"
    )


def test_list_time_coord_consolidations_request_count_independent_of_run_count(
    tmp_path: Path,
) -> None:
    """Filesystem traffic is O(1) in the number of completed runs.

    Builds the fixture at N=3 and N=30 and compares the total number of
    ``ls`` + ``open`` + ``exists`` calls made by
    ``list_time_coord_consolidations``. On the pre-fix code the total scales
    linearly (roughly 3N + a constant per run entry read); after the fix it
    should be a fixed, small constant.
    """

    def _run_case(root: Path, *, n: int) -> tuple[int, int, int]:
        root.mkdir()
        manager, counting_fs = _counting_manager(root)
        try:
            _seed_completed_runs(manager, run_count=n)
            counting_fs.reset()
            result = manager.list_time_coord_consolidations(product=PRODUCT)
        finally:
            manager.close()
        assert result == []
        return counting_fs.counts["ls"], counting_fs.counts["open"], counting_fs.counts["exists"]

    ls_small, open_small, exists_small = _run_case(tmp_path / "case_small", n=3)
    ls_large, open_large, exists_large = _run_case(tmp_path / "case_large", n=30)

    total_small = ls_small + open_small + exists_small
    total_large = ls_large + open_large + exists_large

    assert total_small == total_large, (
        f"list_time_coord_consolidations request count scales with run count: "
        f"N=3 → ls={ls_small}, open={open_small}, exists={exists_small} "
        f"(total {total_small}); "
        f"N=30 → ls={ls_large}, open={open_large}, exists={exists_large} "
        f"(total {total_large}). Must be identical (O(1) in N)."
    )


def test_list_time_coord_consolidations_returns_event_when_canonical_seal_run_present(
    tmp_path: Path,
) -> None:
    """The seal event round-trips WITHOUT touching ordinary run directories."""

    manager, counting_fs = _counting_manager(tmp_path)
    timestamp_iso = "2026-08-27T12:00:00+00:00"
    groups = ("F024",)

    try:
        _seed_completed_runs(manager, run_count=5)
        manager.record_time_coord_consolidation(groups, timestamp_iso)
        counting_fs.reset()

        result = manager.list_time_coord_consolidations(product=PRODUCT)
    finally:
        manager.close()

    assert result == [
        ConsolidatedTimeCoord(
            run_id=SEAL_RUN_ID,
            timestamp_iso=timestamp_iso,
            groups=groups,
        )
    ]

    assert _not_under_seal_dir(counting_fs.ls_paths) == [], (
        f"list_time_coord_consolidations recorded ls calls outside the canonical "
        f"seal dir in seal-present case; offending paths: "
        f"{_not_under_seal_dir(counting_fs.ls_paths)}"
    )
    assert _not_under_seal_dir(counting_fs.exists_paths) == [], (
        f"list_time_coord_consolidations recorded exists probes outside the "
        f"canonical seal dir in seal-present case; offending paths: "
        f"{_not_under_seal_dir(counting_fs.exists_paths)}"
    )
    assert _not_under_seal_dir(counting_fs.open_paths) == [], (
        f"list_time_coord_consolidations opened paths outside the canonical seal "
        f"dir in seal-present case; offending paths: "
        f"{_not_under_seal_dir(counting_fs.open_paths)}"
    )

    assert _ordinary_run_paths(counting_fs.ls_paths) == [], (
        f"list_time_coord_consolidations listed ordinary run directories: "
        f"{_ordinary_run_paths(counting_fs.ls_paths)}"
    )
    assert _ordinary_run_paths(counting_fs.open_paths) == [], (
        f"list_time_coord_consolidations opened ordinary run entries: "
        f"{_ordinary_run_paths(counting_fs.open_paths)}"
    )
    assert _ordinary_run_paths(counting_fs.exists_paths) == [], (
        f"list_time_coord_consolidations exists-probed ordinary run entries: "
        f"{_ordinary_run_paths(counting_fs.exists_paths)}"
    )
