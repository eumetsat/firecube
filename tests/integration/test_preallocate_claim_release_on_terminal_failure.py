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

"""Success-path ``record_run_terminal`` failure must not leak the coord claim.

When ``firecube zarr preallocate`` finishes successfully but the
terminal ``chunk_manager.record_run_terminal`` call raises (e.g. transient
WAL write error), the previous implementation re-raised at the failure site
inside the outer ``finally``. That skipped the claim-release block that ran
immediately after, leaving a stale ``coord_materialization`` claim on the
control-plane. The stale claim blocks every future preallocate for the
product until an operator runs ``firecube chunks claims clear`` by hand.

The fix wraps the terminal-record block in an inner ``try/finally`` that
releases the claim first, defers the terminal-record exception via a local
``_terminal_exc``, and re-raises it after release. If the release itself
also fails, the terminal-record exception (root cause) still propagates
and the release failure is logged separately.

Two scenarios cover the invariant:

* ``test_preallocate_terminal_failure_releases_claim`` — terminal record
  fails, release succeeds: claim file is GONE and the terminal exception
  propagates. This is the claim-leak regression test.
* ``test_preallocate_both_failures_propagate_terminal_error`` — terminal
  record fails AND release also fails: the terminal exception (root cause)
  wins over the release exception, and the release error is logged.

The stub-plugin machinery mirrors ``test_preallocate_release_failure.py``
under a distinct plugin name so all preallocate-failure suites can coexist
in one session.
"""

from __future__ import annotations

import datetime as dt
import logging
from collections.abc import Iterator
from pathlib import Path
from typing import Any, ClassVar

import pytest
from click.testing import CliRunner

from firecube.cli.main import cli
from firecube.core.api import IndexSpec, ItemInfo, RegularTimeAxis
from firecube.core.controlplane.claims import ClaimHandle
from firecube.core.controlplane.manager import ChunkManager
from firecube.ingestor.api import (
    DirectZarrIngestor,
    PipelineBatch,
    PluginContext,
    WriteIntent,
    ZarrArraySpec,
    ZarrGroupSpec,
)
from firecube.ingestor.registry import loader as _loader

pytestmark = pytest.mark.integration


_STUB_PLUGIN_NAME = "preallocate_terminal_failure_test_plugin"
_EPOCH_ISO = "2024-01-01T00:00:00Z"
_CADENCE_S = 600
_SLOT_COUNT = 4
_COORD_NAME = "time"
_GROUP = "data"
_VALUES_ARRAY = "values"
_VALUES_X_DIM = 2
_DEFAULT_OFFSET_S = 2


class PreallocateTerminalFailureIngestor(DirectZarrIngestor):
    """Bounded-axis stub used to reach the success path before terminal record.

    Declares one ``RegularTimeAxis(mode="floor", slot_count=4)`` on a single
    group with an ``int`` item per slot. The plugin never returns write
    intents — preallocate only materializes the coord chunk plan; the actual
    ingest write path is out of scope here.
    """

    PRODUCT_NAME: ClassVar[str] = _STUB_PLUGIN_NAME
    time_dim_name: ClassVar[str] = _COORD_NAME

    def index_spec(self, ctx: PluginContext) -> IndexSpec:
        return IndexSpec(
            name=f"{_STUB_PLUGIN_NAME}_v1",
            groups={
                _GROUP: RegularTimeAxis(
                    coordinate=_COORD_NAME,
                    epoch=_EPOCH_ISO,
                    cadence_s=_CADENCE_S,
                    mode="floor",
                    slot_count=_SLOT_COUNT,
                ),
            },
        )

    def inspect_item(self, item: Any, ctx: PluginContext) -> ItemInfo | None:
        if not isinstance(item, int):
            return None
        slot = int(item)
        seconds_since_epoch = slot * _CADENCE_S + _DEFAULT_OFFSET_S
        coord = dt.datetime(2024, 1, 1, tzinfo=dt.UTC) + dt.timedelta(seconds=seconds_since_epoch)
        return ItemInfo(coordinate=coord)

    def discover_source_files(self, ctx: PluginContext) -> list[Any]:
        return list(range(_SLOT_COUNT))

    def zarr_schema(self, ctx: PluginContext) -> list[ZarrGroupSpec]:
        return [
            ZarrGroupSpec(
                group=_GROUP,
                arrays=[
                    ZarrArraySpec(
                        name=_VALUES_ARRAY,
                        shape=(_SLOT_COUNT, _VALUES_X_DIM),
                        dtype="float32",
                        chunks=(1, _VALUES_X_DIM),
                        fill_value=0.0,
                        expected_time_count=_SLOT_COUNT,
                        time_indexed=True,
                        dimension_names=(_COORD_NAME, "x"),
                    ),
                ],
            )
        ]

    def build_write_intents(self, batch: PipelineBatch, ctx: PluginContext) -> list[WriteIntent]:
        return []


@pytest.fixture(autouse=True)
def _register_stub_plugin() -> Iterator[None]:
    original_loaded = _loader._LOADED
    original_registry = dict(_loader.AVAILABLE_INGESTORS)

    _loader._LOADED = True
    _loader.AVAILABLE_INGESTORS.clear()
    PreallocateTerminalFailureIngestor.name = _STUB_PLUGIN_NAME  # pyright: ignore[reportAttributeAccessIssue]
    _loader.AVAILABLE_INGESTORS[_STUB_PLUGIN_NAME] = PreallocateTerminalFailureIngestor
    try:
        yield
    finally:
        _loader._LOADED = original_loaded
        _loader.AVAILABLE_INGESTORS.clear()
        _loader.AVAILABLE_INGESTORS.update(original_registry)


def _preallocate_args(target: Path, source: Path) -> list[str]:
    return [
        "zarr",
        "preallocate",
        _STUB_PLUGIN_NAME,
        "--target",
        f"file://{target}",
        "--product-name",
        _STUB_PLUGIN_NAME,
        "--storage-type",
        "local",
        "--storage-driver",
        "fsspec",
        "--write-mode",
        "direct",
        "--input-data",
        str(source),
        "--option",
        "no_progress=true",
    ]


def _make_source_dir(tmp_path: Path) -> Path:
    source_dir = tmp_path / "src"
    source_dir.mkdir()
    return source_dir


def _install_error_logger_capture() -> tuple[
    logging.Logger, list[logging.LogRecord], logging.Handler
]:
    firecube_cli_logger = logging.getLogger("firecube.cli.zarr")
    log_records: list[logging.LogRecord] = []

    class _MemoryHandler(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            log_records.append(record)

    memory_handler = _MemoryHandler(level=logging.ERROR)
    firecube_cli_logger.addHandler(memory_handler)
    return firecube_cli_logger, log_records, memory_handler


def _coord_materialization_claims_on_disk(target: Path, product: str) -> list[Path]:
    """Return the raw claim file paths for the coord_materialization domain.

    Uses direct disk inspection (not ``ChunkManager.list_claims``) so the
    claim-file assertion is orthogonal to the control-plane API surface. The
    preallocate command writes claims under
    ``<target-parent>/<product>/.firecube/claims/`` — a stale file there is
    exactly what a leaked claim leaves behind.
    """
    claims_dir = target.parent / product / ".firecube" / "claims"
    if not claims_dir.exists():
        return []
    return [p for p in claims_dir.iterdir() if "coord_materialization" in p.name]


def test_preallocate_terminal_failure_releases_claim(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Terminal-record failure on success path must release the claim.

    Injects ``OSError`` from ``ChunkManager.record_run_terminal`` so the
    preallocate cleanup block sees a success-path terminal-record failure.
    The command must:

    * exit non-zero (the terminal-record exception propagates),
    * expose the injected ``OSError`` as ``result.exception``,
    * emit the ``logger.error`` line about the non-terminal run,
    * leave NO ``coord_materialization`` claim file on disk (the release
      block ran despite the terminal-record failure).

    A regression would leave a claim file present, blocking future
    preallocates until an operator runs ``firecube chunks claims clear``.
    """
    target = tmp_path / "terminal-fails.zarr"
    source = _make_source_dir(tmp_path)
    runner = CliRunner()

    terminal_call_count = {"n": 0}

    def _failing_terminal(self: ChunkManager, **kwargs: Any) -> None:
        terminal_call_count["n"] += 1
        raise OSError("simulated terminal-record failure")

    monkeypatch.setattr(ChunkManager, "record_run_terminal", _failing_terminal)

    _, log_records, handler = _install_error_logger_capture()
    try:
        result = runner.invoke(cli, _preallocate_args(target, source))
    finally:
        logging.getLogger("firecube.cli.zarr").removeHandler(handler)

    assert terminal_call_count["n"] >= 1, (
        "test harness sanity: the monkey-patched record_run_terminal must "
        f"be exercised by the success-path cleanup; observed "
        f"{terminal_call_count['n']!r} calls, output={result.output!r}"
    )
    assert result.exit_code != 0, (
        "terminal-record failure on the success path must propagate; "
        f"got exit_code={result.exit_code!r}, output={result.output!r}"
    )
    assert isinstance(result.exception, OSError), (
        "the propagated exception must be the injected OSError, not an "
        "unrelated failure; "
        f"got {type(result.exception).__name__}: {result.exception!r}\n"
        f"output={result.output!r}"
    )
    assert "simulated terminal-record failure" in str(result.exception), (
        f"OSError message must match the injected text: {result.exception!r}"
    )
    error_messages = [record.getMessage() for record in log_records]
    assert any(
        "failed to record terminal state for preallocate run" in msg for msg in error_messages
    ), (
        "existing logger.error line for terminal-record failure must fire; "
        f"captured error messages: {error_messages!r}"
    )
    leaked_claims = _coord_materialization_claims_on_disk(target, _STUB_PLUGIN_NAME)
    assert leaked_claims == [], (
        "Claim-leak regression: coord_materialization claim file must be released "
        "before the terminal-record exception propagates; found stale "
        f"claim file(s): {[str(p) for p in leaked_claims]!r}\n"
        f"output={result.output!r}"
    )


def test_preallocate_both_failures_propagate_terminal_error(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Terminal-record + release both fail: terminal exception wins.

    Injects failures into BOTH ``ChunkManager.record_run_terminal`` and
    ``ClaimHandle.release`` (only for the coord_materialization domain, so
    the short-lived resolved_index claim still releases cleanly). The
    terminal-record failure is the root cause and must be the exception
    that reaches the CLI runner. The release failure must be logged
    separately without shadowing the terminal-record exception.
    """
    target = tmp_path / "both-fail.zarr"
    source = _make_source_dir(tmp_path)
    runner = CliRunner()

    real_release = ClaimHandle.release
    release_failure_count = {"n": 0}
    terminal_failure_count = {"n": 0}

    def _failing_release(self: ClaimHandle) -> None:
        real_release(self)
        if ":coord_materialization:" in self.info.domain:
            release_failure_count["n"] += 1
            raise OSError("simulated release failure")

    def _failing_terminal(self: ChunkManager, **kwargs: Any) -> None:
        terminal_failure_count["n"] += 1
        raise RuntimeError("simulated terminal-record failure")

    monkeypatch.setattr(ChunkManager, "record_run_terminal", _failing_terminal)
    monkeypatch.setattr(ClaimHandle, "release", _failing_release)

    _, log_records, handler = _install_error_logger_capture()
    try:
        result = runner.invoke(cli, _preallocate_args(target, source))
    finally:
        logging.getLogger("firecube.cli.zarr").removeHandler(handler)

    assert terminal_failure_count["n"] >= 1, (
        f"test harness sanity: monkey-patched record_run_terminal must be "
        f"exercised; observed {terminal_failure_count['n']!r} calls, "
        f"output={result.output!r}"
    )
    assert release_failure_count["n"] >= 1, (
        f"test harness sanity: monkey-patched ClaimHandle.release must be "
        f"exercised for coord_materialization; observed "
        f"{release_failure_count['n']!r} calls, output={result.output!r}"
    )
    assert result.exit_code != 0, (
        f"either failure must propagate to a non-zero exit; "
        f"got exit_code={result.exit_code!r}, output={result.output!r}"
    )
    assert isinstance(result.exception, RuntimeError), (
        "the terminal-record RuntimeError (root cause) must win over the "
        "release OSError; "
        f"got {type(result.exception).__name__}: {result.exception!r}\n"
        f"output={result.output!r}"
    )
    assert "simulated terminal-record failure" in str(result.exception), (
        f"propagated exception message must be the terminal-record one: {result.exception!r}"
    )
    error_messages = [record.getMessage() for record in log_records]
    assert any(
        "failed to record terminal state for preallocate run" in msg for msg in error_messages
    ), f"terminal-record failure must be logged; captured error messages: {error_messages!r}"
    assert any(
        "failed to release materialization claim for run" in msg for msg in error_messages
    ), (
        "release failure must be logged separately even though it is not "
        "the propagated exception; "
        f"captured error messages: {error_messages!r}"
    )
