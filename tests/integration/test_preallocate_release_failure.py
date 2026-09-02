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

"""Success-path claim-release failure must propagate, not silently swallow.

When ``firecube zarr preallocate`` finishes successfully, its cleanup
block releases the coord-materialization claim held for the duration of
the run. If that release call raises (e.g. transient object-store I/O
error), the operator must see a non-zero exit and the traceback of the
underlying error: silently swallowing it leaves a stale claim on the
control-plane that blocks every future preallocate for the product until
``firecube chunks claims clear`` is run by a human. This is the same
loud-failure discipline the surrounding ``record_run_terminal`` block
already applies — the release block was the outlier.

The test replaces ``ClaimHandle.release`` with a stub that raises
``OSError("simulated release failure")``. The stub plugin machinery
mirrors ``test_preallocate_idempotent.py`` / ``test_preallocate_recovery.py``
under a distinct plugin name so the three suites can coexist in one
session.
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


_STUB_PLUGIN_NAME = "preallocate_release_failure_test_plugin"
_EPOCH_ISO = "2024-01-01T00:00:00Z"
_CADENCE_S = 600
_SLOT_COUNT = 4
_COORD_NAME = "time"
_GROUP = "data"
_VALUES_ARRAY = "values"
_VALUES_X_DIM = 2
_DEFAULT_OFFSET_S = 2


class PreallocateReleaseFailureIngestor(DirectZarrIngestor):
    """Minimal bounded-axis stub ingestor for the release-failure scenario.

    Declares a single ``RegularTimeAxis(mode="floor", slot_count=4)`` on
    one group. The plugin returns items for every slot so the preallocate
    run completes successfully — that is the whole point of the scenario:
    the release failure must be injected on the *success* path.
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
    """Install ``PreallocateReleaseFailureIngestor`` under a stable name per test."""
    original_loaded = _loader._LOADED
    original_registry = dict(_loader.AVAILABLE_INGESTORS)

    _loader._LOADED = True
    _loader.AVAILABLE_INGESTORS.clear()
    PreallocateReleaseFailureIngestor.name = _STUB_PLUGIN_NAME  # pyright: ignore[reportAttributeAccessIssue]
    _loader.AVAILABLE_INGESTORS[_STUB_PLUGIN_NAME] = PreallocateReleaseFailureIngestor
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


def test_preallocate_release_failure_reraises_on_success(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Success-path ``ClaimHandle.release`` failure must propagate + log.

    Injects ``OSError`` from ``ClaimHandle.release`` so the preallocate
    run reaches the success-path cleanup with a broken release. The CLI
    must:

    * exit non-zero (Click surfaces the propagated exception),
    * emit the existing ``logger.error`` line about the stale-claim
      remediation (unchanged wording),
    * expose the underlying ``OSError`` (attached to the ``CliRunner``
      result as ``.exception`` when Click propagates it upward).

    A silently-swallowed release failure would leave ``exit_code == 0``
    and no exception on the runner result — the exact stale-claim leak
    this fix closes.
    """
    target = tmp_path / "release-failure.zarr"
    source = _make_source_dir(tmp_path)
    runner = CliRunner()

    real_release = ClaimHandle.release
    release_call_count = {"n": 0}

    def _failing_release(self: ClaimHandle) -> None:
        # Perform the real teardown first so heartbeat threads/files are
        # cleaned up, THEN raise. This models an object-store error that
        # surfaces during the ``rm`` step. The stub only injects a
        # failure for the ``coord_materialization`` claim held by the
        # preallocate command's cleanup block — the other short-lived
        # claim ``ensure_resolved_index`` acquires (domain category
        # ``resolved_index``) must still release cleanly so the run
        # actually reaches the success-path cleanup that this test is
        # exercising.
        real_release(self)
        if ":coord_materialization:" in self.info.domain:
            release_call_count["n"] += 1
            raise OSError("simulated release failure")

    monkeypatch.setattr(ClaimHandle, "release", _failing_release)

    # Attach a dedicated capture handler to the CLI's named logger so the
    # ``logger.error`` assertion is robust across pytest capture modes.
    # ``configure_logging`` clears root handlers each CLI invocation, which
    # would orphan any handler installed on the root logger by pytest itself;
    # a handler on the named logger survives that clear.
    firecube_cli_logger = logging.getLogger("firecube.cli.zarr")
    log_records: list[logging.LogRecord] = []

    class _MemoryHandler(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            log_records.append(record)

    memory_handler = _MemoryHandler(level=logging.ERROR)
    firecube_cli_logger.addHandler(memory_handler)
    try:
        result = runner.invoke(cli, _preallocate_args(target, source))
    finally:
        firecube_cli_logger.removeHandler(memory_handler)

    assert release_call_count["n"] >= 1, (
        "test harness sanity: the coord_materialization claim's monkey-patched "
        f"release must be exercised by the success-path cleanup; observed "
        f"{release_call_count['n']!r} coord_materialization calls, "
        f"output={result.output!r}"
    )
    assert result.exit_code != 0, (
        "success-path release failure must propagate to a non-zero exit "
        "(a silent swallow leaves a stale claim on the control-plane); "
        f"got exit_code={result.exit_code!r}, output={result.output!r}"
    )
    assert isinstance(result.exception, OSError), (
        "the propagated exception must be the underlying OSError from the "
        "release stub, not an unrelated failure; "
        f"got {type(result.exception).__name__}: {result.exception!r}\n"
        f"output={result.output!r}"
    )
    assert "simulated release failure" in str(result.exception), (
        f"OSError message must match the injected text: {result.exception!r}"
    )
    error_messages = [record.getMessage() for record in log_records]
    assert any("failed to release materialization claim" in msg for msg in error_messages), (
        "existing logger.error line must fire before the re-raise; "
        f"captured error messages: {error_messages!r}"
    )
