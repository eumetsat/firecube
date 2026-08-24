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

"""Documented resume behavior for callable static payloads.

On first write the engine resolves the callable and writes the result.
On resume (marker present) the engine resolves the callable again AND reads
the on-disk array for comparison. Both arrays are live simultaneously, so the
peak is approximately 2x the payload size. This is a known limitation stated
in the ``_dispatch_static_intent`` docstring and in the plugin guide.

This test proves the documented behavior is observable: a second ingest run
against the same store completes without error when the callable returns the
same data as the committed array. No SchemaDriftError is raised.

Memory assertion note: the 2x peak is a qualitative property. Asserting a
hard RSS bound here would be flaky across environments and is not the goal.
The goal is to confirm that the resume path executes correctly (callable
resolved, comparison passes, no exception raised).
"""

from __future__ import annotations

import importlib
from collections.abc import Iterator
from pathlib import Path

import pytest
from click.testing import CliRunner

from firecube.cli.main import cli
from firecube.ingestor.registry import loader as _loader

PLUGIN = "callable_payload_safe"
PRODUCT = "callable_payload_safe"


@pytest.fixture(autouse=True)
def reset_plugin_registry() -> Iterator[None]:
    original_loaded = _loader._LOADED
    original_registry = dict(_loader.AVAILABLE_INGESTORS)
    _loader._LOADED = False
    _loader.AVAILABLE_INGESTORS.clear()
    importlib.reload(importlib.import_module("callable_payload_test_plugin"))
    _loader._LOADED = True
    yield
    _loader._LOADED = original_loaded
    _loader.AVAILABLE_INGESTORS.clear()
    _loader.AVAILABLE_INGESTORS.update(original_registry)


def _ingest_args(target_path: Path, *, force_reingest: bool = False) -> list[str]:
    args = [
        "ingest",
        PLUGIN,
        "--target",
        f"file://{target_path}",
        "--product-name",
        PRODUCT,
        "--storage-type",
        "local",
        "--storage-driver",
        "fsspec",
        "--write-mode",
        "direct",
        "--option",
        "no_progress=true",
        "--option",
        "pipeline_parallel=false",
    ]
    if force_reingest:
        args += ["--option", "force_reingest=true"]
    return args


def test_callable_static_resume_completes_without_error(tmp_path: Path) -> None:
    """Resume with a callable static payload succeeds when data matches.

    First run: engine resolves the callable, writes the array, stamps the
    ``firecube_static_written`` marker. Peak = 1x payload (one array live).

    Second run (resume): engine resolves the callable again to get the
    incoming data, then reads the on-disk array for NaN-aware comparison.
    Both arrays are live simultaneously during the comparison (peak = 2x
    payload). The comparison passes because the callable returns the same
    stable module-level constant both times. No SchemaDriftError is raised.
    """
    target_path = tmp_path / "out.zarr"

    # First write: creates the store and stamps the static marker.
    first = CliRunner().invoke(cli, _ingest_args(target_path))
    assert first.exit_code == 0, (
        f"First ingest failed (exit {first.exit_code}).\nOutput:\n{first.output}"
    )
    assert target_path.exists(), f"Zarr store not created at {target_path}"

    # Resume: the marker is present; the engine resolves the callable and
    # reads the on-disk array for comparison. Both are live simultaneously.
    # The callable returns the same constant, so the comparison passes.
    second = CliRunner().invoke(cli, _ingest_args(target_path, force_reingest=True))
    assert second.exit_code == 0, (
        f"Resume ingest failed (exit {second.exit_code}).\n"
        "Expected: callable static payload matches committed data, no SchemaDriftError.\n"
        f"Output:\n{second.output}"
    )
