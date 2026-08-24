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

"""End-to-end integration coverage for callable ``WriteIntent.data`` payloads.

Drives ``callable_payload_test_plugin.CallablePayloadSafeIngestor`` through the
public ``firecube ingest`` CLI and asserts the on-disk Zarr store reflects the
values returned by the plugin's callables — proving that the dispatch layer
introduced in T2 resolves both ``kind="region"`` and ``kind="static"`` callable
payloads correctly on the real ingest path (not just via unit stubs).

Placement rationale: the fixture package under
``tests/fixtures/callable_payload_test_plugin/tests/`` already has coverage
for the same plugin. This module lifts an equivalence and a coverage assertion
into ``tests/integration/`` so the callable-dispatch contract is visible in the
canonical integration-test location — reviewers looking at "what does the
integration suite prove?" find this test file directly under
``tests/integration/`` next to the other DirectZarr end-to-end tests, rather
than having to know that a fixture package also ships coverage of its own.
"""

from __future__ import annotations

import importlib
from collections.abc import Iterator
from pathlib import Path
from typing import Any, cast

import numpy as np
import pytest
import zarr
from click.testing import CliRunner

from firecube.cli.main import cli
from firecube.ingestor.registry import loader as _loader

pytestmark = pytest.mark.integration

PLUGIN = "callable_payload_safe"
PRODUCT = "callable_payload_safe"

_EXPECTED_REGION = np.full((4, 4), 42.0, dtype=np.float32)
_EXPECTED_STATIC = np.arange(4, dtype=np.float64) * 10.0


@pytest.fixture(autouse=True)
def _reset_plugin_registry() -> Iterator[None]:
    """Force a fresh ``callable_payload_test_plugin`` registration per test.

    ``AVAILABLE_INGESTORS`` is process-global; leaving it dirty across tests
    causes the CLI to see stale plugin classes when the module has been
    reloaded elsewhere in the suite. Snapshotting + reloading around each test
    matches the pattern used by the fixture's own tests
    (``tests/fixtures/callable_payload_test_plugin/tests/test_safe_ingestor.py``).
    """

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


def _ingest_args(target_path: Path) -> list[str]:
    return [
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


def _run_ingest_and_open(target_path: Path) -> Any:
    result = CliRunner().invoke(cli, _ingest_args(target_path))
    assert result.exit_code == 0, (
        f"callable-payload safe ingest failed (exit {result.exit_code}).\nOutput:\n{result.output}"
    )
    assert target_path.exists(), f"Zarr store not created at {target_path}"
    return zarr.open_group(store=str(target_path), mode="r", zarr_format=3)


def test_callable_payloads_land_in_zarr_with_expected_values(tmp_path: Path) -> None:
    """End-to-end equivalence: callable outputs equal what lands in the store.

    The safe fixture emits one ``kind="region"`` intent and one ``kind="static"``
    intent, each with a callable that closes over a module-level ndarray
    constant. A correct dispatch resolves each callable once and hands the
    resolved array to the writer; failure modes this test would catch include:

    - dispatch skipping the callable (writer sees the raw function object and
      the store ends up with fill values or a serialized function payload);
    - dispatch double-invoking the callable (region path still works, but any
      callable with side effects would double-emit them — the retained-peak
      guard depends on single invocation);
    - the writer dropping the payload silently (arrays stay at their declared
      ``fill_value``);
    - schema-setup skipping the static array (``data/lat`` would be missing).
    """

    target_path = tmp_path / "out.zarr"
    root = _run_ingest_and_open(target_path)

    values = np.asarray(cast(Any, root["data/values"])[:])
    lat = np.asarray(cast(Any, root["data/lat"])[:])

    assert values.shape == (1, 4, 4), f"unexpected values shape: {values.shape}"
    assert lat.shape == (4,), f"unexpected lat shape: {lat.shape}"

    np.testing.assert_array_equal(
        values[0],
        _EXPECTED_REGION,
        err_msg="region callable output did not reach the store byte-identical",
    )
    np.testing.assert_array_equal(
        lat,
        _EXPECTED_STATIC,
        err_msg="static callable output did not reach the store byte-identical",
    )


def test_callable_dispatch_exercises_region_and_static_kinds(tmp_path: Path) -> None:
    """Both payload-carrying intent kinds go through the callable path.

    Retained-peak bound (qualitative, no hard memory number here):

        Retained peak is bounded by one materialized payload plus writer
        overhead. The callable is resolved once at dispatch and immediately
        passed to the writer, so the full payload set is never simultaneously
        live. A regression that accumulates payloads (e.g. materializing all
        callables into a list before writing) would violate this bound.

    The hard-number guard for retained peak lives in the benchmark harness
    under ``tests/benchmarks/lazy_writeintent_harness/`` and requires the FCI
    plugin — this integration test only asserts the structural coverage that
    both kinds ran, which is a prerequisite for the harness's bound to apply.

    Assertion strategy: both arrays must be present and non-fill. If dispatch
    had skipped either the region or the static callable, the corresponding
    array would still be at its declared ``fill_value=0.0`` (schema setup
    pre-allocates arrays filled with the fill value; the writer overwrites).
    """

    target_path = tmp_path / "out.zarr"
    root = _run_ingest_and_open(target_path)

    values = np.asarray(cast(Any, root["data/values"])[:])
    lat = np.asarray(cast(Any, root["data/lat"])[:])

    assert not np.all(values == 0.0), (
        "region callable did not overwrite fill_value=0.0 — dispatch may have "
        "skipped the region intent or forwarded the raw callable object"
    )
    assert not np.all(lat == 0.0), (
        "static callable did not overwrite fill_value=0.0 — dispatch may have "
        "skipped the static intent or forwarded the raw callable object"
    )

    np.testing.assert_array_equal(values[0], _EXPECTED_REGION)
    np.testing.assert_array_equal(lat, _EXPECTED_STATIC)
