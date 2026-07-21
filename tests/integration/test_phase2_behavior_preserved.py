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

"""T17: Phase 2 single-pod behavior must be PRESERVED EXACTLY when no slot flags are passed.

These integration tests assert that the Phase 3 slot-range parallelism work did
not regress Phase 2 single-pod ingestion. Specifically:

- A non-capable plugin (``SUPPORTS_SLOT_RANGE_PARALLELISM`` left as default
  ``False``) must continue to ingest end-to-end without any parallel
  machinery being engaged.
- A capable plugin (``SUPPORTS_SLOT_RANGE_PARALLELISM = True``) invoked
  WITHOUT ``--slot-start``/``--slot-end`` must behave **byte-for-byte
  identically** to a non-capable plugin: the capability gate must not
  fire, no global schema setup claim (``zarr_schema_global``) must be
  written, the per-batch auto-compute schema path must be used, and run
  IDs must not carry a ``__slot=`` suffix.
- The Phase 2 per-slot claim mechanism must remain operational so the
  ingestion completes without deadlocking on the control plane.

All assertions are made against on-disk artefacts under
``{target}/.firecube/`` and against captured stdout / log records, so the
tests do not rely on internal call counters that could be reordered.
"""

from __future__ import annotations

import importlib
import logging
from collections.abc import Iterator
from pathlib import Path
from typing import Any, cast

import numpy as np
import pytest
import zarr
from click.testing import CliRunner

from firecube.cli.main import cli
from firecube.core.controlplane import ChunkManager
from firecube.ingestor.registry import loader as _loader
from tests.helpers.storage import make_test_binding

pytestmark = pytest.mark.integration


_PARALLEL_EVIDENCE_MARKER = "Parallel evidence:"
_SCHEMA_VERIFY_MARKER = "stage=schema_verify"
_GLOBAL_SCHEMA_CLAIM_CATEGORY = "zarr_schema_global"
_SLOT_RUN_ID_SUFFIX = "__slot="


@pytest.fixture(autouse=True)
def reset_plugin_registry() -> Iterator[None]:
    """Reset plugin discovery state so fixture plugins re-register per test.

    Mirrors the pattern used in ``tests/unit/test_cli_zarr_setup_schema.py``:
    clear the loader cache, reload both fixture plugin modules so their
    ``@register_ingestor`` decorators run again against the freshly cleared
    registry, then restore the original state on teardown.
    """
    original_loaded = _loader._LOADED
    original_registry = dict(_loader.AVAILABLE_INGESTORS)
    _loader._LOADED = False
    _loader.AVAILABLE_INGESTORS.clear()
    importlib.reload(importlib.import_module("direct_zarr_capable_test_plugin"))
    importlib.reload(importlib.import_module("direct_zarr_non_capable_test_plugin"))
    yield
    _loader._LOADED = original_loaded
    _loader.AVAILABLE_INGESTORS.clear()
    _loader.AVAILABLE_INGESTORS.update(original_registry)


def _ingest_args(plugin: str, product_name: str, target_path: Path) -> list[str]:
    return [
        "ingest",
        plugin,
        "--target",
        f"file://{target_path}",
        "--product-name",
        product_name,
        "--storage-type",
        "local",
        "--storage-driver",
        "fsspec",
        "--write-mode",
        "direct",
        "--option",
        "no_progress=true",
        "--option",
        "pipeline_batch_size=300",
        "--option",
        "pipeline_parallel=false",
    ]


def _run_ingest(
    caplog: pytest.LogCaptureFixture,
    plugin: str,
    product_name: str,
    target_path: Path,
) -> tuple[Any, str]:
    """Invoke the CLI and return (result, combined_output_with_logs).

    Combining ``result.output`` with ``caplog.text`` makes the absence
    assertions robust regardless of whether the logging backend is wired to
    stdout for the given run.
    """
    caplog.set_level(logging.INFO)
    result = CliRunner().invoke(
        cli,
        _ingest_args(plugin, product_name, target_path),
    )
    combined = result.output + "\n" + caplog.text
    return result, combined


def _assert_no_slot_suffix_in_run_ids(target_path: Path) -> None:
    """Every run directory under ``.firecube/runs/`` must lack ``__slot=``."""
    runs_dir = target_path / ".firecube" / "runs"
    if not runs_dir.is_dir():
        return
    offending = [
        run_dir.name
        for run_dir in runs_dir.iterdir()
        if run_dir.is_dir() and _SLOT_RUN_ID_SUFFIX in run_dir.name
    ]
    assert not offending, (
        "Single-pod mode (no --slot-start/--slot-end) must not produce "
        f"slot-suffixed run IDs; found: {offending}"
    )


def _assert_no_global_schema_claim(target_path: Path) -> None:
    """No claim file may reference the ``zarr_schema_global`` category.

    Claim files are released (deleted) once their write completes, so in a
    clean post-run state ``.firecube/claims/`` is typically empty. The check
    inspects any claim file that happens to still be on disk for the
    parallel-only category marker, which would indicate the global schema
    setup path was engaged.
    """
    claims_dir = target_path / ".firecube" / "claims"
    if not claims_dir.is_dir():
        return
    for claim_file in claims_dir.iterdir():
        if not claim_file.is_file():
            continue
        content = claim_file.read_text(encoding="utf-8", errors="replace")
        assert _GLOBAL_SCHEMA_CLAIM_CATEGORY not in content, (
            "Single-pod mode must not create zarr_schema_global claims; "
            f"found in {claim_file.name}: {content}"
        )


def _assert_direct_zarr_rows(target_path: Path, *, expected_rows: int) -> None:
    arr = cast(
        Any,
        zarr.open_group(store=str(target_path), mode="r", zarr_format=3)["data/data"],
    )
    assert arr.shape == (expected_rows, 10), f"unexpected array shape: {arr.shape}"
    np.testing.assert_array_equal(
        np.asarray(arr[0, :]),
        np.zeros(10, dtype=np.float32),
    )
    np.testing.assert_array_equal(
        np.asarray(arr[expected_rows - 1, :]),
        np.full(10, float(expected_rows - 1), dtype=np.float32),
    )


def test_non_capable_subclass_phase2_behavior(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """Non-capable plugin (SUPPORTS=False default) runs as standard Phase 2.

    Asserts:
        - exit code 0
        - Zarr store created under target path
        - ``data/data`` array exists in the resulting store
        - no ``__slot=`` suffix in any created run IDs
        - no ``"Parallel evidence:"`` log lines emitted
    """
    target_path = tmp_path / "out.zarr"
    result, combined = _run_ingest(
        caplog,
        "direct_zarr_non_capable_test_plugin",
        "direct_zarr_non_capable_test_product",
        target_path,
    )

    assert result.exit_code == 0, result.output
    assert target_path.exists(), f"Zarr store not created at {target_path}"

    _assert_direct_zarr_rows(target_path, expected_rows=200)

    _assert_no_slot_suffix_in_run_ids(target_path)

    assert _PARALLEL_EVIDENCE_MARKER not in combined, (
        "Phase 2 single-pod mode must not emit parallel evidence log lines; "
        f"found marker in output:\n{combined}"
    )


def test_capable_plugin_no_flags_behaves_as_single_pod(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """Capable plugin (SUPPORTS=True) without slot flags == Phase 2 single-pod.

    The key invariant: declaring ``SUPPORTS_SLOT_RANGE_PARALLELISM = True``
    alone must NOT change runtime behavior when the operator does not pass
    slot flags. No global schema setup, no parallel evidence, no slot
    suffix.
    """
    target_path = tmp_path / "out.zarr"
    result, combined = _run_ingest(
        caplog,
        "direct_zarr_capable_test_plugin",
        "direct_zarr_capable_test_product",
        target_path,
    )

    assert result.exit_code == 0, result.output
    assert target_path.exists(), f"Zarr store not created at {target_path}"
    _assert_direct_zarr_rows(target_path, expected_rows=200)

    assert _PARALLEL_EVIDENCE_MARKER not in combined, (
        "SUPPORTS_SLOT_RANGE_PARALLELISM=True without slot flags must not "
        "trigger any parallel evidence logging; "
        f"found marker in output:\n{combined}"
    )

    _assert_no_slot_suffix_in_run_ids(target_path)
    _assert_no_global_schema_claim(target_path)


def test_single_pod_ingest_releases_write_claims(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """Single-pod direct-Zarr ingest leaves no active write-domain claims."""
    target_path = tmp_path / "out.zarr"
    result, _ = _run_ingest(
        caplog,
        "direct_zarr_non_capable_test_plugin",
        "direct_zarr_non_capable_test_product",
        target_path,
    )

    assert result.exit_code == 0, result.output
    assert target_path.exists(), f"Zarr store not created at {target_path}"

    manager = ChunkManager(
        binding=make_test_binding(tmp_path, product=target_path.name),
        workspace=tmp_path,
    )
    try:
        assert manager.list_claims() == []
    finally:
        manager.close()
