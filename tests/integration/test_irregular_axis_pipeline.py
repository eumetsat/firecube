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

"""Full-pipeline integration coverage for ``IrregularTimeAxis(values=AUTO)``.

Exercises the composite CLI flow shipped by A8 (``firecube zarr preallocate
--dry-run``), A9 (``firecube zarr index show --derived``), and A10 (irregular
coord-array materialization inside ``firecube zarr preallocate``) against all
six fixture ingestors from ``irregular_axis_test_plugin`` (A6).

Existing coverage in ``tests/integration/`` already asserts single-command
behaviour:

* ``test_auto_discovery.py`` — ``resolve_index_spec_for_ingestor`` at the
  binding boundary.
* ``test_irregular_coord_materialization.py`` — per-command preallocate
  behaviour (idempotency, dry-run non-mutation, coord dtype).
* ``test_frozen_index_safety.py`` — freeze/late-arrival safety contracts.

What is NOT covered elsewhere is the **composite pipeline** an operator
actually invokes: dry-run to preview, preallocate to persist, index show to
read back, and the failure paths that must exit non-zero and leave zero
side-effects on disk. This file fills that gap.

Failure paths use ``firecube zarr preallocate`` (which runs the discovery
pipeline as its first step) as the reproducer: if discovery fails, no
control-plane files or Zarr arrays land on disk. The plan's phrase
"dry-run → preallocate → index show → ingest" refers to that pipeline
staging — the ``firecube ingest`` command itself requires per-item payload
data that the ``irregular_axis_test_plugin`` fixtures do not supply
(``build_write_intents`` returns ``WriteIntent.slot(..., data=None)`` because
those fixtures target discovery/preallocate contracts, not payload writes).
End-to-end payload-carrying ingest coverage lives in
``tests/integration/test_callable_payload_integration.py``; composing
``irregular_axis_test_plugin`` with ``callable_payload_test_plugin`` would
require a new hybrid fixture ingestor and is intentionally out of scope for
A11.
"""

from __future__ import annotations

import importlib
import json
from collections.abc import Iterator
from pathlib import Path
from typing import Any, cast

import numpy as np
import pytest
import zarr
from click.testing import CliRunner

from firecube.cli.main import cli
from firecube.core.controlplane.types import INDEX_CURRENT_FILENAME, INDEX_DIRNAME
from firecube.ingestor.registry import loader as _loader

pytestmark = pytest.mark.integration

pytest.importorskip(
    "irregular_axis_test_plugin",
    reason="irregular_axis_test_plugin fixture is installed by A6",
)

_EXPECTED_SLOT_COUNT = 5
_BASE = np.datetime64("2026-01-01T00:00:00", "ns")
_STEP = np.timedelta64(600, "s").astype("timedelta64[ns]")
_EXPECTED_TIMESTAMPS = np.asarray(
    [_BASE + i * _STEP for i in range(_EXPECTED_SLOT_COUNT)],
    dtype="datetime64[ns]",
)
_EXPECTED_COORD_VALUES_JSON: list[str] = [
    "2026-01-01T00:00:00.000000000Z",
    "2026-01-01T00:10:00.000000000Z",
    "2026-01-01T00:20:00.000000000Z",
    "2026-01-01T00:30:00.000000000Z",
    "2026-01-01T00:40:00.000000000Z",
]


@pytest.fixture(autouse=True)
def _reset_plugin_registry() -> Iterator[None]:
    """Reload ``irregular_axis_test_plugin`` around each test.

    ``AVAILABLE_INGESTORS`` is process-global. Snapshotting + reloading around
    each test matches the pattern used in
    ``tests/integration/test_irregular_coord_materialization.py`` and prevents
    stale registrations from earlier tests bleeding into these CLI invocations.
    """
    original_loaded = _loader._LOADED
    original_registry = dict(_loader.AVAILABLE_INGESTORS)
    _loader._LOADED = False
    _loader.AVAILABLE_INGESTORS.clear()
    importlib.reload(importlib.import_module("irregular_axis_test_plugin"))
    _loader._LOADED = True
    yield
    _loader._LOADED = original_loaded
    _loader.AVAILABLE_INGESTORS.clear()
    _loader.AVAILABLE_INGESTORS.update(original_registry)


def _preallocate_args(
    plugin: str,
    product: str,
    target_path: Path,
    *,
    dry_run: bool = False,
) -> list[str]:
    args = [
        "zarr",
        "preallocate",
        plugin,
        "--target",
        f"file://{target_path}",
        "--product-name",
        product,
        "--storage-type",
        "local",
        "--storage-driver",
        "fsspec",
        "--write-mode",
        "direct",
        "--option",
        "no_progress=true",
    ]
    if dry_run:
        args.append("--dry-run")
    return args


def _index_show_args(
    product: str,
    target_path: Path,
    *,
    json_flag: bool = True,
    derived: bool = False,
) -> list[str]:
    args = [
        "zarr",
        "index",
        "show",
        "--target",
        f"file://{target_path}",
        "--product-name",
        product,
    ]
    if json_flag:
        args.append("--json")
    if derived:
        args.append("--derived")
    return args


def _dry_run_manifest(plugin: str, product: str, target_path: Path) -> dict[str, Any]:
    result = CliRunner().invoke(
        cli,
        _preallocate_args(plugin, product, target_path, dry_run=True),
    )
    assert result.exit_code == 0, result.output
    return cast(dict[str, Any], json.loads(result.stdout))


def _persist_and_read_hash(plugin: str, product: str, target_path: Path) -> str:
    prealloc = CliRunner().invoke(
        cli,
        _preallocate_args(plugin, product, target_path),
    )
    assert prealloc.exit_code == 0, prealloc.output
    show = CliRunner().invoke(
        cli,
        _index_show_args(product, target_path, json_flag=True),
    )
    assert show.exit_code == 0, show.output
    return cast(str, json.loads(show.output)["identity_hash"])


def _index_current_path(target_path: Path) -> Path:
    return target_path / ".firecube" / INDEX_DIRNAME / INDEX_CURRENT_FILENAME


def _coord_array_values(target_path: Path, group: str = "data") -> np.ndarray:
    root = zarr.open_group(store=str(target_path), mode="r", zarr_format=3)
    return np.asarray(cast(Any, root[f"{group}/timestamp"])[:])


def test_full_pipeline_dry_run_then_preallocate_then_index_show_agree_on_hash(
    tmp_path: Path,
) -> None:
    """A8 dry-run + A10 preallocate + A9 index show all yield the same identity_hash.

    The composite pipeline an operator runs is: (1) dry-run to preview the
    manifest without side effects, (2) real preallocate to persist the frozen
    manifest and materialise the coord array, (3) index show to read back the
    persisted record. All three MUST report byte-equal ``identity_hash``
    values; a drift between any pair would mean an operator's dry-run preview
    is not a faithful predictor of the real preallocate, or that index show
    reads a different record than preallocate wrote. This test protects the
    A8-A9-A10 chain end-to-end at the CLI boundary.
    """
    target_path = tmp_path / "out.zarr"

    dry_manifest = _dry_run_manifest("irregular_axis_safe", "irregular_axis_safe", target_path)
    assert not target_path.exists(), "dry-run must not create any filesystem state; A8 contract"

    prealloc = CliRunner().invoke(
        cli,
        _preallocate_args("irregular_axis_safe", "irregular_axis_safe", target_path),
    )
    assert prealloc.exit_code == 0, prealloc.output
    assert "resolved index: created" in prealloc.output
    assert "irregular coord materialization" in prealloc.output, (
        f"A10 materialization step missing from CLI output:\n{prealloc.output}"
    )
    assert _index_current_path(target_path).is_file(), (
        f"preallocate must persist .firecube/index/current.json; not found under {target_path}"
    )

    show = CliRunner().invoke(
        cli,
        _index_show_args("irregular_axis_safe", target_path, json_flag=True),
    )
    assert show.exit_code == 0, show.output
    show_hash = json.loads(show.output)["identity_hash"]

    assert dry_manifest["identity_hash"] == show_hash, (
        f"dry-run hash {dry_manifest['identity_hash'][:16]}... != "
        f"index-show hash {show_hash[:16]}..."
    )

    coord_values = _coord_array_values(target_path)
    assert np.array_equal(coord_values, _EXPECTED_TIMESTAMPS)


def test_reverse_order_axis_values_and_manifest_items_match_safe_ingestor(
    tmp_path: Path,
) -> None:
    """Reverse discovery order MUST produce byte-identical axis values and manifest items.

    ``IrregularAxisReverseOrderIngestor`` yields items in indices 4..0 while
    ``IrregularAxisSafeIngestor`` yields 0..4. Both plugins reach the same
    five timestamps and same five source_ref dicts, so after
    coordinate-value sort:

    * ``groups.data.params.values`` MUST be byte-identical between plugins
      (the sorted coordinate axis is the derived output every downstream
      consumer sees).
    * ``items`` (manifest entries, sorted by identity_hash) MUST be
      byte-identical (each entry is derived only from source_ref +
      coordinate_value, both invariant across plugins).

    Note: the top-level ``identity_hash`` legitimately differs because the
    plugins use distinct ``IndexSpec.name`` values (``irregular_axis_safe_v1``
    vs ``irregular_axis_reverse_order_v1``) and the spec name is part of the
    canonical bytes. This test asserts sort determinism at the axis and
    manifest level, which is the semantically meaningful invariant.
    """
    safe_path = tmp_path / "safe.zarr"
    reverse_path = tmp_path / "reverse.zarr"

    safe = _dry_run_manifest("irregular_axis_safe", "irregular_axis_safe", safe_path)
    reverse = _dry_run_manifest(
        "irregular_axis_reverse_order", "irregular_axis_reverse_order", reverse_path
    )

    safe_values = safe["index"]["groups"]["data"]["params"]["values"]
    reverse_values = reverse["index"]["groups"]["data"]["params"]["values"]
    assert safe_values == reverse_values == _EXPECTED_COORD_VALUES_JSON, (
        f"axis values diverged: safe={safe_values!r} reverse={reverse_values!r}"
    )

    safe_items = safe["items"]
    reverse_items = reverse["items"]
    assert safe_items == reverse_items, (
        "manifest items diverged despite identical sources; sort determinism broken"
    )


def test_concrete_axis_values_match_auto_axis_values(tmp_path: Path) -> None:
    """Concrete-tuple axis and AUTO discovery MUST produce byte-identical axis values.

    ``IrregularAxisConcreteIngestor`` declares the five timestamps as a
    literal tuple; ``IrregularAxisSafeIngestor`` discovers them from files.
    Both paths MUST yield the exact same JSON-serialised axis values so an
    operator can migrate from AUTO to concrete (or vice versa) without any
    observable change in the persisted coord axis. Byte equality of the
    ``groups.data.params.values`` list is the strongest such invariant.

    Note: concrete-axis manifests intentionally OMIT the ``items`` key (A4
    asymmetric canonical bytes) and use a different ``IndexSpec.name``, so
    the top-level ``identity_hash`` legitimately differs. This test isolates
    the axis-value equivalence claim from those unrelated fields.
    """
    auto_path = tmp_path / "auto.zarr"
    concrete_path = tmp_path / "concrete.zarr"

    auto = _dry_run_manifest("irregular_axis_safe", "irregular_axis_safe", auto_path)
    concrete = _dry_run_manifest(
        "irregular_axis_concrete", "irregular_axis_concrete", concrete_path
    )

    assert (
        auto["index"]["groups"]["data"]["params"]["values"]
        == concrete["index"]["groups"]["data"]["params"]["values"]
        == _EXPECTED_COORD_VALUES_JSON
    ), "concrete-vs-AUTO axis values diverged; concrete-vs-AUTO equivalence broken"
    assert "items" not in concrete, (
        "concrete axis manifests MUST omit 'items' (A4 asymmetric canonical bytes); "
        f"got keys {sorted(concrete.keys())!r}"
    )
    assert "items" in auto and len(auto["items"]) == _EXPECTED_SLOT_COUNT

    _persist_and_read_hash("irregular_axis_safe", "irregular_axis_safe", auto_path)
    _persist_and_read_hash("irregular_axis_concrete", "irregular_axis_concrete", concrete_path)
    assert np.array_equal(
        _coord_array_values(auto_path),
        _coord_array_values(concrete_path),
    )


def test_duplicate_ingestor_fails_preallocate_and_writes_no_cube(tmp_path: Path) -> None:
    """Duplicate coordinate MUST fail preallocate loudly and leave zero side effects.

    ``IrregularAxisDuplicateIngestor`` yields two items that map to the same
    timestamp coordinate. The preallocate pipeline MUST:

    * exit non-zero;
    * surface the ``DuplicateIrregularCoordinateError`` message (with both
      offending item references and the unique-coordinate marker)
      via the CLI error channel;
    * create NO Zarr store, NO ``.firecube`` control-plane files, and
      NO placeholder directories at the target.

    Zero-side-effects is the safety-net contract: a botched discovery must
    not leave a half-frozen manifest that a resume would then treat as
    authoritative.
    """
    target_path = tmp_path / "duplicate.zarr"

    result = CliRunner().invoke(
        cli,
        _preallocate_args("irregular_axis_duplicate", "irregular_axis_duplicate", target_path),
    )

    assert result.exit_code != 0, result.output
    assert "irregular_axis_duplicate" in result.output
    assert "coordinates must be unique" in result.output
    assert "items '0' and '0'" in result.output, (
        "duplicate error must name the offending item identities for operator triage"
    )
    assert not target_path.exists(), (
        f"failed preallocate must not leave a partial target; found "
        f"{list(target_path.iterdir()) if target_path.exists() else '(nothing)'}"
    )


def test_empty_ingestor_fails_preallocate_and_writes_no_cube(tmp_path: Path) -> None:
    """Empty discovery MUST fail preallocate with NoDiscoveredItemsError and no side effects.

    ``IrregularAxisEmptyIngestor.discover_source_files`` returns ``[]``.
    The preallocate pipeline MUST exit non-zero, name the missing coordinate
    (``timestamp``), and create nothing on disk. A silent freeze on empty
    input would produce an all-fill Zarr store with no way to distinguish
    "not written yet" from "genuinely empty" — the guard is the whole point
    of ``NoDiscoveredItemsError``.
    """
    target_path = tmp_path / "empty.zarr"

    result = CliRunner().invoke(
        cli,
        _preallocate_args("irregular_axis_empty", "irregular_axis_empty", target_path),
    )

    assert result.exit_code != 0, result.output
    assert "irregular_axis_empty" in result.output
    assert "no items found" in result.output
    assert "timestamp" in result.output
    assert not target_path.exists(), (
        f"failed preallocate must not leave a partial target; found "
        f"{list(target_path.iterdir()) if target_path.exists() else '(nothing)'}"
    )


def test_missing_coord_ingestor_fails_preallocate_and_writes_no_cube(tmp_path: Path) -> None:
    """Missing coordinate on one item MUST fail preallocate and leave no side effects.

    ``IrregularAxisMissingCoordIngestor.inspect_item`` returns
    ``ItemInfo(coordinate=None)`` for item index 2. The AUTO discovery path
    MUST raise ``MissingIrregularCoordinateError`` naming the offending item
    and the coordinate, exit non-zero, and create nothing on disk. Silent
    slot skipping would misalign every downstream slot index against the
    coordinate the operator declared.
    """
    target_path = tmp_path / "missing_coord.zarr"

    result = CliRunner().invoke(
        cli,
        _preallocate_args(
            "irregular_axis_missing_coord", "irregular_axis_missing_coord", target_path
        ),
    )

    assert result.exit_code != 0, result.output
    assert "irregular_axis_missing_coord" in result.output
    assert "no resolvable coordinate" in result.output
    assert "timestamp" in result.output
    assert "item 2" in result.output, (
        "missing-coord error must name the offending item (index 2) for operator triage"
    )
    assert not target_path.exists(), (
        f"failed preallocate must not leave a partial target; found "
        f"{list(target_path.iterdir()) if target_path.exists() else '(nothing)'}"
    )


def test_index_show_derived_flag_on_irregular_axis_is_a_documented_noop(
    tmp_path: Path,
) -> None:
    """A9 --derived flag on IrregularTimeAxis groups MUST emit a documented no-op note.

    The A9 flag computes derived coordinate values at read time for
    ``RegularTimeAxis`` groups (which persist ``epoch`` + ``cadence_s`` and
    derive the axis on the fly). For ``IrregularTimeAxis`` groups the
    coordinate values are already materialised on disk by A10 preallocate,
    so ``--derived`` MUST NOT recompute or overwrite them. The CLI advertises
    this contract via a text note on the group; this test guards that
    contract at the user-visible boundary so a future change that quietly
    starts synthesising coordinates for irregular axes would fail here.
    """
    target_path = tmp_path / "derived_noop.zarr"

    prealloc = CliRunner().invoke(
        cli,
        _preallocate_args("irregular_axis_safe", "irregular_axis_safe", target_path),
    )
    assert prealloc.exit_code == 0, prealloc.output

    show_derived = CliRunner().invoke(
        cli,
        _index_show_args("irregular_axis_safe", target_path, json_flag=False, derived=True),
    )
    assert show_derived.exit_code == 0, show_derived.output
    assert "data" in show_derived.output
    assert "not a regular_time axis" in show_derived.output
    assert "no-op" in show_derived.output

    coord_values = _coord_array_values(target_path)
    assert np.array_equal(coord_values, _EXPECTED_TIMESTAMPS), (
        "--derived on IrregularTimeAxis must not touch the persisted coord array"
    )
