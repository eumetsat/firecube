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

"""Byte-level parity oracle for coordinate-store bytes across time-axis shapes.

Drives five pre-existing fixture ingestors through the real ``firecube`` CLI
(``zarr preallocate`` / ``ingest``) against real local Zarr stores under
``tmp_path``, then pins a sha256-per-file manifest (plus the raw
``zarr.json`` text of every such file, for a human-readable diff target) of
every byte written under the store root, excluding the ``.firecube/``
control-plane subtree (run ids, timestamps, ``current.json``, events -- none
of which are part of the coordinate/data payload this gate protects).

Scenarios:

* ``gregorian_regular_exact_slot_count`` -- ``regular_axis_dense_coord``:
  Gregorian ``RegularTimeAxis(mode="exact")`` sized via ``slot_count``.
  Preallocate-only: this fixture's ``build_write_intents`` returns ``[]``,
  and no existing test in this repo drives a real ``ingest`` payload write
  for it (see ``tests/integration/test_calendar_axis_direct_pipeline.py``'s
  ``test_gregorian_regular_axis_fixture_unchanged``, which is also
  preallocate-only).
* ``gregorian_regular_exact_end_date`` -- ``regular_axis_end_date``: same
  axis shape, sized via ``end_date`` instead of ``slot_count``.
  Preallocate-only for the same reason (``build_write_intents`` returns
  ``[]``).
* ``gregorian_irregular_explicit`` -- ``irregular_axis_concrete``: Gregorian
  ``IrregularTimeAxis`` with an explicit, concrete tuple of five
  timestamps. Preallocate-only: irregular axes are fully materialized at
  preallocate, and every existing test that exercises this fixture
  (``tests/integration/test_irregular_axis_pipeline.py``,
  ``tests/integration/test_irregular_coord_materialization.py``) only ever
  calls ``zarr preallocate`` against it, never ``ingest``.
* ``calendar_360_day_regular`` -- ``calendar_axis_regular`` with
  ``calendar=360_day, slot_count=10``: preallocate + ingest, so this
  scenario pins both the calendar-encoded coordinate bytes and the real
  payload bytes written by ``ingest``. ``slot_count=10`` matches the
  smallest value used anywhere in
  ``tests/integration/test_calendar_axis_direct_pipeline.py`` (its
  idempotency test).
* ``calendar_noleap_irregular`` -- ``calendar_axis_irregular_explicit`` with
  ``calendar=noleap``: preallocate + ingest over the fixture's fixed
  5-value gapped day-offset axis (day offsets 0, 2, 5, 9, 14).

Exclusion rule: everything under ``.firecube/`` (by path prefix) is
excluded from the manifest -- it is run-id/timestamp-bearing control-plane
bookkeeping, not the coordinate/data payload. No data-array or group
``zarr.json`` attr-level exclusions are needed: a repo-wide check confirms
``firecube_run_id``/``firecube_span_id`` are reserved array-attr keys that
no current code path stamps, and ``firecube_consolidated_at`` is only
stamped by ``firecube zarr consolidate-time-coord``, which none of these
five scenarios call. Determinism is additionally verified empirically
inside this test: every scenario's store is built twice (into two separate
``tmp_path`` subdirectories) and the two manifests must be byte-identical
before either is compared against the committed golden.

Regeneration: set ``FIRECUBE_REGENERATE_STORE_MANIFEST=1`` to (re)write
``tests/integration/_coord_store_bytes_manifest.json`` from this tree's
current output and skip the assertion. Regenerating is a deliberate,
persisted-format decision requiring evidence that the previous bytes were
wrong -- not something to flip casually because a refactor happened to
change output.

No mocks of firecube internals; assertions read the store (raw file bytes)
via the real CLI (``CliRunner``) and the real local filesystem only.
"""

from __future__ import annotations

import difflib
import hashlib
import json
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import calendar_axis_test_plugin  # noqa: F401  (fixture package must be installed)
import irregular_axis_test_plugin  # noqa: F401  (fixture package must be installed)
import pytest
import regular_axis_test_plugin  # noqa: F401  (fixture package must be installed)
from click.testing import CliRunner

from firecube.cli.main import cli

pytestmark = [pytest.mark.integration, pytest.mark.snapshot, pytest.mark.contract]

_CONTROL_PLANE_DIR = ".firecube"
_GOLDEN_PATH = Path(__file__).parent / "_coord_store_bytes_manifest.json"
_REGENERATE_ENV_VAR = "FIRECUBE_REGENERATE_STORE_MANIFEST"


@dataclass(frozen=True)
class ScenarioSpec:
    """One store-bytes-parity scenario: a fixture plugin driven through the CLI."""

    scenario_id: str
    plugin: str
    options: dict[str, Any] = field(default_factory=dict)
    needs_ingest: bool = False


_SCENARIOS: tuple[ScenarioSpec, ...] = (
    ScenarioSpec("gregorian_regular_exact_slot_count", "regular_axis_dense_coord"),
    ScenarioSpec("gregorian_regular_exact_end_date", "regular_axis_end_date"),
    ScenarioSpec("gregorian_irregular_explicit", "irregular_axis_concrete"),
    ScenarioSpec(
        "calendar_360_day_regular",
        "calendar_axis_regular",
        options={"calendar": "360_day", "slot_count": 10},
        needs_ingest=True,
    ),
    ScenarioSpec(
        "calendar_noleap_irregular",
        "calendar_axis_irregular_explicit",
        options={"calendar": "noleap"},
        needs_ingest=True,
    ),
)


def _base_args(product: str, target: Path, *, options: dict[str, Any]) -> list[str]:
    args = [
        "--target",
        f"file://{target}",
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
    for key, value in options.items():
        args.extend(["--option", f"{key}={value}"])
    return args


def _preallocate(plugin: str, target: Path, *, options: dict[str, Any]) -> Any:
    return CliRunner().invoke(
        cli,
        ["zarr", "preallocate", plugin, *_base_args(plugin, target, options=options)],
    )


def _ingest(plugin: str, target: Path, *, options: dict[str, Any]) -> Any:
    return CliRunner().invoke(
        cli,
        ["ingest", plugin, *_base_args(plugin, target, options=options)],
    )


def _hash_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _snapshot_manifest(target: Path) -> dict[str, Any]:
    """Hash every non-``.firecube/`` file under *target*, plus raw ``zarr.json`` text."""
    files: dict[str, str] = {}
    zarr_json_text: dict[str, str] = {}
    for path in target.rglob("*"):
        if not path.is_file():
            continue
        relpath = path.relative_to(target).as_posix()
        if relpath == _CONTROL_PLANE_DIR or relpath.startswith(f"{_CONTROL_PLANE_DIR}/"):
            continue
        files[relpath] = _hash_file(path)
        if path.name == "zarr.json":
            zarr_json_text[relpath] = path.read_text(encoding="utf-8")
    return {"files": files, "zarr_json_text": zarr_json_text}


def _build_scenario_store(store_root: Path, scenario: ScenarioSpec) -> dict[str, Any]:
    """Run preallocate (and ingest, if the scenario needs it) via the real CLI."""
    target = store_root / "cube.zarr"

    pre = _preallocate(scenario.plugin, target, options=scenario.options)
    assert pre.exit_code == 0, (
        f"scenario {scenario.scenario_id!r}: preallocate failed:\n{pre.output}"
    )

    if scenario.needs_ingest:
        ing = _ingest(scenario.plugin, target, options=scenario.options)
        assert ing.exit_code == 0, (
            f"scenario {scenario.scenario_id!r}: ingest failed:\n{ing.output}"
        )

    return _snapshot_manifest(target)


def _assert_manifests_equal(scenario_id: str, run1: dict[str, Any], run2: dict[str, Any]) -> None:
    """Assert two builds of the same scenario produced byte-identical manifests."""
    files1, files2 = run1["files"], run2["files"]
    if files1 == files2:
        return
    keys1, keys2 = set(files1), set(files2)
    only_in_1 = sorted(keys1 - keys2)
    only_in_2 = sorted(keys2 - keys1)
    mismatched = sorted(k for k in keys1 & keys2 if files1[k] != files2[k])
    details = [f"scenario {scenario_id!r}: non-deterministic build (run1 != run2)."]
    if only_in_1:
        details.append(f"  only in run1: {only_in_1}")
    if only_in_2:
        details.append(f"  only in run2: {only_in_2}")
    if mismatched:
        first = mismatched[0]
        details.append(f"  hash mismatch at {first!r}: run1={files1[first]} run2={files2[first]}")
        details.append(f"  all mismatched paths: {mismatched}")
    pytest.fail("\n".join(details))


def test_coord_store_bytes_parity(tmp_path: Path) -> None:
    """Build every scenario twice, assert determinism, then diff against the golden.

    Failure output names the scenario, the specific path, and (for
    ``zarr.json`` entries) a unified text diff, so a single run surfaces
    every divergence rather than stopping at the first one.
    """
    actual: dict[str, Any] = {}

    for scenario in _SCENARIOS:
        run1 = _build_scenario_store(tmp_path / "run1" / scenario.scenario_id, scenario)
        run2 = _build_scenario_store(tmp_path / "run2" / scenario.scenario_id, scenario)
        _assert_manifests_equal(scenario.scenario_id, run1, run2)

        actual[scenario.scenario_id] = {
            "plugin": scenario.plugin,
            "product_name": scenario.plugin,
            "files": run1["files"],
            "zarr_json_text": run1["zarr_json_text"],
        }

    if os.environ.get(_REGENERATE_ENV_VAR) == "1":
        _GOLDEN_PATH.write_text(
            json.dumps(actual, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
        pytest.skip(f"regenerated golden manifest at {_GOLDEN_PATH} ({_REGENERATE_ENV_VAR}=1)")

    golden: dict[str, Any] = json.loads(_GOLDEN_PATH.read_text(encoding="utf-8"))

    failures: list[str] = []
    all_scenario_ids = sorted(set(actual) | set(golden))
    for scenario_id in all_scenario_ids:
        if scenario_id not in golden:
            failures.append(f"scenario {scenario_id!r}: present in actual but missing from golden")
            continue
        if scenario_id not in actual:
            failures.append(f"scenario {scenario_id!r}: present in golden but missing from actual")
            continue

        actual_files = actual[scenario_id]["files"]
        golden_files = golden[scenario_id]["files"]
        actual_texts = actual[scenario_id]["zarr_json_text"]
        golden_texts = golden[scenario_id]["zarr_json_text"]

        all_paths = sorted(set(actual_files) | set(golden_files))
        for relpath in all_paths:
            if relpath not in golden_files:
                failures.append(
                    f"scenario {scenario_id!r}, path {relpath!r}: "
                    "present in actual but missing from golden"
                )
                continue
            if relpath not in actual_files:
                failures.append(
                    f"scenario {scenario_id!r}, path {relpath!r}: "
                    "present in golden but missing from actual"
                )
                continue
            if actual_files[relpath] == golden_files[relpath]:
                continue

            message = (
                f"scenario {scenario_id!r}, path {relpath!r}: hash mismatch "
                f"(golden={golden_files[relpath]}, actual={actual_files[relpath]})"
            )
            if relpath in actual_texts and relpath in golden_texts:
                diff = "\n".join(
                    difflib.unified_diff(
                        golden_texts[relpath].splitlines(),
                        actual_texts[relpath].splitlines(),
                        fromfile=f"golden/{relpath}",
                        tofile=f"actual/{relpath}",
                        lineterm="",
                    )
                )
                message = f"{message}\n{diff}"
            failures.append(message)

    if failures:
        pytest.fail(
            f"{len(failures)} store-bytes-parity divergence(s):\n\n" + "\n\n".join(failures)
        )
