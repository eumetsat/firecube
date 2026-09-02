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

"""The control-plane spec page must not drift from the code it documents.

``docs/reference/control-plane-spec.md`` is a normative file-format
specification, so unlike the other reference pages it cannot be generated
from docstrings. These tests pin its highest-drift claims to their sources:
the layout paths to the control-plane path constants, the WAL event-type
table to the ``EVENT_*`` constants, the precedence table to the exhaustive
row enumeration in the precedence-matrix test docstring, and the version and
staleness literals to their defining constants. Adding an event type or
renaming a path without updating the spec fails here.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from firecube.core.controlplane import types as cp_types
from firecube.core.controlplane.claims import DEFAULT_STALE_THRESHOLD_S

pytestmark = pytest.mark.docs_static

_REPO_ROOT = Path(__file__).resolve().parents[2]
SPEC_PAGE = _REPO_ROOT / "docs" / "reference" / "control-plane-spec.md"
MATRIX_TEST = _REPO_ROOT / "tests" / "integration" / "test_resolved_index_precedence_matrix.py"


def _spec_text() -> str:
    return SPEC_PAGE.read_text(encoding="utf-8")


def test_layout_paths_match_constants() -> None:
    spec = _spec_text()
    expected_paths = [
        cp_types.CONTROL_DIRNAME,
        cp_types.SCHEMA_FILENAME,
        cp_types.LATEST_POINTER,
        f"{cp_types.RUNS_DIRNAME}/",
        f"{cp_types.CLAIMS_DIRNAME}/",
        f"{cp_types.SNAPSHOT_DIRNAME}/",
        f"{cp_types.INDEX_DIRNAME}/{cp_types.INDEX_CURRENT_FILENAME}",
        f"{cp_types.SLOT_INDEX_DIRNAME}/{cp_types.SLOT_INDEX_CURRENT_FILENAME}",
    ]
    missing = [path for path in expected_paths if path not in spec]
    assert not missing, (
        f"control-plane spec layout is missing paths defined in "
        f"firecube.core.controlplane.types: {missing}"
    )


def test_every_wal_event_type_is_documented() -> None:
    spec = _spec_text()
    event_types = {
        value
        for name, value in vars(cp_types).items()
        if name.startswith("EVENT_") and isinstance(value, str)
    }
    assert event_types, "no EVENT_* constants found; the source layout changed"
    missing = sorted(value for value in event_types if f"`{value}`" not in spec)
    assert not missing, (
        f"control-plane spec event-type table is missing WAL event types "
        f"defined in firecube.core.controlplane.types: {missing}"
    )


def test_schema_versions_and_stale_default_match_code() -> None:
    spec = _spec_text()
    assert f'`"{cp_types.SCHEMA_VERSION}"`' in spec, (
        f"spec must state the control-plane envelope version "
        f'`"{cp_types.SCHEMA_VERSION}"` from types.SCHEMA_VERSION'
    )
    record_version = cp_types.ResolvedIndexRecord.__dataclass_fields__["schema_version"].default
    assert f'`"{record_version}"`' in spec, (
        f'spec must state the resolved-index record version `"{record_version}"`'
    )
    assert f"default {DEFAULT_STALE_THRESHOLD_S}" in spec, (
        f"spec must state the claim staleness default of {DEFAULT_STALE_THRESHOLD_S} seconds "
        f"from claims.DEFAULT_STALE_THRESHOLD_S"
    )


def test_precedence_table_matches_matrix_test_rows() -> None:
    """Each row the matrix test docstring enumerates appears in the spec table
    with a compatible outcome.

    The docstring of
    ``test_apply_resolved_index_precedence_never_raises_assertion_error``
    is the authoritative row-by-row contract. This test extracts its row
    numbers and outcome classes and requires the spec's precedence table to
    carry the same rows with matching outcome language.
    """

    docstring_rows: dict[int, str] = {}
    for line in MATRIX_TEST.read_text(encoding="utf-8").splitlines():
        match = re.search(r"row (\d)\s*[—-]+\s*(.+?)\.?$", line)
        if match:
            docstring_rows[int(match.group(1))] = match.group(2)
    assert set(docstring_rows) == set(range(1, 8)), (
        f"expected rows 1-7 in the matrix test docstring, found {sorted(docstring_rows)}; "
        f"realign the spec's precedence table with the docstring"
    )

    spec_rows: dict[int, str] = {}
    for line in _spec_text().splitlines():
        match = re.match(r"\|\s*(\d)\s*\|(.+)", line)
        if match:
            spec_rows[int(match.group(1))] = match.group(2).lower()
    assert set(spec_rows) == set(range(1, 8)), (
        f"spec precedence table must have exactly rows 1-7, found {sorted(spec_rows)}"
    )

    for row, contract in docstring_rows.items():
        spec_row = spec_rows[row]
        if "ResolvedIndexConflictError" in contract:
            expected = "conflict"
        elif "ManifestError" in contract:
            expected = "manifest error"
        elif "matched_existing" in contract:
            expected = "matched_existing"
        else:
            expected = "created"
        assert expected in spec_row, (
            f"spec precedence row {row} must state outcome {expected!r} to match the "
            f"matrix test docstring ({contract!r}), got: {spec_row!r}"
        )
