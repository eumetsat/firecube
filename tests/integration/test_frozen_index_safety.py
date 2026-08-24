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

"""Frozen-index safety contracts for ``IrregularTimeAxis(values=AUTO)`` discovery.

Each test names one safety contract that ``resolve_index_spec_for_ingestor``
MUST honour for the AUTO discovery path. The contracts protect the on-disk
frozen manifest from silent corruption when a plugin's discovered items
drift, arrive out of order, collide, or vanish between runs.

Contracts covered:

1. Deterministic ordering -- identical input yields identical ``identity_hash``.
2. Duplicate rejection -- coordinate collision names BOTH offending items.
3. Freeze-then-late-arrival refusal.
4. Empty discovery refusal -- zero items raises ``NoDiscoveredItemsError``.
5. Non-monotonic to canonical order -- reverse input sorts to forward manifest.
6. Path stability contract -- every manifest entry carries a non-empty
   ``source_ref`` that a lazy WriteIntent callable can safely close over.
7. Identity-hash equality across dry-run vs preallocate.

All tests exercise the real fixture ingestors from
``irregular_axis_test_plugin`` (installed by A6). No mocks fake
``inspect_item``: the contract being guarded is precisely the contract
between a real plugin and the runtime binding, so faking either side would
prove nothing.
"""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import pytest
from click.testing import CliRunner

from firecube.cli.main import cli
from firecube.core.errors import (
    DuplicateIrregularCoordinateError,
    NoDiscoveredItemsError,
    ResolvedIndexConflictError,
)
from firecube.ingestor.runtime.index_binding import resolve_index_spec_for_ingestor

pytestmark = pytest.mark.integration

plugin = pytest.importorskip(
    "irregular_axis_test_plugin",
    reason="irregular_axis_test_plugin fixture is installed by A6",
)


def _ctx() -> SimpleNamespace:
    """Return a minimal PluginContext-shaped namespace sufficient for AUTO discovery."""
    return SimpleNamespace(source="fixture-source", options={}, storage=None)


def _bare(cls: type[Any]) -> Any:
    """Construct an ingestor instance without invoking ``BaseIngestor.__init__``.

    ``BaseIngestor.__init__`` requires a runtime ``ChunkManager`` and a full
    plugin context. Discovery-time hooks only touch class-level constants and
    pure ``Path`` computations, so a bare instance is sufficient for
    ``resolve_index_spec_for_ingestor`` to exercise the AUTO discovery path.
    """
    return cast(Any, object.__new__(cls))


def test_deterministic_ordering_produces_byte_identical_identity_hash() -> None:
    """Safety contract 1: identical source state MUST produce byte-identical identity_hash.

    Two invocations of ``resolve_index_spec_for_ingestor`` against the same
    fixture in the same directory MUST return ``IndexBinding`` values whose
    ``resolved.identity_hash`` are byte-equal. Any deviation (unstable dict
    iteration, unsorted manifests, unstable coordinate canonicalisation)
    would silently break freeze detection because a re-resolve with the same
    inputs would produce a different hash than the persisted one and either
    trigger a false conflict or overwrite the frozen record.
    """
    binding1 = resolve_index_spec_for_ingestor(
        _bare(plugin.IrregularAxisReverseOrderIngestor), _ctx()
    )
    binding2 = resolve_index_spec_for_ingestor(
        _bare(plugin.IrregularAxisReverseOrderIngestor), _ctx()
    )

    assert binding1 is not None
    assert binding2 is not None
    assert binding1.resolved.identity_hash == binding2.resolved.identity_hash


def test_duplicate_rejection_names_both_conflicting_item_identities() -> None:
    """Safety contract 2: a coordinate collision MUST raise naming BOTH offending items.

    ``IrregularAxisDuplicateIngestor`` maps two discovered item identities to
    the same timestamp coordinate. Discovery MUST refuse this with
    ``DuplicateIrregularCoordinateError`` (a subclass of ``ConfigurationError``),
    and the message MUST expose both offending item references so the
    operator can locate the collision without re-reading source data.
    """
    with pytest.raises(DuplicateIrregularCoordinateError) as excinfo:
        resolve_index_spec_for_ingestor(_bare(plugin.IrregularAxisDuplicateIngestor), _ctx())

    exc = excinfo.value
    assert exc.coordinate_name == "timestamp"
    assert "2026-01-01T00:00:00" in str(exc.coordinate_value)
    assert exc.first_item
    assert exc.second_item
    assert exc.first_item == "0"
    assert exc.second_item == "0"


def test_freeze_then_late_arrival_refuses_via_identity_hash_mismatch(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Safety contract 3: a persisted manifest MUST refuse a late-arriving item.

    The first ``firecube zarr preallocate`` call freezes the AUTO-discovered
    irregular axis into ``.firecube/index/current.json``. The second call runs
    against the same target after the fixture source set gains a sixth item.
    That changes the resolved-index ``identity_hash`` and MUST raise a
    ``ResolvedIndexConflictError`` instead of silently replacing the frozen
    record.
    """

    target_dir = tmp_path / "out.zarr"
    base_args = [
        "zarr",
        "preallocate",
        "irregular_axis_safe",
        "--target",
        f"file://{target_dir}",
        "--product-name",
        "irregular_axis_safe",
        "--storage-type",
        "local",
        "--storage-driver",
        "fsspec",
        "--write-mode",
        "staged",
    ]

    first_result = CliRunner().invoke(cli, base_args)
    assert first_result.exit_code == 0, first_result.output

    current_json = target_dir / ".firecube" / "index" / "current.json"
    first_bytes = current_json.read_bytes()

    monkeypatch.setattr(plugin, "_ITEM_COUNT", 6)

    second_result = CliRunner().invoke(cli, base_args)
    assert second_result.exit_code != 0, second_result.output
    assert isinstance(second_result.exception, ResolvedIndexConflictError)
    failure_text = f"{type(second_result.exception).__name__}: {second_result.exception}"
    assert (
        "ResolvedIndexConflictError" in failure_text
        or "identity" in failure_text.lower()
        or "conflict" in failure_text.lower()
    ), failure_text
    assert current_json.read_bytes() == first_bytes


def test_empty_discovery_raises_no_discovered_items_error() -> None:
    """Safety contract 4: zero-item discovery MUST raise NoDiscoveredItemsError.

    ``IrregularAxisEmptyIngestor`` returns an empty ``discover_source_files``
    result. Discovery MUST refuse this rather than silently freezing an
    empty axis (which would give an all-fill Zarr store with no way to
    distinguish "not written yet" from "no data exists"). The exception
    MUST expose the coordinate name and source description on typed fields
    so operators do not have to parse message prose.
    """
    with pytest.raises(NoDiscoveredItemsError) as excinfo:
        resolve_index_spec_for_ingestor(_bare(plugin.IrregularAxisEmptyIngestor), _ctx())

    exc = excinfo.value
    assert exc.coordinate_name == "timestamp"
    assert exc.source_ref == "fixture-source"


def test_reverse_order_input_resolves_to_sorted_manifest() -> None:
    """Safety contract 5: manifest items MUST be in sorted coordinate order regardless of discovery order.

    ``IrregularAxisReverseOrderIngestor`` yields items in reverse index
    order (4, 3, 2, 1, 0). The resolved ``ResolvedIndex.items`` manifest
    MUST be canonically sorted by ``coordinate_value``, and the resolver's
    ``coordinate(group, index)`` view MUST expose the same forward order.
    Without this invariant, ``identity_hash`` would depend on discovery
    order and freeze detection would false-alarm on any re-run whose
    file-system iteration order changed.
    """
    binding = resolve_index_spec_for_ingestor(
        _bare(plugin.IrregularAxisReverseOrderIngestor), _ctx()
    )

    assert binding is not None
    assert binding.resolved.items is not None
    coord_values = [entry.coordinate_value for entry in binding.resolved.items]
    assert coord_values == sorted(coord_values)
    axis_coords = [
        binding.resolved.coordinate("data", i) for i in range(binding.resolved.size("data"))
    ]
    assert axis_coords == sorted(axis_coords)


def test_manifest_source_ref_present_supports_lazy_writeintent_closure() -> None:
    """Safety contract 6: each manifest entry MUST carry a non-empty source_ref usable by a lazy closure.

    Lazy ``WriteIntent`` payloads (from plan C) are callables that MUST close
    over the manifest ``source_ref`` string, not over open file handles or
    other lifetime-bound resources. This test documents that contract by
    (a) asserting every entry has a truthy ``source_ref`` and a declared
    ``source_ref_kind`` from the allowed set, and (b) exercising the safe
    closure pattern (capture the string as a default argument, not an open
    handle) to prove the source_ref can be dereferenced after discovery
    returns without depending on any ambient state.
    """
    binding = resolve_index_spec_for_ingestor(_bare(plugin.IrregularAxisSafeIngestor), _ctx())

    assert binding is not None
    assert binding.resolved.items is not None
    allowed_kinds = {"path", "uri", "identifier"}

    captured_refs: list[str] = []
    for entry in binding.resolved.items:
        assert entry.source_ref, f"empty source_ref for entry {entry.identity_hash!r}"
        assert entry.source_ref_kind in allowed_kinds

        ref = entry.source_ref

        def _closure(captured_ref: str = ref) -> str:
            return captured_ref

        captured_refs.append(_closure())

    assert captured_refs == [entry.source_ref for entry in binding.resolved.items]


def test_dry_run_and_preallocate_produce_equal_identity_hash(tmp_path: Any) -> None:
    """Safety contract 7: dry-run identity_hash MUST equal the real preallocate identity_hash.

    A dry-run preallocation surfaces the frozen manifest without writing
    Zarr metadata. When the operator subsequently issues a real
    preallocate against unchanged sources, the resolved-index
    ``identity_hash`` MUST be byte-equal to the dry-run value so a plan
    computed in dry-run mode is a faithful preview of the mutation.
    """
    import json

    from click.testing import CliRunner

    from firecube.cli.main import cli

    target_dir = tmp_path / "out.zarr"
    target_uri = f"file://{target_dir}"
    base_args = [
        "zarr",
        "preallocate",
        "irregular_axis_safe",
        "--target",
        target_uri,
        "--product-name",
        "irregular_axis_safe",
        "--storage-type",
        "local",
        "--storage-driver",
        "fsspec",
        "--write-mode",
        "staged",
    ]

    dry_result = CliRunner().invoke(cli, [*base_args, "--dry-run"])
    assert dry_result.exit_code == 0, dry_result.output
    dry_hash = json.loads(dry_result.output)["identity_hash"]

    real_result = CliRunner().invoke(cli, base_args)
    assert real_result.exit_code == 0, real_result.output

    show_result = CliRunner().invoke(
        cli,
        [
            "zarr",
            "index",
            "show",
            "--target",
            target_uri,
            "--product-name",
            "irregular_axis_safe",
            "--json",
        ],
    )
    assert show_result.exit_code == 0, show_result.output
    real_hash = json.loads(show_result.output)["identity_hash"]

    assert dry_hash == real_hash, (
        f"dry-run identity_hash {dry_hash[:16]}... != "
        f"real preallocate identity_hash {real_hash[:16]}..."
    )
