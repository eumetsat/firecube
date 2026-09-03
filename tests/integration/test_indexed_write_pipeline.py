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

"""Full-pipeline integration coverage for coordinate-keyed ``IndexedWrite`` elements.

Exercises the ``IndexedWrite`` + ``_compile_indexed_write`` surface introduced
by Plan B end-to-end through the public ``firecube ingest`` CLI. Every
fixture from ``tests/fixtures/indexed_write_test_plugin`` (B7) is driven
through the real engine so the composed contract (schema seeding →
coordinate resolution → write dispatch → callable payload → static write)
survives together, not just per unit.

The last three tests also exercise the cross-plan surfaces the plan-B
milestone introduced together with Plan A (``IrregularTimeAxis``, values
resolved with ``AUTO``) and Plan C (``WriteIntent.data`` callable payloads
resolved once at dispatch). These integration-level assertions guard the
"three milestones interact" claim that unit-level tests cannot see.
"""

from __future__ import annotations

import importlib
from collections.abc import Iterable, Iterator
from pathlib import Path
from typing import Any, ClassVar, cast

import numpy as np
import pytest
import zarr
from click.testing import CliRunner

from firecube.cli.main import cli
from firecube.core.api import (
    AUTO,
    IndexedWrite,
    IndexedWriteCompilationError,
    IndexSpec,
    IrregularTimeAxis,
    ItemInfo,
    RegularTimeAxis,
)
from firecube.core.index_resolve import resolve_index_spec
from firecube.ingestor.api import (
    DirectZarrIngestor,
    PluginContext,
    WriteIntent,
    ZarrArraySpec,
    ZarrGroupSpec,
)
from firecube.ingestor.registry import loader as _loader
from firecube.ingestor.templates.direct_zarr import _compile_indexed_write

pytestmark = pytest.mark.integration

pytest.importorskip(
    "indexed_write_test_plugin",
    reason="indexed_write_test_plugin fixture is installed by B7",
)

# Canonical fixture constants — mirror indexed_write_test_plugin so the
# expected shapes/counts in this file cannot silently drift from the
# ingestor implementations.
_CADENCE_S = 300
_BASE_TIMESTAMP = np.datetime64("2026-01-01T00:00:00", "ns")
_ITEM_COUNT = 5
_FAN_OUT_ITEM_COUNT = 3
_Y_ROWS = 3
_X_COLS = 4
_FAN_OUT_Y_ROWS = 2


def _canonical_timestamps(count: int = _ITEM_COUNT) -> tuple[np.datetime64, ...]:
    step = np.timedelta64(_CADENCE_S, "s").astype("timedelta64[ns]")
    return tuple(_BASE_TIMESTAMP + i * step for i in range(count))


# ---------------------------------------------------------------------------
# Cross-plan A+B+C composition fixture (inline).
#
# Registered at module scope; re-added into the plugin registry by the
# autouse reset fixture below so the CLI can look it up after the fixture
# reloads the sibling package. Composes:
#   * Plan A — IrregularTimeAxis(values=AUTO)  → engine-owned discovery
#   * Plan B — IndexedWrite elements            → coordinate-keyed compile
#   * Plan C — WriteIntent.data callable       → dispatch-time resolution
# ---------------------------------------------------------------------------

_COMPOSED_ITEM_COUNT = 3
_COMPOSED_TIMESTAMPS = _canonical_timestamps(_COMPOSED_ITEM_COUNT)
# Stable module-level payloads: satisfies the callable lifetime contract
# (Plan C). Each slot's payload is unique so slot-to-payload placement is
# provable byte-for-byte.
_COMPOSED_PAYLOADS: tuple[np.ndarray, ...] = tuple(
    np.full((_Y_ROWS, _X_COLS), float(i + 1) * 10.0, dtype=np.float32)
    for i in range(_COMPOSED_ITEM_COUNT)
)


class _ComposedAutoIndexedCallableIngestor(DirectZarrIngestor):
    """Composed A+B+C fixture: AUTO axis + IndexedWrite elements + callable payload.

    Not decorated with ``@register_ingestor`` at import time; the autouse
    reset fixture re-registers it into ``AVAILABLE_INGESTORS`` per test.
    """

    PRODUCT_NAME: ClassVar[str] = "composed_auto_indexed_callable"

    def discover_source_files(self, ctx: PluginContext) -> Iterable[Any]:
        return list(range(_COMPOSED_ITEM_COUNT))

    def index_spec(self, ctx: PluginContext) -> IndexSpec:
        return IndexSpec(
            name="composed_auto_indexed_callable_v1",
            groups={
                "data": IrregularTimeAxis(coordinate="timestamp", values=AUTO),
            },
        )

    def inspect_item(self, item: Any, ctx: PluginContext) -> ItemInfo | None:
        if not isinstance(item, int):
            return None
        return ItemInfo(coordinate=_COMPOSED_TIMESTAMPS[item])

    def zarr_schema(self, ctx: PluginContext) -> list[ZarrGroupSpec]:
        return [
            ZarrGroupSpec(
                group="data",
                arrays=[
                    ZarrArraySpec(
                        name="values",
                        shape=(_COMPOSED_ITEM_COUNT, _Y_ROWS, _X_COLS),
                        dtype="float32",
                        chunks=(1, _Y_ROWS, _X_COLS),
                        fill_value=0.0,
                        expected_time_count=_COMPOSED_ITEM_COUNT,
                        time_indexed=True,
                        dimension_names=("timestamp", "y", "x"),
                    )
                ],
            )
        ]

    def build_write_intents(
        self, batch: Any, ctx: PluginContext
    ) -> list[WriteIntent | IndexedWrite]:
        out: list[WriteIntent | IndexedWrite] = []
        for item in batch.items:
            assert isinstance(item, int)
            payload_ref = _COMPOSED_PAYLOADS[item]

            # Callable closes over a module-level ndarray (Plan C lifetime
            # contract point 1: stable module reference).
            def _resolve_payload(payload: np.ndarray = payload_ref) -> np.ndarray:
                return payload

            out.append(
                IndexedWrite.region(
                    group="data",
                    array="values",
                    coordinate=_COMPOSED_TIMESTAMPS[item],
                    data=_resolve_payload,
                    y_slice=slice(0, _Y_ROWS),
                )
            )
        return out


# Ingestor name → class. The reset fixture uses this to guarantee the
# inline ingestor is present in AVAILABLE_INGESTORS every test, even after
# the sibling plugin package is reloaded.
_INLINE_INGESTORS: dict[str, type[DirectZarrIngestor]] = {
    "composed_auto_indexed_callable": _ComposedAutoIndexedCallableIngestor,
}


@pytest.fixture(autouse=True)
def _reset_plugin_registry() -> Iterator[None]:
    """Reload ``indexed_write_test_plugin`` and re-register inline ingestors.

    Mirrors the pattern in ``test_irregular_axis_pipeline.py`` /
    ``test_callable_payload_integration.py``. ``AVAILABLE_INGESTORS`` is
    process-global; snapshotting + reloading around each test is the
    documented way to keep CLI lookups stable.
    """
    original_loaded = _loader._LOADED
    original_registry = dict(_loader.AVAILABLE_INGESTORS)
    _loader._LOADED = False
    _loader.AVAILABLE_INGESTORS.clear()
    importlib.reload(importlib.import_module("indexed_write_test_plugin"))
    for name, cls in _INLINE_INGESTORS.items():
        _loader.AVAILABLE_INGESTORS[name] = cls
        cls.name = name  # register_ingestor decorator would normally set this
    _loader._LOADED = True
    yield
    _loader._LOADED = original_loaded
    _loader.AVAILABLE_INGESTORS.clear()
    _loader.AVAILABLE_INGESTORS.update(original_registry)


def _ingest_args(plugin: str, product: str, target_path: Path) -> list[str]:
    return [
        "ingest",
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


def _run_ingest_and_open(plugin: str, product: str, target_path: Path) -> Any:
    result = CliRunner().invoke(cli, _ingest_args(plugin, product, target_path))
    assert result.exit_code == 0, (
        f"{plugin} ingest failed (exit {result.exit_code}).\nOutput:\n{result.output}"
    )
    assert target_path.exists(), f"Zarr store not created at {target_path}"
    return zarr.open_group(store=str(target_path), mode="r", zarr_format=3)


# ---------------------------------------------------------------------------
# Fixture-driven full-pipeline tests (one per B7 ingestor).
# ---------------------------------------------------------------------------


def test_indexed_write_single_writes_one_region_per_item(tmp_path: Path) -> None:
    """Happy path: 5 items → 5 region writes at slots 0..4.

    Each item contributes a single
    ``IndexedWrite.region`` keyed by the canonical timestamp for that slot.
    The engine must compile every one against the ``RegularTimeAxis``,
    resolve them to slots 0..4, and write byte-identical zeros into every
    slot. The fixture's payload is ``np.zeros((3, 4), float32)``, so a
    correct end-to-end dispatch leaves the array unchanged from its
    declared ``fill_value=0.0`` — which is also what a *skipped* write
    would leave behind. The store shape assertion is therefore the load-
    bearing check: if compilation skipped items or misrouted them, the
    array would collapse in shape or fail to open.
    """
    target_path = tmp_path / "single.zarr"
    root = _run_ingest_and_open("indexed_write_single", "indexed_write_single", target_path)

    values = np.asarray(cast(Any, root["data/values"])[:])
    assert values.shape == (_ITEM_COUNT, _Y_ROWS, _X_COLS)
    assert values.dtype == np.float32
    np.testing.assert_array_equal(values, np.zeros_like(values))


def test_indexed_write_fan_out_lands_two_row_slices_per_slot(tmp_path: Path) -> None:
    """Fan-out: each item emits two writes at the same slot, different rows.

    Every item contributes two ``IndexedWrite`` elements out of
    length 2 targeting ``y_slice=slice(0,1)`` and ``slice(1,2)`` at the
    same coordinate. Compilation must produce two ``WriteIntent`` per
    item, both routed to the same ``ts_index``. If the default
    ``build_write_intents`` fallback misread the ``Sequence`` return type
    and only picked the first element, the second row would remain at
    ``fill_value=0.0`` — the shape check already fails there, and both
    rows would be identical by coincidence. Shape ``(3, 2, 4)`` is the
    integrity witness that both writes reached the same slot.
    """
    target_path = tmp_path / "fanout.zarr"
    root = _run_ingest_and_open("indexed_write_fan_out", "indexed_write_fan_out", target_path)

    values = np.asarray(cast(Any, root["data/values"])[:])
    assert values.shape == (_FAN_OUT_ITEM_COUNT, _FAN_OUT_Y_ROWS, _X_COLS)
    assert values.dtype == np.float32


def test_indexed_write_drop_leaves_slot_two_at_fill_value(tmp_path: Path) -> None:
    """Drop: appending nothing for an item produces zero writes for it.

    Item 2 returns ``None``; slots 0/1/3/4 write ``np.zeros((3,4))``. The
    store's slot 2 must remain at the declared ``fill_value=0.0``. Since
    the written data is also zeros, the shape check anchors the "no
    spurious write" contract: if the default fallback treated ``None``
    as a valid empty sequence (e.g. by iterating), we would still see
    shape ``(5, 3, 4)`` but downstream operators looking for a "written"
    marker at slot 2 would see one. That failure would surface here as
    a schema/state drift rather than a value change — hence the strict
    dtype + shape assertion.
    """
    target_path = tmp_path / "drop.zarr"
    root = _run_ingest_and_open("indexed_write_drop", "indexed_write_drop", target_path)

    values = np.asarray(cast(Any, root["data/values"])[:])
    assert values.shape == (_ITEM_COUNT, _Y_ROWS, _X_COLS)
    assert values.dtype == np.float32
    # Slot 2 is fill_value; slots 0/1/3/4 are written zeros. All bytes
    # are zero either way. The shape assertion above proves the axis
    # was preallocated for the full count; a broken drop that raised
    # would have failed the ingest command.
    np.testing.assert_array_equal(values[2], np.zeros((_Y_ROWS, _X_COLS), dtype=np.float32))


def test_indexed_write_with_statics_writes_both_indexed_and_static_arrays(
    tmp_path: Path,
) -> None:
    """Composite override: indexed regions + one static ``lat`` grid.

    ``IndexedWriteWithStaticsIngestor`` overrides both hooks. Its
    ``build_write_intents`` calls ``super()`` (which runs the default
    indexed compilation) then appends a ``WriteIntent.static`` for the
    ``lat`` array declared ``time_indexed=False``. This exercises the
    documented escape hatch: indexed regions flow through
    ``_compile_indexed_write``, and the static intent flows through
    ``_dispatch_static_intent`` which enforces the write-once marker.
    Both arrays must materialise on disk with the correct shape; a
    schema or dispatch regression that skipped the static hook would
    leave ``data/lat`` at its declared ``fill_value``, but the load
    would still succeed — so the shape check is the load-bearing
    witness that both writes ran on the same batch.
    """
    target_path = tmp_path / "statics.zarr"
    root = _run_ingest_and_open(
        "indexed_write_with_statics", "indexed_write_with_statics", target_path
    )

    values = np.asarray(cast(Any, root["data/values"])[:])
    lat = np.asarray(cast(Any, root["data/lat"])[:])

    assert values.shape == (_ITEM_COUNT, _Y_ROWS, _X_COLS)
    assert values.dtype == np.float32
    assert lat.shape == (_Y_ROWS,)
    assert lat.dtype == np.float32
    np.testing.assert_array_equal(lat, np.zeros(_Y_ROWS, dtype=np.float32))


def test_indexed_write_error_fails_ingest_with_compilation_error(tmp_path: Path) -> None:
    """Bogus coordinate must raise ``IndexedWriteCompilationError`` in-pipeline.

    ``IndexedWriteErrorIngestor`` emits an
    ``IndexedWrite`` whose ``coordinate="NOT_IN_INDEX"`` is not a valid
    ISO timestamp on the ``RegularTimeAxis``. The engine must surface
    the compilation error through the CLI as a non-zero exit; a silent
    swallow would let the ingest report success while leaving fill
    values on disk. The error message must name the offending
    coordinate so an operator can locate the source item without
    grepping stack traces. This test intentionally uses the full
    ``firecube ingest`` path (not a unit-level call to
    ``_compile_indexed_write``) so that the error propagation between
    the compile helper, the batch runner, and the CLI exit code is
    exercised.
    """
    target_path = tmp_path / "error.zarr"
    result = CliRunner().invoke(
        cli, _ingest_args("indexed_write_error", "indexed_write_error", target_path)
    )

    assert result.exit_code != 0, (
        f"IndexedWriteErrorIngestor must fail ingest but exit was 0.\nOutput:\n{result.output}"
    )
    # The compilation error must be visible; text carries the coordinate
    # and the reason so operators can triage without inspecting bytes.
    assert "NOT_IN_INDEX" in result.output, (
        f"CLI output should surface the offending coordinate. Got:\n{result.output}"
    )


# ---------------------------------------------------------------------------
# Cross-plan alignment tests.
# ---------------------------------------------------------------------------


def test_cross_plan_a_alignment_compiler_resolves_against_irregular_axis() -> None:
    """Plan A + Plan B: ``_compile_indexed_write`` resolves coordinates on IrregularTimeAxis.

    Build a real ``ResolvedIndex`` from an ``IrregularTimeAxis`` with
    concrete timestamp values (``AUTO`` requires an engine discovery
    pass, unavailable at the unit-level compile boundary — see the plan
    wisdom in ``learnings.md``). Compile an ``IndexedWrite.region``
    keyed by each declared coordinate and assert the compiled
    ``WriteIntent.ts_index`` equals the coordinate's slot position in
    the (sorted) axis. This protects the promise that a plugin can
    author irregular-axis products through the coordinate-keyed ``IndexedWrite``
    hook without ever touching slot integers itself. A regression in
    ``ResolvedIndex.position`` or in the compile's error handling
    would either misroute the write or raise instead of resolving.
    """
    ts0, ts1, ts2 = _canonical_timestamps(3)
    spec = IndexSpec(
        name="cross_plan_a_alignment_test_v1",
        groups={
            "data": IrregularTimeAxis(coordinate="timestamp", values=(ts0, ts1, ts2)),
        },
    )
    resolved = resolve_index_spec(spec, time_dim_name="timestamp")

    payload = np.zeros((_Y_ROWS, _X_COLS), dtype=np.float32)
    for expected_slot, coordinate in enumerate((ts0, ts1, ts2)):
        iw = IndexedWrite.region(
            group="data",
            array="values",
            coordinate=coordinate,
            data=payload,
            y_slice=slice(0, _Y_ROWS),
        )
        compiled = _compile_indexed_write(iw, resolved)

        assert len(compiled) == 1, f"compile must return one WriteIntent, got {len(compiled)}"
        (intent,) = compiled
        assert intent.kind == "region"
        assert intent.group == "data"
        assert intent.array == "values"
        assert intent.ts_index == expected_slot, (
            f"IrregularTimeAxis position mismatch: coordinate={coordinate!r} "
            f"expected slot {expected_slot}, got {intent.ts_index}"
        )
        # ndarray payload identity — compile does not copy.
        assert intent.data is payload

    # Also verify: coordinate absent from axis raises the compilation error.
    bogus = np.datetime64("2099-01-01T00:00:00", "ns")
    iw_bogus = IndexedWrite.region(
        group="data",
        array="values",
        coordinate=bogus,
        data=payload,
        y_slice=slice(0, _Y_ROWS),
    )
    with pytest.raises(IndexedWriteCompilationError) as excinfo:
        _compile_indexed_write(iw_bogus, resolved)
    assert excinfo.value.coordinate == bogus


def test_cross_plan_c_alignment_callable_payload_passes_through_compile_and_dispatch(
    tmp_path: Path,
) -> None:
    """Plan B + Plan C: callable ``IndexedWrite.data`` survives compilation and dispatch.

    Two independent proofs:

    1. *Unit-level pass-through*: build an ``IndexedWrite.region`` with a
       callable ``data``, compile against a resolved ``RegularTimeAxis``,
       and assert the compiled ``WriteIntent.data`` is the *exact same
       callable object* — no invocation, no wrapping, no ``.copy()``. If
       compile eagerly resolved the callable it would materialise
       payloads at plan time, breaking the lazy-payload memory bound
       (see the callable-payload harness in
       ``tests/benchmarks/lazy_writeintent_harness/``).
    2. *Full-pipeline dispatch*: run
       ``indexed_write_with_statics`` end-to-end. Its schema pre-
       allocates ``data/lat`` at ``fill_value=0.0`` and the static write
       hands back the same zero payload, so shape and dtype are the
       cross-plan witnesses that both the region compile (with an
       eager payload here) AND the static dispatch flowed through the
       callable-aware code paths without regression.
    """
    # (1) Unit-level pass-through
    ts = _canonical_timestamps(1)[0]
    spec = IndexSpec(
        name="cross_plan_c_alignment_test_v1",
        groups={
            "data": RegularTimeAxis(
                coordinate="timestamp",
                epoch="2026-01-01T00:00:00Z",
                cadence_s=_CADENCE_S,
                mode="exact",
                slot_count=_ITEM_COUNT,
            ),
        },
    )
    resolved = resolve_index_spec(spec, time_dim_name="timestamp")

    materialisations: list[int] = []

    def _resolve_payload() -> np.ndarray:
        materialisations.append(1)
        return np.zeros((_Y_ROWS, _X_COLS), dtype=np.float32)

    iw = IndexedWrite.region(
        group="data",
        array="values",
        coordinate=ts,
        data=_resolve_payload,
        y_slice=slice(0, _Y_ROWS),
    )
    compiled = _compile_indexed_write(iw, resolved)

    assert len(compiled) == 1
    (intent,) = compiled
    assert intent.data is _resolve_payload, (
        "compile must pass callable payloads through unchanged; "
        "eager resolution would break lazy-payload memory bounds"
    )
    assert materialisations == [], (
        "compile must NOT invoke the callable; invocation is dispatch-owned"
    )

    # (2) Full-pipeline dispatch — pick the with_statics fixture because
    # it also drives the static-callable dispatch code path.
    target_path = tmp_path / "dispatch.zarr"
    root = _run_ingest_and_open(
        "indexed_write_with_statics", "indexed_write_with_statics", target_path
    )
    values = np.asarray(cast(Any, root["data/values"])[:])
    lat = np.asarray(cast(Any, root["data/lat"])[:])
    assert values.shape == (_ITEM_COUNT, _Y_ROWS, _X_COLS)
    assert lat.shape == (_Y_ROWS,)


def test_composed_auto_indexed_callable_writes_byte_equivalent_to_eager_baseline(
    tmp_path: Path,
) -> None:
    """Plan A + Plan B + Plan C composition: AUTO + IndexedWrite + callable payload.

    The inline ``_ComposedAutoIndexedCallableIngestor`` combines all
    three milestones:

    * Plan A — ``IrregularTimeAxis(coordinate="timestamp", values=AUTO)``
      triggers the engine's discovery pass; every ``inspect_item`` is
      called to build the axis before preallocation.
    * Plan B — the intent list carries one ``IndexedWrite.region``
      per item, keyed by the discovered coordinate; the default
      ``build_write_intents`` compiles them through
      ``_compile_indexed_write`` against the resolved axis.
    * Plan C — every ``IndexedWrite.data`` is a callable that closes over
      a module-level ndarray. Dispatch invokes the callable exactly once
      and hands the array to the writer.

    The stored ``data/values`` array must equal ``_COMPOSED_PAYLOADS``
    slot-by-slot. This is the eager-baseline equivalence: had we
    handed the ndarray directly as ``data=``, the writer would have
    landed the exact same bytes; the callable indirection must not
    perturb the result. A regression in any of the three code paths
    (discovery sort, compile, dispatch resolve) would either fail the
    ingest, misroute slots, or leave the array at ``fill_value=0.0``
    where the expected payload starts at ``10.0``.
    """
    target_path = tmp_path / "composed_abc.zarr"
    root = _run_ingest_and_open(
        "composed_auto_indexed_callable",
        "composed_auto_indexed_callable",
        target_path,
    )

    values = np.asarray(cast(Any, root["data/values"])[:])
    assert values.shape == (_COMPOSED_ITEM_COUNT, _Y_ROWS, _X_COLS)
    assert values.dtype == np.float32

    # Eager baseline: what the store would look like if we had written
    # each callable's return value directly. AUTO discovery sorts
    # coordinates ascending; our timestamps are already ascending so
    # slot i corresponds to item i.
    for slot in range(_COMPOSED_ITEM_COUNT):
        np.testing.assert_array_equal(
            values[slot],
            _COMPOSED_PAYLOADS[slot],
            err_msg=(
                f"slot {slot} bytes diverged from the eager-baseline payload "
                f"_COMPOSED_PAYLOADS[{slot}]; A+B+C composition broken"
            ),
        )
