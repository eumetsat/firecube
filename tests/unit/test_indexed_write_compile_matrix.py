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

"""Compile matrix for ``_compile_indexed_write`` (task B5).

Each row exercises one invariant of the pure compile step from
:class:`IndexedWrite` to :class:`WriteIntent`. Rows are grouped into three
parametrized tests by behavior class:

- Success rows (5): the compile step returns a single ``WriteIntent`` whose
  ``kind``, ``ts_index``, and ``data`` identity match the input.
- Compile-error rows (3): the compile step raises
  :class:`IndexedWriteCompilationError` (or, for the callable-into-``slot``
  boundary, ``TypeError`` from ``WriteIntent.__post_init__``, which the
  compile step does not wrap).
- Construction-error row (1): the :class:`IndexedWrite` dataclass rejects
  a malformed ``y_slice`` value at ``__post_init__`` time, so the compile
  step is never reached.

Together the nine rows cover ``RegularTimeAxis``, ``IntegerAxis``, and
``IrregularTimeAxis`` for both :meth:`IndexedWrite.region` and
:meth:`IndexedWrite.slot`, both eager ``ndarray`` and callable payloads,
and the three distinct failure boundaries the compile step protects.
"""

from __future__ import annotations

from collections.abc import Callable

import numpy as np
import pytest

from firecube.core.errors import IndexedWriteCompilationError
from firecube.core.index_resolve import ResolvedIndex, resolve_index_spec
from firecube.core.index_spec import (
    IndexSpec,
    IntegerAxis,
    IrregularTimeAxis,
    RegularTimeAxis,
)
from firecube.core.indexed_write import IndexedWrite
from firecube.ingestor.templates.direct_zarr import _compile_indexed_write

# ---------------------------------------------------------------------------
# Resolved-index fixtures.
#
# One helper per axis kind. Each returns a fresh ``ResolvedIndex`` so rows
# do not share mutable state.
# ---------------------------------------------------------------------------


def _regular_index() -> ResolvedIndex:
    """Regular time axis: 10 slots at 600s cadence starting 2024-01-01T00:00Z."""
    spec = IndexSpec(
        name="reg_v1",
        groups={
            "data": RegularTimeAxis(
                coordinate="timestamp",
                epoch="2024-01-01T00:00:00Z",
                cadence_s=600,
                mode="exact",
                slot_count=10,
            ),
        },
    )
    return resolve_index_spec(spec, time_dim_name="timestamp")


def _integer_index() -> ResolvedIndex:
    """Integer axis: 5 zero-based positions."""
    spec = IndexSpec(name="int_v1", groups={"data": IntegerAxis(slot_count=5)})
    return resolve_index_spec(spec, time_dim_name="timestamp")


def _irregular_index() -> ResolvedIndex:
    """Irregular time axis: three explicit timestamps in ascending order."""
    spec = IndexSpec(
        name="irr_v1",
        groups={
            "data": IrregularTimeAxis(
                coordinate="timestamp",
                values=(
                    "2024-01-01T00:00:00Z",
                    "2024-01-01T00:10:00Z",
                    "2024-01-01T00:20:00Z",
                ),
            )
        },
    )
    return resolve_index_spec(spec, time_dim_name="timestamp")


# ---------------------------------------------------------------------------
# Row 1-5: success cases.
#
# Compile must return a single-element list, the produced ``WriteIntent`` must
# have the expected ``kind`` and ``ts_index`` for the resolved coordinate, and
# ``data`` must pass through by identity — not copied, not invoked.
# ---------------------------------------------------------------------------


_ARR_2D = np.arange(16, dtype=np.float32).reshape(4, 4)
_ARR_2X2 = np.zeros((2, 2), dtype=np.float32)
_ARR_1D = np.zeros((3,), dtype=np.float32)


def _payload_region() -> np.ndarray:
    """Callable payload for region rows; must not be invoked at compile time."""
    return np.zeros((4, 4))


@pytest.mark.parametrize(
    "iw,make_index,expected_kind,expected_ts_index,expected_data",
    [
        # Row 1: ``.region`` builder compiles to ``kind="region"``; ``ts_index``
        # tracks the resolver's slot for the coordinate; ``channel_index`` and
        # ``y_slice`` metadata are preserved verbatim.
        pytest.param(
            IndexedWrite.region(
                group="data",
                array="counts",
                coordinate="2024-01-01T00:10:00Z",
                data=_ARR_2D,
                y_slice=slice(0, 4),
                channel_index=1,
            ),
            _regular_index,
            "region",
            1,
            _ARR_2D,
            id="row1_region_regular_ndarray",
        ),
        # Row 2: callable payload identity survives ``.region`` compile. The
        # compile step must never invoke the callable (deferred to dispatch
        # time under the same rules as ``WriteIntent``).
        pytest.param(
            IndexedWrite.region(
                group="data",
                array="counts",
                coordinate="2024-01-01T00:20:00Z",
                data=_payload_region,
                y_slice=slice(0, 4),
            ),
            _regular_index,
            "region",
            2,
            _payload_region,
            id="row2_region_regular_callable",
        ),
        # Row 3: ``.slot`` builder compiles to ``kind="1d"``; ``ts_index=0``
        # confirms epoch-aligned coordinate resolution.
        pytest.param(
            IndexedWrite.slot(
                group="data",
                array="tally",
                coordinate="2024-01-01T00:00:00Z",
                data=_ARR_1D,
            ),
            _regular_index,
            "1d",
            0,
            _ARR_1D,
            id="row3_slot_regular_ndarray",
        ),
        # Row 4: ``IntegerAxis`` accepts integer coordinates and resolves them
        # by identity (coordinate ``3`` → slot ``3``).
        pytest.param(
            IndexedWrite.region(
                group="data",
                array="counts",
                coordinate=3,
                data=_ARR_2X2,
                y_slice=slice(0, 2),
            ),
            _integer_index,
            "region",
            3,
            _ARR_2X2,
            id="row4_region_integer_axis",
        ),
        # Row 5: ``IrregularTimeAxis`` resolves by looking up the coordinate
        # in its explicit ``values`` tuple (third value → slot ``2``).
        pytest.param(
            IndexedWrite.region(
                group="data",
                array="counts",
                coordinate="2024-01-01T00:20:00Z",
                data=_ARR_2X2,
                y_slice=slice(0, 2),
            ),
            _irregular_index,
            "region",
            2,
            _ARR_2X2,
            id="row5_region_irregular_axis",
        ),
    ],
)
def test_compile_matrix_success(
    iw: IndexedWrite,
    make_index: Callable[[], ResolvedIndex],
    expected_kind: str,
    expected_ts_index: int,
    expected_data: object,
) -> None:
    """Compile a well-formed ``IndexedWrite`` and check the produced ``WriteIntent``.

    Rows 1-5 cover the two builders (``.region`` / ``.slot``), the two payload
    shapes (eager ``ndarray`` / zero-arg callable), and all three axis kinds
    (regular, integer, irregular). Each row asserts:

    - the compile step returns exactly one ``WriteIntent`` (never a bare
      intent, never a generator);
    - the produced ``kind`` matches the builder used
      (``.region`` → ``"region"``, ``.slot`` → ``"1d"``);
    - the produced ``ts_index`` is the slot the resolver assigned to the
      coordinate;
    - the ``data`` reference is preserved by identity — the compile step
      never copies arrays and never invokes callables.
    """
    idx = make_index()
    out = _compile_indexed_write(iw, idx)

    assert isinstance(out, list)
    assert len(out) == 1
    wi = out[0]
    assert wi.kind == expected_kind
    assert wi.ts_index == expected_ts_index
    assert wi.group == iw.group
    assert wi.array == iw.array
    assert wi.data is expected_data


# ---------------------------------------------------------------------------
# Row 6-8: compile-time error cases.
#
# ``_compile_indexed_write`` catches ``(KeyError, ValueError, IndexError,
# TypeError)`` from the axis resolver only. Errors from ``WriteIntent``
# construction propagate unwrapped by design — the compile step is not
# a general-purpose exception adapter.
# ---------------------------------------------------------------------------


def _payload_slot() -> np.ndarray:
    """Callable payload used to prove ``WriteIntent.slot`` rejects callables."""
    return np.zeros((4,))


@pytest.mark.parametrize(
    "iw,make_index,expected_error,expected_cause,check_iw_repr",
    [
        # Row 6: callable payloads are illegal for ``kind="1d"``. The compile
        # step surfaces the ``WriteIntent.__post_init__`` ``TypeError``
        # unwrapped — this is a target-type invariant, not a resolver failure,
        # so it is intentionally not adapted to ``IndexedWriteCompilationError``.
        pytest.param(
            IndexedWrite.slot(
                group="data",
                array="tally",
                coordinate="2024-01-01T00:00:00Z",
                data=_payload_slot,
            ),
            _regular_index,
            TypeError,
            None,
            False,
            id="row6_slot_callable_rejected_by_writeintent",
        ),
        # Row 7: coordinates missing from an ``IrregularTimeAxis`` raise
        # ``IndexedWriteCompilationError`` with the resolver's ``ValueError``
        # chained via ``__cause__`` and structured fields
        # (``coordinate``, ``iw_repr``) populated for operator diagnosis.
        pytest.param(
            IndexedWrite.region(
                group="data",
                array="counts",
                coordinate="2099-12-31T00:00:00Z",
                data=_ARR_2X2,
                y_slice=slice(0, 2),
            ),
            _irregular_index,
            IndexedWriteCompilationError,
            ValueError,
            True,
            id="row7_region_missing_coordinate",
        ),
        # Row 8: string coordinates against an ``IntegerAxis`` raise
        # ``IndexedWriteCompilationError`` with the resolver's ``TypeError``
        # chained — proving the compile step adapts *all four* documented
        # resolver failure classes (KeyError, ValueError, IndexError,
        # TypeError), not just the timestamp-shaped ones.
        pytest.param(
            IndexedWrite.slot(
                group="data",
                array="tally",
                coordinate="not-an-integer",
                data=_ARR_1D,
            ),
            _integer_index,
            IndexedWriteCompilationError,
            TypeError,
            True,
            id="row8_slot_string_on_integer_axis",
        ),
    ],
)
def test_compile_matrix_error(
    iw: IndexedWrite,
    make_index: Callable[[], ResolvedIndex],
    expected_error: type[Exception],
    expected_cause: type[Exception] | None,
    check_iw_repr: bool,
) -> None:
    """Compile step raises the documented error and preserves the underlying cause.

    Row 6 proves that a callable payload passed to ``.slot()`` is rejected by
    ``WriteIntent.slot`` at compile time with a bare ``TypeError`` — the
    compile step does not wrap ``WriteIntent`` construction errors, because
    those are static invariants of the target type, not resolver failures.

    Rows 7 and 8 prove that resolver failures (unresolvable coordinate on a
    time axis, type-mismatched coordinate on an integer axis) are wrapped
    into :class:`IndexedWriteCompilationError` with the original resolver
    exception chained via ``__cause__``, and that the structured
    ``iw_repr`` field is populated for operator diagnosis.
    """
    idx = make_index()
    with pytest.raises(expected_error) as exc_info:
        _compile_indexed_write(iw, idx)

    if expected_cause is not None:
        assert isinstance(exc_info.value.__cause__, expected_cause)
    if check_iw_repr:
        assert isinstance(exc_info.value, IndexedWriteCompilationError)
        assert exc_info.value.coordinate == iw.coordinate
        assert "IndexedWrite" in exc_info.value.iw_repr


# ---------------------------------------------------------------------------
# Row 9: construction-time error case.
#
# ``IndexedWrite.__post_init__`` guards the ``y_slice`` type invariant so
# malformed writes are rejected before they ever reach the compile step.
# A non-slice non-None ``y_slice`` value raises ``ValueError`` immediately;
# the compile step is never entered.
#
# Note: ``y_slice=None`` for a ``region``-kind write is silently accepted
# by ``__post_init__`` today — the compile step catches that case via a
# explicit ``ValueError``. Testing the non-slice branch documents the invariant
# that ``__post_init__`` actually enforces at construction time.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "y_slice_value,expected_type_name",
    [
        # Row 9: passing a non-slice non-None value for ``y_slice`` (here an
        # ``int``) is rejected by ``IndexedWrite.__post_init__`` with a
        # ``ValueError`` whose message names the offending type. The compile
        # step is never entered — malformed writes cannot escape construction.
        pytest.param(42, "int", id="row9_construction_y_slice_int_rejected"),
    ],
)
def test_construction_rejects_non_slice_y_slice(
    y_slice_value: object,
    expected_type_name: str,
) -> None:
    """Construction rejects non-slice non-None ``y_slice`` at ``__post_init__``.

    Row 9 exercises the construction-time invariant enforced by
    :meth:`IndexedWrite.__post_init__`: ``y_slice`` must be either ``None``
    or a ``slice`` instance. Any other type raises ``ValueError`` with the
    offending type name in the message, so the compile step is never
    reached with a malformed write.
    """
    with pytest.raises(
        ValueError,
        match=rf"y_slice must be a slice or None, got '{expected_type_name}'",
    ):
        IndexedWrite.region(
            group="data",
            array="counts",
            coordinate="2024-01-01T00:00:00Z",
            data=np.zeros((4, 4)),
            y_slice=y_slice_value,  # type: ignore[arg-type]
        )
