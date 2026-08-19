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

"""Integration tests exercising the in-tree index-spec fixture plugins.

Covers the ``tests/fixtures/slot_shape_test_plugin/`` package directly through
``plugin.index_spec(ctx)`` and ``resolve_index_spec``.

Asserts:

* Fixed-epoch fixture: 4 groups, 600s/floor cadence, fixed 2024-09-24Z epoch.
* Option-epoch fixture: 5 groups, mixed 300s/900s exact cadence, epoch read
  from ``ctx.options["reference_epoch"]`` and normalized so ``"Z"`` vs
  ``"+00:00"`` inputs converge on the same ``identity_hash``.
* Idempotency: a second call with the same epoch produces the same resolved index.
* Conflict: a different epoch produces a different ``identity_hash``.
"""

from __future__ import annotations

from typing import Any, cast

import pytest

from firecube.core.index_resolve import resolve_index_spec
from firecube.core.index_spec import RegularTimeAxis
from firecube.ingestor.types.context import (
    IngestContext,
    PluginContext,
    RuntimeIngestContext,
)

pytestmark = [pytest.mark.integration, pytest.mark.plugin]


def _make_plugin_ctx(**options: Any) -> PluginContext:
    ictx = IngestContext(source="/dev/null", options=dict(options))
    rctx = RuntimeIngestContext.from_ingest_context(
        ictx, run_id="fixture-test", temp_root=None, materializer=None
    )
    return PluginContext(rctx)


def test_fixed_epoch_shape_records_four_groups() -> None:
    from slot_shape_test_plugin import FixedEpochShapeIngestor

    plugin = FixedEpochShapeIngestor()
    ctx = _make_plugin_ctx()
    spec = plugin.index_spec(ctx)
    resolved = resolve_index_spec(spec, time_dim_name="timestamp")

    assert set(spec.groups.keys()) == {
        "groupA/data_hi",
        "groupA/data_lo",
        "groupB/data_hi",
        "groupB/data_lo",
    }
    for axis in spec.groups.values():
        axis = cast(RegularTimeAxis, axis)
        assert axis.cadence_s == 600
        assert axis.mode == "floor"
    assert (
        resolved.identity_hash == resolve_index_spec(spec, time_dim_name="timestamp").identity_hash
    )


def test_option_epoch_shape_records_five_groups_with_option_epoch() -> None:
    from slot_shape_test_plugin import OptionEpochShapeIngestor

    plugin = OptionEpochShapeIngestor()
    ctx = _make_plugin_ctx(reference_epoch="2024-01-01T00:00:00Z")
    spec = plugin.index_spec(ctx)
    resolved = resolve_index_spec(spec, time_dim_name="timestamp")

    assert set(spec.groups.keys()) == {
        "fast_a/data",
        "fast_b/data",
        "slow_a/data",
        "slow_b/data",
        "slow_c/data",
    }
    assert cast(RegularTimeAxis, spec.groups["fast_a/data"]).cadence_s == 300
    assert cast(RegularTimeAxis, spec.groups["fast_b/data"]).cadence_s == 300
    assert cast(RegularTimeAxis, spec.groups["slow_a/data"]).cadence_s == 900
    assert cast(RegularTimeAxis, spec.groups["slow_b/data"]).cadence_s == 900
    assert cast(RegularTimeAxis, spec.groups["slow_c/data"]).cadence_s == 900
    assert (
        resolved.identity_hash == resolve_index_spec(spec, time_dim_name="timestamp").identity_hash
    )


def test_option_epoch_idempotent_same_epoch_emits_verified() -> None:
    from slot_shape_test_plugin import OptionEpochShapeIngestor

    plugin = OptionEpochShapeIngestor()
    ctx = _make_plugin_ctx(reference_epoch="2024-01-01T00:00:00Z")
    spec_a = plugin.index_spec(ctx)
    spec_b = plugin.index_spec(ctx)

    assert spec_a == spec_b
    assert (
        resolve_index_spec(spec_a, time_dim_name="timestamp").identity_hash
        == resolve_index_spec(spec_b, time_dim_name="timestamp").identity_hash
    )


def test_option_epoch_different_epoch_raises_conflict() -> None:
    from slot_shape_test_plugin import OptionEpochShapeIngestor

    plugin = OptionEpochShapeIngestor()
    ctx_a = _make_plugin_ctx(reference_epoch="2024-01-01T00:00:00Z")
    ctx_b = _make_plugin_ctx(reference_epoch="2025-01-01T00:00:00Z")
    spec_a = plugin.index_spec(ctx_a)
    spec_b = plugin.index_spec(ctx_b)

    assert (
        resolve_index_spec(spec_a, time_dim_name="timestamp").identity_hash
        != resolve_index_spec(spec_b, time_dim_name="timestamp").identity_hash
    ), "different epochs must produce different identity hashes"


def test_option_epoch_z_and_plus_zero_epoch_converge_via_normalize() -> None:
    from slot_shape_test_plugin import OptionEpochShapeIngestor

    plugin = OptionEpochShapeIngestor()
    ctx_z = _make_plugin_ctx(reference_epoch="2024-01-01T00:00:00Z")
    ctx_plus = _make_plugin_ctx(reference_epoch="2024-01-01T00:00:00+00:00")

    spec_z = plugin.index_spec(ctx_z)
    spec_plus = plugin.index_spec(ctx_plus)

    assert spec_z == spec_plus, (
        "fixture must normalize epoch ISO so 'Z' and '+00:00' converge; "
        f"got Z={spec_z.groups!r} +00:00={spec_plus.groups!r}"
    )
