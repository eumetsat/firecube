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

"""Integration regression: cf_time_dim plugin telemetry must report real dates.

The ``cf_time_dim_test_plugin`` fixture writes a CF-encoded time coord
``times = np.arange(n, dtype=np.float64)`` with
``attrs={"units": "days since 2000-01-01", ...}``. Before
``AppendCoverageBuilder.record_batch`` learned to decode CF-encoded
numeric time arrays, the coverage ``time_min``/``time_max`` reported
``1970-01-01`` because raw float days were fed to ``pd.Timestamp``
(implicit ``ns`` epoch interpretation). These tests lock the
post-fix behaviour: any regression that re-introduces the bug must turn
both tests red.

Two layers of coverage:

1. ``test_float64_cf_time_coverage_builder_reports_year_2000`` exercises
   ``AppendCoverageBuilder.record_batch`` directly with the same dtype
   and attrs the fixture emits. Fastest signal on the decode path.
2. ``test_cf_time_dim_plugin_run_summary_reports_year_2000`` runs the
   real plugin end-to-end via ``ingestor.run(ctx)`` and inspects the
   typed ``ResultMetrics.pipeline.coverage`` produced by the engine.
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pytest
import xarray as xr

from firecube.core.controlplane import SpanCoverage
from firecube.ingestor.runtime.zarr.append_services import AppendCoverageBuilder
from tests.helpers.storage import make_test_context

pytestmark = pytest.mark.integration


def test_float64_cf_time_coverage_builder_reports_year_2000() -> None:
    """Float64+CF-units time coord must decode to year-2000 coverage bounds.

    Mirrors exactly the coord layout produced by ``cf_time_dim_test_plugin``:
    ``np.arange(n, dtype=np.float64)`` values with
    ``units="days since 2000-01-01"``. If the decode path regresses, the
    builder would store ``pd.Timestamp(0.0)`` and ``pd.Timestamp(9.0)``
    which both render as 1970-01-01 in ISO form.
    """
    builder = AppendCoverageBuilder(time_dim_name="time")
    n = 10
    times = np.arange(n, dtype=np.float64)
    ds = xr.Dataset(
        coords={
            "time": xr.DataArray(
                times,
                dims=["time"],
                attrs={
                    "units": "days since 2000-01-01",
                    "calendar": "standard",
                    "standard_name": "time",
                    "axis": "T",
                },
            ),
        },
    )

    builder.record_batch(start_i=0, count=n, ds=ds, aligned=True)
    entry = builder.build_entry(
        group="default",
        coverage_arrays=["default/temperature"],
        state_var_name="firecube_timestamp_state",
        state_deleted_value=2,
    )

    assert entry is not None, "Coverage builder produced no entry for a non-empty batch."

    time_min_str = str(entry["time_min"])
    time_max_str = str(entry["time_max"])

    assert "1970" not in time_min_str, (
        f"coverage time_min={time_min_str!r} starts in 1970 — CF decode path "
        "regressed: float64+units array was interpreted as ns-since-epoch."
    )
    assert "1970" not in time_max_str, (
        f"coverage time_max={time_max_str!r} starts in 1970 — CF decode path "
        "regressed: float64+units array was interpreted as ns-since-epoch."
    )
    assert time_min_str.startswith("2000-01-01"), (
        f"Expected time_min to start at 2000-01-01 (day 0 of CF epoch), got {time_min_str!r}."
    )
    assert time_max_str.startswith(f"2000-01-{n:02d}"), (
        f"Expected time_max to land on 2000-01-{n:02d} (day {n - 1} of CF epoch), "
        f"got {time_max_str!r}."
    )


def test_cf_time_dim_plugin_run_summary_reports_year_2000(tmp_path: Path) -> None:
    """End-to-end: cf_time_dim plugin run-summary coverage must be year 2000.

    Drives the real fixture plugin through ``ingestor.run(ctx)`` and asserts
    the aggregated ``metrics["zarr"]["coverage"]`` carried into the run
    summary reports year-2000 ``time_min``/``time_max``. Catches any
    regression in the engine pathway that aggregates per-batch coverage
    into the final ``ResultMetrics`` (not just the in-process builder).
    The aggregator (``runtime/aggregation.py:merge_batch_metrics``) merges
    all batch coverage under ``metrics["zarr"]["coverage"]`` — the same
    key path ``runtime/recording.py:_span_coverage_from_metrics`` reads
    when promoting batches to span records.
    """
    from cf_time_dim_test_plugin import CFTimeDimIngestor

    source_dir = tmp_path / "source"
    source_dir.mkdir(exist_ok=True)
    (source_dir / "dummy.nc").touch()

    ctx = make_test_context(
        tmp_path,
        source=str(source_dir),
        product="cf_time_dim_telemetry.zarr",
        options={
            "write_mode": "direct",
            "pipeline_batch_size": 1,
            "pipeline_parallel": False,
            "pipeline_workers": 1,
            "no_progress": True,
            "cleanup_workspace": True,
        },
    )
    ingestor = CFTimeDimIngestor()
    result = ingestor.run(ctx)

    assert result.output_path, f"Plugin run produced no output_path: {result!r}"

    metrics_dict = result.metrics.to_dict()
    zarr_metrics = metrics_dict.get("zarr") or {}
    coverage = zarr_metrics.get("coverage") or []

    assert coverage, (
        "metrics['zarr']['coverage'] is empty — engine did not record any span "
        "coverage for the cf_time_dim ingest run."
    )

    for span in coverage:
        if isinstance(span, SpanCoverage):
            time_min = str(span.time_min) if span.time_min is not None else ""
            time_max = str(span.time_max) if span.time_max is not None else ""
            group = span.group
        else:
            time_min = str(span.get("time_min") or "")
            time_max = str(span.get("time_max") or "")
            group = span.get("group", "<unknown>")

        assert time_min, (
            f"Span {group!r} has no time_min — telemetry would surface as 'null' "
            "instead of a real CF-decoded timestamp."
        )
        assert "1970" not in time_min, (
            f"Run summary span {group!r} time_min={time_min!r} reports 1970 — "
            "CF-decode regression in the engine coverage path."
        )
        assert "1970" not in time_max, (
            f"Run summary span {group!r} time_max={time_max!r} reports 1970 — "
            "CF-decode regression in the engine coverage path."
        )
        assert "2000" in time_min, (
            f"Run summary span {group!r} time_min={time_min!r} should land in "
            "year 2000 (CF epoch declared by the fixture)."
        )
