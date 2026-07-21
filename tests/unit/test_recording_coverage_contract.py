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

from __future__ import annotations

import logging

import pytest

from firecube.ingestor.runtime.recording import _span_coverage_from_metrics
from firecube.ingestor.types.result_metrics import ResultMetrics, _coerce_result_metrics


@pytest.mark.unit
def test_coverage_extractor_warns_when_missing(caplog):
    logger = logging.getLogger("firecube.test.recording")
    with caplog.at_level(logging.DEBUG, logger=logger.name):
        result = _span_coverage_from_metrics({"zarr": {}}, logger=logger, context="batch b1")

    assert result is None
    assert any("No span coverage present in metrics" in rec.message for rec in caplog.records)


@pytest.mark.unit
def test_coverage_extractor_supports_legacy_zarr_path(caplog):
    logger = logging.getLogger("firecube.test.recording")
    metrics = {"zarr": {"coverage": [{"group": "F120", "arrays": [], "time_index_ranges": []}]}}

    with caplog.at_level(logging.DEBUG, logger=logger.name):
        result = _span_coverage_from_metrics(metrics, logger=logger, context="batch b2")

    assert result is not None
    assert result[0].group == "F120"


@pytest.mark.unit
def test_coverage_extractor_reads_run_level_zarr_coverage_from_typed_metrics():
    """Run-level ``ResultMetrics`` carry coverage under ``zarr.coverage``.

    ``merge_batch_metrics`` aggregates per-batch coverage into
    ``metrics["zarr"]["coverage"]`` while ``finalize`` sets ``pipeline`` to the
    run summary (which has no coverage). The extractor reads typed metrics, so
    it must fall back to the nested ``zarr`` location instead of trusting the
    empty ``pipeline.coverage`` — otherwise run registration sees no coverage.
    """
    metrics = _coerce_result_metrics(
        {
            "zarr": {"coverage": [{"group": "F120", "arrays": [], "time_index_ranges": [[0, 1]]}]},
            "pipeline": {"duration_total_s": 1.0},
        }
    )

    # Precondition: this is exactly the shape that exposed the bug.
    assert isinstance(metrics, ResultMetrics)
    assert metrics.pipeline is not None
    assert metrics.pipeline.coverage == []

    result = _span_coverage_from_metrics(metrics)
    assert result is not None
    assert result[0].group == "F120"
