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

from firecube.ingestor.runtime.parallel_evidence import log_filter_evidence, log_schema_evidence


@pytest.mark.unit
def test_log_filter_evidence_emits_correct_format(caplog):
    logger = logging.getLogger("firecube.test.parallel_evidence")

    with caplog.at_level(logging.INFO, logger=logger.name):
        log_filter_evidence(
            logger,
            stage="pre_batch_filter",
            planned_range=(0, 100),
            original_count=200,
            filtered_count=100,
            dropped_count=100,
        )

    assert any("stage=pre_batch_filter" in rec.message for rec in caplog.records)
    assert any("planned_range=[0,100)" in rec.message for rec in caplog.records)
    assert any("original_count=200" in rec.message for rec in caplog.records)
    assert any("filtered_count=100" in rec.message for rec in caplog.records)
    assert any("dropped_count=100" in rec.message for rec in caplog.records)


@pytest.mark.unit
def test_log_filter_evidence_zero_drops_still_emits(caplog):
    logger = logging.getLogger("firecube.test.parallel_evidence.zero_drops")

    with caplog.at_level(logging.INFO, logger=logger.name):
        log_filter_evidence(
            logger,
            stage="pre_batch_filter",
            planned_range=(10, 20),
            original_count=5,
            filtered_count=5,
            dropped_count=0,
        )

    assert any("dropped_count=0" in rec.message for rec in caplog.records)


@pytest.mark.unit
def test_log_schema_evidence_emits_correct_format(caplog):
    logger = logging.getLogger("firecube.test.parallel_evidence.schema")

    with caplog.at_level(logging.INFO, logger=logger.name):
        log_schema_evidence(
            logger,
            stage="schema_verify",
            group="data",
            existing_shape=(1000, 10),
            expected_shape=1000,
            status="verified",
        )

    assert any("stage=schema_verify" in rec.message for rec in caplog.records)
    assert any("status=verified" in rec.message for rec in caplog.records)


@pytest.mark.unit
def test_log_filter_evidence_at_info_level(caplog):
    logger = logging.getLogger("firecube.test.parallel_evidence.level")

    with caplog.at_level(logging.INFO, logger=logger.name):
        log_filter_evidence(
            logger,
            stage="pre_batch_filter",
            planned_range=(1, 2),
            original_count=1,
            filtered_count=1,
            dropped_count=0,
        )

    assert caplog.records
    assert caplog.records[0].levelno == logging.INFO
