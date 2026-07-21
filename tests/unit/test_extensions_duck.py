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

import pytest

from firecube.ingestor.extensions.duck import DuckDbMixin


class _DuckSetup(DuckDbMixin):
    def __init__(self) -> None:
        super().__init__()


def _current_temp_directory_size(ingestor: _DuckSetup) -> str:
    row = ingestor.con.execute("SELECT current_setting('max_temp_directory_size')").fetchone()
    assert row is not None
    return str(row[0]).strip()


@pytest.mark.unit
def test_duckdb_max_temp_directory_size_applies() -> None:
    ingestor = _DuckSetup()
    try:
        ingestor.setup_duckdb(options={"duckdb_max_temp_directory_size": "1GB"})

        applied_value = _current_temp_directory_size(ingestor)

        assert applied_value
        assert applied_value not in {"0", "0B", "0 bytes", "0 Bytes"}
    finally:
        ingestor.teardown_duckdb()


@pytest.mark.unit
def test_duckdb_old_short_name_silently_ignored() -> None:
    baseline = _DuckSetup()
    old_key = _DuckSetup()
    try:
        baseline.setup_duckdb()
        baseline_value = _current_temp_directory_size(baseline)

        old_key.setup_duckdb(options={"duckdb_max_temp_size": "1GB"})
        old_key_value = _current_temp_directory_size(old_key)

        assert old_key_value == baseline_value
    finally:
        baseline.teardown_duckdb()
        old_key.teardown_duckdb()
