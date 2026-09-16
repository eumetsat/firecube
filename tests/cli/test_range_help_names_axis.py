# Copyright 2025-2026 EUMETSAT
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import annotations

from pathlib import Path

from click.testing import CliRunner

from firecube.cli.main import cli
from firecube.core.controlplane import ChunkManager, SpanCoverage
from tests.helpers.storage import make_test_binding


def test_delete_range_help_names_record_time_axis() -> None:
    result = CliRunner().invoke(cli, ["chunks", "delete", "--help"])

    assert result.exit_code == 0, result.output
    assert "delete chunks in a record-time range" in result.output
    assert "YYYY-MM-DD,YYYY-MM-DD" in result.output
    assert "use --time-range for data time" in result.output


def test_delete_epilog_includes_time_range_example() -> None:
    result = CliRunner().invoke(cli, ["chunks", "delete", "--help"])

    assert result.exit_code == 0, result.output
    assert "--time-range 2024-01-01:2024-03-31" in result.output


def _seed_product_with_april_2024_span(tmp_path: Path) -> str:
    """Record one complete run with a span covering 2024-04-01..02; return its URI."""
    product_uri = f"file://{tmp_path / 'PRODUCT_A.zarr'}"
    manager = ChunkManager(
        binding=make_test_binding(tmp_path, product="PRODUCT_A.zarr"),
        workspace=tmp_path,
    )
    try:
        manager.record_run_started(
            product="PRODUCT_A.zarr",
            run_id="run-001",
            output_path=product_uri,
            output_format="zarr",
            size=0,
            meta={"plugin": "test"},
        )
        manager.record_span(
            product="PRODUCT_A.zarr",
            run_id="run-001",
            batch_id="batch-001",
            group="F024",
            status="active",
            coverage=SpanCoverage(
                group="F024",
                arrays=["F024/FWI"],
                time_index_ranges=[[0, 1]],
                time_min="2024-04-01T00:00:00",
                time_max="2024-04-02T00:00:00",
            ),
            meta={
                "plugin": "test",
                "group": "F024",
                "time_min": "2024-04-01T00:00:00",
                "time_max": "2024-04-02T00:00:00",
            },
        )
        manager.record_run_terminal(
            product="PRODUCT_A.zarr",
            run_id="run-001",
            output_path=product_uri,
            output_format="zarr",
            size=1,
            meta={"plugin": "test"},
            status="complete",
        )
    finally:
        manager.close()
    return product_uri


def _invoke_no_match_delete(tmp_path: Path, product_uri: str, *time_filter: str) -> str:
    result = CliRunner().invoke(
        cli,
        [
            "chunks",
            "delete",
            "--product-name",
            product_uri,
            "--workspace",
            str(tmp_path),
            *time_filter,
            "--manifest-only",
            "--dry-run",
        ],
    )
    assert result.exit_code == 0, result.output
    return result.output


def test_delete_no_match_message_names_record_and_data_time_axes(tmp_path: Path) -> None:
    product_uri = _seed_product_with_april_2024_span(tmp_path)

    output = _invoke_no_match_delete(tmp_path, product_uri, "--range", "2000-01-01,2000-01-02")

    assert (
        "No chunks found matching the given criteria. Note: --range filters by "
        "record time; use --time-range for data time."
    ) in output


def test_delete_no_match_with_time_range_names_data_time_axis(tmp_path: Path) -> None:
    """A `--time-range` miss is explained in data-time terms, not by a `--range` note."""
    product_uri = _seed_product_with_april_2024_span(tmp_path)

    output = _invoke_no_match_delete(tmp_path, product_uri, "--time-range", "2000-01-01:2000-01-02")

    assert "No chunks found matching the given criteria." in output
    assert "--time-range filters by data time" in output
    assert "for record time" in output
    assert "--range filters by record time" not in output


def test_delete_no_match_with_start_date_names_the_option_passed(tmp_path: Path) -> None:
    product_uri = _seed_product_with_april_2024_span(tmp_path)

    output = _invoke_no_match_delete(tmp_path, product_uri, "--start-date", "2030-01-01")

    assert "--start-date filters by record time; use --time-range for data time" in output
    assert "--range filters" not in output


def test_delete_no_match_without_time_filter_has_no_axis_note(tmp_path: Path) -> None:
    product_uri = _seed_product_with_april_2024_span(tmp_path)

    output = _invoke_no_match_delete(tmp_path, product_uri, "--pattern", "no-such-key-*")

    assert "No chunks found matching the given criteria." in output
    assert "Note:" not in output
