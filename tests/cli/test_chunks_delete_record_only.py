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

"""`chunks delete` on span records reports that their data is not deleted."""

from __future__ import annotations

from pathlib import Path

import pytest
from click.testing import CliRunner

from firecube.cli.main import cli
from firecube.core.controlplane import ChunkManager, SpanCoverage
from tests.helpers.storage import make_test_binding

PRODUCT = "PRODUCT_A.zarr"


def _seed_product_with_span(tmp_path: Path) -> str:
    """Record one complete run with one span on 2024-04-01; return the product URI."""
    product_uri = f"file://{tmp_path / PRODUCT}"
    manager = ChunkManager(binding=make_test_binding(tmp_path, product=PRODUCT), workspace=tmp_path)
    try:
        run = {
            "product": PRODUCT,
            "run_id": "run-001",
            "output_path": product_uri,
            "output_format": "zarr",
            "meta": {"plugin": "test"},
        }
        manager.record_run_started(**run, size=0)
        manager.record_span(
            product=PRODUCT,
            run_id="run-001",
            batch_id="batch-001",
            group="F024",
            status="active",
            coverage=SpanCoverage(
                group="F024",
                arrays=["F024/FWI"],
                time_index_ranges=[[0, 0]],
                time_min="2024-04-01T00:00:00",
                time_max="2024-04-01T00:00:00",
            ),
            meta={
                "plugin": "test",
                "group": "F024",
                "time_min": "2024-04-01T00:00:00",
                "time_max": "2024-04-01T00:00:00",
            },
        )
        manager.record_run_terminal(**run, size=1, status="complete")
    finally:
        manager.close()
    return product_uri


@pytest.mark.parametrize("mode", ["--dry-run", "--yes-i-really-mean-it"])
def test_delete_reports_span_records_without_storage(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mode: str
) -> None:
    monkeypatch.setenv("FIRECUBE_STORAGE_TYPE", "local")
    monkeypatch.setenv("FIRECUBE_STORAGE_DRIVER", "fsspec")
    product_uri = _seed_product_with_span(tmp_path)

    result = CliRunner().invoke(
        cli,
        [
            "chunks",
            "delete",
            "--product-name",
            product_uri,
            "--workspace",
            str(tmp_path),
            "--time-range",
            "2024-04-01:2024-04-01",
            mode,
        ],
    )

    assert result.exit_code == 0, result.output
    assert "Storage errors" not in result.output
    assert "Records without storage objects: 1" in result.output
    assert "firecube chunks delete-span" in result.output
