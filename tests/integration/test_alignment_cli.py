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

"""The CLI emits one alignment warning and one summary per ingest run."""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest

pytestmark = pytest.mark.integration

_REPO_ROOT = Path(__file__).resolve().parents[2]
_FIXTURE_PLUGINS = _REPO_ROOT / "tests" / "fixtures" / "firecube_test_plugins"
_SYNTHETIC_PRECIP = _REPO_ROOT / "tests" / "fixtures" / "gen_synthetic_precip_daily.py"

_WARNING_TEXT = "unaligned with chunk layout"
_SUMMARY_TEXT = "Alignment summary"


def _generate_days(out_dir: Path, start_day: int, end_day: int) -> None:
    result = subprocess.run(
        [sys.executable, str(_SYNTHETIC_PRECIP), str(out_dir), str(start_day), str(end_day)],
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr


def _firecube_command(*args: str) -> list[str]:
    return [
        "uv",
        "run",
        "--with-editable",
        str(_REPO_ROOT),
        "--with-editable",
        str(_FIXTURE_PLUGINS),
        "firecube",
        *args,
    ]


def _run_firecube(*args: str) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(
        _firecube_command(*args),
        capture_output=True,
        text=True,
        timeout=180,
        check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    return result


def _last_json_object(text: str) -> dict[str, Any]:
    manifest_start = text.rfind('{\n  "plugin"')
    assert manifest_start >= 0, text
    payload = json.loads(text[manifest_start:])
    assert isinstance(payload, dict), payload
    return payload


def _ingest(source: Path, target: Path, *, batch_size: int) -> subprocess.CompletedProcess[str]:
    return _run_firecube(
        "ingest",
        "precip_daily",
        "--input-data",
        str(source),
        "--target",
        target.as_uri(),
        "--product-name",
        "m6test",
        "--storage-type",
        "local",
        "--storage-driver",
        "fsspec",
        "--output-format",
        "zarr",
        "--write-mode",
        "direct",
        "--option",
        "layout=timeseries",
        "--option",
        f"pipeline_batch_size={batch_size}",
        "--option",
        "no_progress=true",
    )


def _alignment_lines(stderr: str) -> tuple[list[str], list[str]]:
    lines = stderr.splitlines()
    warnings = [line for line in lines if _WARNING_TEXT in line]
    summaries = [line for line in lines if _SUMMARY_TEXT in line]
    return warnings, summaries


def test_unaligned_batches_warn_once_and_summarise_once(tmp_path: Path) -> None:
    """30 days in batches of 10 against chunk 365: one warning, one summary of 2."""
    source = tmp_path / "source"
    target = tmp_path / "store.zarr"
    _generate_days(source, 1, 30)

    result = _ingest(source, target, batch_size=10)

    warnings, summaries = _alignment_lines(result.stderr)
    assert len(warnings) == 1, result.stderr
    assert len(summaries) == 1, result.stderr
    assert "2 unaligned batch(es)" in summaries[0]
    assert "'default'" in summaries[0]
    assert "chunk=365" in summaries[0]
    manifest = _last_json_object(result.stdout)
    assert manifest["metrics"]["zarr"]["unaligned_batches"] == 2


def test_single_batch_run_logs_no_alignment_lines(tmp_path: Path) -> None:
    """One batch of 30 against chunk 365 is only the final tail: nothing is logged."""
    source = tmp_path / "source"
    target = tmp_path / "store.zarr"
    _generate_days(source, 1, 30)

    result = _ingest(source, target, batch_size=365)

    warnings, summaries = _alignment_lines(result.stderr)
    assert warnings == [], result.stderr
    assert summaries == [], result.stderr
    manifest = _last_json_object(result.stdout)
    assert manifest["metrics"]["zarr"]["unaligned_batches"] == 0
