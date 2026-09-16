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

import inspect
import json
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest

from firecube.ingestor.runtime import recording

pytestmark = pytest.mark.integration

_REPO_ROOT = Path(__file__).resolve().parents[2]
_FIXTURE_PLUGINS = _REPO_ROOT / "tests" / "fixtures" / "firecube_test_plugins"
_SYNTHETIC_PRECIP = _REPO_ROOT / "tests" / "fixtures" / "gen_synthetic_precip_daily.py"


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
    if manifest_start >= 0:
        payload = json.loads(text[manifest_start:])
        assert isinstance(payload, dict), payload
        return payload

    decoder = json.JSONDecoder()
    parsed: dict[str, Any] | None = None
    for index, char in enumerate(text):
        if char != "{":
            continue
        try:
            candidate, _ = decoder.raw_decode(text[index:])
        except json.JSONDecodeError:
            continue
        if isinstance(candidate, dict):
            parsed = candidate
    assert parsed is not None, text
    return parsed


def _ingest(source: Path, target: Path, *, resume_existing: bool = False) -> dict[str, Any]:
    args = [
        "ingest",
        "precip_daily",
        "--input-data",
        str(source),
        "--target",
        target.as_uri(),
        "--product-name",
        "m4test",
        "--storage-type",
        "local",
        "--storage-driver",
        "fsspec",
        "--output-format",
        "zarr",
        "--write-mode",
        "direct",
        "--option",
        "no_progress=true",
    ]
    if resume_existing:
        args.extend(["--option", "resume_existing=true"])
    return _last_json_object(_run_firecube(*args).stdout)


def _runs_list(target: Path) -> list[dict[str, Any]]:
    result = _run_firecube(
        "chunks",
        "runs",
        "list",
        "--product-name",
        target.as_uri(),
        "-f",
        "json",
    )
    payload = json.loads(result.stdout)
    assert isinstance(payload, list), payload
    return payload


def test_timestamps_skipped_in_runs_list(tmp_path: Path) -> None:
    """resume_existing run logs skipped count in runs list."""
    source = tmp_path / "source"
    target = tmp_path / "store.zarr"
    _generate_days(source, 1, 30)

    _ingest(source, target)
    _ingest(source, target, resume_existing=True)

    assert max(run.get("timestamps_skipped", 0) for run in _runs_list(target)) == 30


def test_timestamps_skipped_in_manifest(tmp_path: Path) -> None:
    """manifest has non-zero timestamps_skipped after resume."""
    source = tmp_path / "source"
    target = tmp_path / "store.zarr"
    _generate_days(source, 1, 30)

    _ingest(source, target)
    manifest = _ingest(source, target, resume_existing=True)

    assert manifest["metrics"]["pipeline"]["timestamps_skipped"] == 30


def test_timestamps_skipped_zero_when_no_skips(tmp_path: Path) -> None:
    """Verified-correct: default 0 when nothing skipped."""
    source = tmp_path / "source"
    target = tmp_path / "store.zarr"
    _generate_days(source, 1, 5)

    manifest = _ingest(source, target)
    runs = _runs_list(target)

    assert manifest["metrics"]["pipeline"]["timestamps_skipped"] == 0
    assert [run.get("timestamps_skipped") for run in runs] == [0]


def test_extract_no_bare_except() -> None:
    """grep asserts `except Exception` removed from _extract_timestamps_skipped."""
    source = inspect.getsource(recording._extract_timestamps_skipped)

    assert "except Exception" not in source
