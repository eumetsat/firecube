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


def _ingest(source: Path, target: Path, *, chunk_len: int) -> None:
    result = subprocess.run(
        _firecube_command(
            "ingest",
            "precip_daily",
            "--input-data",
            str(source),
            "--target",
            target.as_uri(),
            "--product-name",
            "f4test",
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
            "--option",
            f'zarr_chunk_shape={{"time":{chunk_len}}}',
        ),
        capture_output=True,
        text=True,
        timeout=180,
        check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr


def _chunk_list(target: Path) -> list[dict[str, Any]]:
    result = subprocess.run(
        _firecube_command(
            "chunks",
            "list",
            "--product-name",
            target.as_uri(),
            "--include-span",
            "--format",
            "json",
        ),
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    payload = json.loads(result.stdout)
    assert isinstance(payload, list) and payload, payload
    return payload


def test_chunk_len_used_matches_configured_or_stored(tmp_path: Path) -> None:
    """chunk_len_used matches the stored append-axis chunk length."""
    source = tmp_path / "source"
    target = tmp_path / "store.zarr"
    _generate_days(source, 1, 10)

    _ingest(source, target, chunk_len=30)

    chunks = _chunk_list(target)
    assert {chunk.get("chunk_len_used") for chunk in chunks} == {30}
    assert {chunk["span"].get("chunk_len_used") for chunk in chunks} == {30}
