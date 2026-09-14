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

"""Shared helpers for staged-ingest integration tests.

Imported explicitly (this is NOT a conftest); each helper avoids re-implementing
the same subprocess wiring, S3 env plumbing, or manifest-parsing boilerplate.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any

_REPO_ROOT = Path(__file__).resolve().parents[2]
SYNTHETIC_PRECIP_SCRIPT = _REPO_ROOT / "tests" / "fixtures" / "gen_synthetic_precip_daily.py"


def generate_days(out_dir: Path, start_day: int, end_day: int) -> None:
    """Materialise synthetic daily precipitation files under ``out_dir``."""
    result = subprocess.run(
        [sys.executable, str(SYNTHETIC_PRECIP_SCRIPT), str(out_dir), str(start_day), str(end_day)],
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr


def firecube_command(*args: str) -> list[str]:
    """Build a ``firecube`` invocation via ``uv run``.

    Assumes the CLI and required fixture plugins are already installed in the
    test environment (see ``tests/conftest.py::pytest_sessionstart`` and
    ``AGENTS.md`` for setup steps). No per-invocation ``--with-editable`` is
    added — that would re-resolve editable installs on every test call.
    """
    return ["uv", "run", "firecube", *args]


def last_manifest(text: str) -> dict[str, Any]:
    """Extract the final ingestion manifest JSON emitted on stdout."""
    manifest_start = text.rfind('{\n  "plugin"')
    assert manifest_start >= 0, text
    payload = json.loads(text[manifest_start:])
    assert isinstance(payload, dict), payload
    return payload


def s3_env(
    endpoint: str,
    *,
    access_key: str,
    secret_key: str,
    region: str,
) -> dict[str, str]:
    """Compose the AWS + FIRECUBE env vars required by staged S3 ingests."""
    return {
        **os.environ,
        "AWS_ACCESS_KEY_ID": access_key,
        "AWS_SECRET_ACCESS_KEY": secret_key,
        "AWS_DEFAULT_REGION": region,
        "AWS_EC2_METADATA_DISABLED": "true",
        "AWS_S3_ADDRESSING_STYLE": "path",
        "FIRECUBE_ENDPOINT_URL": endpoint,
        "FIRECUBE_ACCESS_KEY": access_key,
        "FIRECUBE_SECRET_KEY": secret_key,
        "FIRECUBE_REGION": region,
        "FIRECUBE_PATH_STYLE": "true",
    }


def run_staged_ingest(
    plugin: str,
    source: Path,
    target_uri: str,
    *,
    product_name: str,
    storage_type: str,
    env: dict[str, str] | None = None,
    resume: bool = False,
    batch_size: int = 5,
    extra_options: dict[str, str] | None = None,
    timeout: int = 300,
) -> dict[str, Any]:
    """Run a staged-mode ingest and return the last-emitted manifest."""
    args: list[str] = [
        "ingest",
        plugin,
        "--input-data",
        str(source),
        "--target",
        target_uri,
        "--product-name",
        product_name,
        "--storage-type",
        storage_type,
        "--storage-driver",
        "fsspec",
        "--output-format",
        "zarr",
        "--write-mode",
        "staged",
        "--option",
        "no_progress=true",
        "--option",
        f"pipeline_batch_size={batch_size}",
    ]
    if resume:
        args.extend(["--option", "resume_existing=true"])
    for key, value in (extra_options or {}).items():
        args.extend(["--option", f"{key}={value}"])
    result = subprocess.run(
        firecube_command(*args),
        capture_output=True,
        text=True,
        timeout=timeout,
        check=False,
        env=env,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    return last_manifest(result.stdout)
