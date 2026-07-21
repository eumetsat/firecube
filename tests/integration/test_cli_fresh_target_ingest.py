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

"""T4.3 — CLI fresh-target smoke test for the storage-flat-uri regression (fixed in T1.2)."""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
RUNNER = REPO_ROOT / "tests" / "fixtures" / "test_stub_ingestor" / "run_firecube.py"
STUB_PLUGIN_NAME = "fc_test_stub"


def _run_ingest(
    target_uri: str, source_dir: Path, *, extra_env: dict[str, str] | None = None
) -> subprocess.CompletedProcess[str]:
    env = os.environ.copy()
    env["VIRTUAL_ENV"] = sys.prefix
    if extra_env:
        env.update(extra_env)

    cmd = [
        "uv",
        "run",
        "python",
        str(RUNNER),
        "ingest",
        STUB_PLUGIN_NAME,
        "--input-data",
        str(source_dir),
        "--target",
        target_uri,
        "--output-format",
        "zarr",
        "--write-mode",
        "direct",
        "--storage-type",
        "local",
        "--storage-driver",
        "fsspec",
        "--option",
        "no_progress=true",
    ]

    return subprocess.run(cmd, capture_output=True, text=True, env=env, cwd=str(REPO_ROOT))


@pytest.mark.integration
def test_fresh_local_target(tmp_path: Path) -> None:
    target_dir = tmp_path / "fresh.zarr"
    target_uri = f"file://{target_dir}"
    source_dir = tmp_path / "source"
    source_dir.mkdir()

    proc = _run_ingest(target_uri, source_dir)

    assert proc.returncode == 0, (
        f"Ingest failed (exit={proc.returncode})\n"
        f"--- stdout ---\n{proc.stdout}\n--- stderr ---\n{proc.stderr}"
    )
    assert "FileNotFoundError" not in proc.stderr, (
        f"Original regression detected in stderr:\n{proc.stderr}"
    )
    assert target_dir.exists(), f"Target dir not created at {target_dir}"

    zarr_metas = list(target_dir.rglob("zarr.json"))
    assert zarr_metas, (
        f"No zarr.json found under {target_dir} (zarr v3 metadata missing). "
        f"Tree: {sorted(p.relative_to(target_dir) for p in target_dir.rglob('*'))}"
    )

    control_root = target_dir / ".firecube"
    assert control_root.is_dir(), f".firecube/ control plane not created at {control_root}"
    assert (control_root / "schema.json").exists(), "Control plane schema.json missing"

    runs_dir = control_root / "runs"
    assert runs_dir.is_dir(), f"WAL runs directory missing at {runs_dir}"
    wal_files = list(runs_dir.rglob("events-*.jsonl"))
    assert wal_files, (
        f"No events-*.jsonl WAL file found under {runs_dir}. "
        f"Run dirs: {[p.name for p in runs_dir.iterdir()]}"
    )

    default_group = target_dir / "default"
    assert default_group.exists(), (
        "Direct write mode must produce output under target_dir "
        f"(staged would route via temp workspace); {default_group} missing"
    )


@pytest.mark.integration
def test_fresh_local_target_obstore_parity(tmp_path: Path) -> None:
    target_dir = tmp_path / "fresh-obstore.zarr"
    target_uri = f"file://{target_dir}"
    source_dir = tmp_path / "source"
    source_dir.mkdir()

    env = os.environ.copy()
    env["VIRTUAL_ENV"] = sys.prefix

    cmd = [
        "uv",
        "run",
        "python",
        str(RUNNER),
        "ingest",
        STUB_PLUGIN_NAME,
        "--input-data",
        str(source_dir),
        "--target",
        target_uri,
        "--output-format",
        "zarr",
        "--write-mode",
        "direct",
        "--storage-type",
        "local",
        "--storage-driver",
        "obstore",
        "--option",
        "no_progress=true",
    ]

    proc = subprocess.run(cmd, capture_output=True, text=True, env=env, cwd=str(REPO_ROOT))

    assert proc.returncode == 0, (
        f"Obstore parity ingest failed (exit={proc.returncode})\n"
        f"--- stdout ---\n{proc.stdout}\n--- stderr ---\n{proc.stderr}"
    )
    assert "FileNotFoundError" not in proc.stderr, proc.stderr
    assert any(target_dir.rglob("zarr.json")), "obstore driver: no zarr.json produced"
    assert (target_dir / ".firecube").is_dir(), "obstore driver: .firecube/ missing"
