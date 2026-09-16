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

import os
import subprocess
from pathlib import Path

import pytest

pytestmark = pytest.mark.integration


def _base_env() -> dict[str, str]:
    env = os.environ.copy()
    env.setdefault("PYTHONUNBUFFERED", "1")
    return env


def _junk_source(path: Path) -> Path:
    path.mkdir()
    (path / "notes.txt").write_text("notes\n", encoding="utf-8")
    (path / "table.csv").write_text("a,b\n1,2\n", encoding="utf-8")
    return path


def _precip_cmd(source: Path, target: Path, *, options: list[str] | None = None) -> list[str]:
    cmd = [
        "uv",
        "run",
        "firecube",
        "ingest",
        "precip_daily",
        "--input-data",
        str(source),
        "--target",
        target.as_uri(),
        "--product-name",
        "w14_zero_input",
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
    for option in options or []:
        cmd.extend(["--option", option])
    return cmd


def _run(cmd: list[str], *, env: dict[str, str] | None = None) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        cmd,
        text=True,
        capture_output=True,
        timeout=120,
        check=False,
        env=env,
    )


def _assert_no_firecube(target: Path) -> None:
    assert not (target / ".firecube").exists(), (
        "zero-input guard must fire before register_run_started creates control-plane state"
    )


def test_zero_input_no_explicit_patterns_raises(tmp_path: Path) -> None:
    source = _junk_source(tmp_path / "input")
    target = tmp_path / "target.zarr"

    result = _run(_precip_cmd(source, target))

    assert result.returncode != 0, result.stdout + result.stderr
    _assert_no_firecube(target)


def test_zero_input_with_explicit_patterns_raises_before_firecube(tmp_path: Path) -> None:
    source = _junk_source(tmp_path / "input")
    target = tmp_path / "target.zarr"

    result = _run([*_precip_cmd(source, target), "--input-filters", '["*.missing"]'])

    assert result.returncode != 0, result.stdout + result.stderr
    _assert_no_firecube(target)


def test_zero_input_message_names_input_filters_and_suffixes(tmp_path: Path) -> None:
    source = _junk_source(tmp_path / "input")
    target = tmp_path / "target.zarr"

    result = _run(_precip_cmd(source, target))
    combined = result.stdout + result.stderr

    assert result.returncode != 0, combined
    assert "--input-filters" in combined
    for suffix in (".zip", ".h5", ".nc", ".nc4", ".hdf", ".he5"):
        assert suffix in combined
    assert "allow_empty_source=true" in combined


def test_allow_empty_source_still_exits_zero_with_warning(tmp_path: Path) -> None:
    source = _junk_source(tmp_path / "input")
    target = tmp_path / "target.zarr"

    result = _run(_precip_cmd(source, target, options=["allow_empty_source=true"]))
    combined = result.stdout + result.stderr

    assert result.returncode == 0, combined
    assert "No input files found" in combined


def _dynamic_cmd(plugin: str, source: Path, target: Path) -> list[str]:
    return [
        "uv",
        "run",
        "firecube",
        "ingest",
        plugin,
        "--input-data",
        str(source),
        "--target",
        target.as_uri(),
        "--product-name",
        "w14_single_discovery",
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


def test_plugin_override_exempted(tmp_path: Path) -> None:
    source = _junk_source(tmp_path / "input")
    target = tmp_path / "target.zarr"
    counter = tmp_path / "counter.txt"
    env = _base_env() | {"W14_DISCOVERY_COUNTER": str(counter)}

    result = _run(_dynamic_cmd("w14_override_empty", source, target), env=env)

    assert result.returncode == 0, result.stdout + result.stderr
    assert counter.read_text(encoding="utf-8") == "1"


def test_discover_source_files_called_exactly_once_per_ingest(tmp_path: Path) -> None:
    source = _junk_source(tmp_path / "input")
    target = tmp_path / "target.zarr"
    counter = tmp_path / "counter.txt"
    env = _base_env() | {"W14_DISCOVERY_COUNTER": str(counter)}

    result = _run(_dynamic_cmd("w14_counter_success", source, target), env=env)

    assert result.returncode == 0, result.stdout + result.stderr
    assert counter.read_text(encoding="utf-8") == "1"


def test_lazy_discovery_iterator_not_re_listed(tmp_path: Path) -> None:
    source = _junk_source(tmp_path / "input")
    target = tmp_path / "target.zarr"
    counter = tmp_path / "counter.txt"
    env = _base_env() | {"W14_DISCOVERY_COUNTER": str(counter)}

    result = _run(_dynamic_cmd("w14_lazy_success", source, target), env=env)

    assert result.returncode == 0, result.stdout + result.stderr
    assert counter.read_text(encoding="utf-8") == "1"
