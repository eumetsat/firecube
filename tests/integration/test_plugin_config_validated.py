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

import subprocess
import sys
from pathlib import Path

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


def _ingest(
    plugin: str,
    source: Path,
    target: Path,
    *extra: str,
    product_name: str = "test",
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [
            "uv",
            "run",
            "--with-editable",
            str(_REPO_ROOT),
            "--with-editable",
            str(_FIXTURE_PLUGINS),
            "firecube",
            "ingest",
            plugin,
            "--input-data",
            str(source),
            "--target",
            target.as_uri(),
            "--product-name",
            product_name,
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
            *extra,
        ],
        capture_output=True,
        text=True,
        timeout=180,
        check=False,
    )


def _assert_configuration_error_before_store(
    result: subprocess.CompletedProcess[str], target: Path
) -> None:
    combined = result.stdout + result.stderr
    assert result.returncode == 1, combined
    assert "Error:" in combined
    assert "zarr_shard_shape" in combined
    assert not target.exists(), "invalid config must fail before Zarr/.firecube store creation"


def test_plugin_get_zarr_config_invalid_sharding_raises_before_store(tmp_path: Path) -> None:
    """invalid plugin sharding config raises ConfigurationError before store creation."""
    source = tmp_path / "source"
    target = tmp_path / "target.zarr"
    _generate_days(source, 1, 6)

    result = _ingest("test_giraffe_override", source, target)

    _assert_configuration_error_before_store(result, target)


def test_plugin_get_zarr_config_valid_still_works(tmp_path: Path) -> None:
    """Verified-correct: legit plugin overrides still function."""
    source = tmp_path / "source"
    target = tmp_path / "target.zarr"
    _generate_days(source, 1, 2)

    result = _ingest(
        "precip_daily",
        source,
        target,
        "--option",
        "layout=areastats",
        product_name="precip_daily",
    )

    assert result.returncode == 0, result.stdout + result.stderr
    assert (target / ".firecube").exists()
    assert (target / "default").exists()


def test_option_derived_config_still_validated(tmp_path: Path) -> None:
    """Verified-correct: --option path validation is preserved."""
    source = tmp_path / "source"
    target = tmp_path / "target.zarr"
    _generate_days(source, 1, 2)

    result = _ingest(
        "precip_daily",
        source,
        target,
        "--option",
        "zarr_sharding=true",
        "--option",
        'zarr_chunk_shape={"time":365}',
        product_name="precip_daily",
    )

    _assert_configuration_error_before_store(result, target)
