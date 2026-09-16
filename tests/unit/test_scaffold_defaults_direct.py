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

import pytest

from firecube.ingestor.devtools.scaffolding import create_plugin_structure, to_snake_case

pytestmark = pytest.mark.unit


def _render_readme(tmp_path: Path, name: str, template_type: str) -> str:
    create_plugin_structure(name, tmp_path, template_type=template_type)
    slug = to_snake_case(name).replace("_", "-")
    return (tmp_path / f"firecube-{slug}" / "README.md").read_text(encoding="utf-8")


def _assert_local_example_uses_direct(readme: str) -> None:
    assert "--write-mode direct\n```" in readme


@pytest.mark.parametrize(
    ("name", "template_type"),
    [
        ("zarr-m3", "zarr"),
        ("parquet-m3", "parquet"),
        ("base-m3", "base"),
        ("direct-zarr-m3", "direct_zarr"),
    ],
)
def test_template_readme_uses_direct(tmp_path: Path, name: str, template_type: str) -> None:
    readme = _render_readme(tmp_path, name, template_type)
    _assert_local_example_uses_direct(readme)
