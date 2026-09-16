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

"""Tests that scaffold generates plugin headers using --author and --license."""

from __future__ import annotations

import datetime

import pytest

from firecube.ingestor.devtools.scaffolding import create_plugin_structure


@pytest.mark.parametrize("template_type", ["zarr", "parquet", "base", "direct_zarr"])
def test_scaffold_header_includes_author(
    tmp_path: pytest.TempPathFactory, template_type: str
) -> None:
    author = "Test Author"
    license_id = "Apache-2.0"

    project_root = create_plugin_structure(
        "my_plugin",
        tmp_path,  # type: ignore[arg-type]  # pytest tmp_path is Path at runtime
        author_name=author,
        author_email="test@example.com",
        license=license_id,
        template_type=template_type,
    )

    ingestor_file = project_root / "src" / "firecube_my_plugin" / "ingestor.py"
    content = ingestor_file.read_text()

    assert author in content
    assert license_id in content
    assert "SPDX-License-Identifier:" in content
    assert str(datetime.date.today().year) in content
    assert "EUMETSAT" not in content
