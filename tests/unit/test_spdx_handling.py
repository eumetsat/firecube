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

from firecube.ingestor.devtools.scaffolding import create_plugin_structure

pytestmark = pytest.mark.unit


def _generated_ingestor(tmp_path: Path, *, author_name: str = "Test Author", license: str) -> str:
    project_root = create_plugin_structure(
        "m3_spdx",
        tmp_path,
        author_name=author_name,
        author_email="test@example.com",
        license=license,
        template_type="zarr",
    )
    return (project_root / "src" / "firecube_m3_spdx" / "ingestor.py").read_text(encoding="utf-8")


@pytest.mark.parametrize(
    ("license", "expected"),
    [
        ("Proprietary", "LicenseRef-Proprietary"),
        ("My Company License", "LicenseRef-My-Company-License"),
    ],
)
def test_non_spdx_license_emits_licenseref(tmp_path: Path, license: str, expected: str) -> None:
    content = _generated_ingestor(tmp_path, license=license)

    assert f"SPDX-License-Identifier: {expected}" in content
    assert f"SPDX-License-Identifier: {license}\n" not in content


def test_empty_author_uses_fallback(tmp_path: Path) -> None:
    content = _generated_ingestor(tmp_path, author_name="", license="MIT")

    assert "Copyright" in content
    assert "Firecube Developer" in content
    assert "Copyright " in content
    assert "Copyright  " not in content


@pytest.mark.parametrize(
    "license",
    ["MIT", "GPL-3.0-or-later", "EUPL-1.2", "AGPL-3.0-only", "CC-BY-4.0", "MIT OR Apache-2.0"],
)
def test_valid_spdx_unchanged(tmp_path: Path, license: str) -> None:
    content = _generated_ingestor(tmp_path, license=license)

    assert f"SPDX-License-Identifier: {license}\n" in content
    assert "LicenseRef-" not in content
