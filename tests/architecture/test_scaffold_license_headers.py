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

pytestmark = pytest.mark.architecture


def test_tpl_files_have_apache_header() -> None:
    template_dir = Path("src/firecube/ingestor/devtools/_templates")
    template_paths = sorted(template_dir.glob("*.tpl"))

    assert template_paths
    for template_path in template_paths:
        content = template_path.read_text(encoding="utf-8")
        assert "Licensed under the Apache License" in content, template_path
        assert "FIRECUBE_TEMPLATE_LICENSE_HEADER_BEGIN" in content, template_path
        assert "FIRECUBE_TEMPLATE_LICENSE_HEADER_END" in content, template_path
