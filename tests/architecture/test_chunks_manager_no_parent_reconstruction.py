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

"""Architecture tombstone for chunk-manager target control-root construction."""

from __future__ import annotations

from pathlib import Path

import pytest


@pytest.mark.architecture
def test_chunks_manager_does_not_reconstruct_target_via_parent() -> None:
    """Assert that src/firecube/cli/chunks/_manager.py does not call product_uri.parent() for target-based control-root construction. This would reintroduce the BASENAME heuristics anti-pattern documented in STYLE.md."""

    source = Path("src/firecube/cli/chunks/_manager.py").read_text(encoding="utf-8")

    assert "product_uri.parent()" not in source
