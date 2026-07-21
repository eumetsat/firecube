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

"""Generic runtime helpers for ingestion plugins.

These helpers are intentionally optional. Plugins that need a workspace and/or
temporary directory root can use them to avoid duplicating path wiring logic.
"""

from __future__ import annotations

import tempfile
from collections.abc import Mapping
from pathlib import Path
from typing import Any


def resolve_workspace_root(
    options: Mapping[str, Any],
    *,
    prefix: str,
    option_key: str = "workspace",
) -> tuple[Path, bool]:
    """Return (workspace_root, autocreated).

    If `options[option_key]` is present, the workspace is treated as
    user-provided (autocreated=False). Otherwise, a temporary directory is
    created with the given prefix.
    """
    user_value = options.get(option_key)
    if user_value:
        return Path(user_value), False
    return Path(tempfile.mkdtemp(prefix=f"{prefix}_")), True


def ensure_workspace_dirs(
    workspace_root: Path,
    *,
    temp_subdir: str = "tmp",
) -> tuple[Path, Path]:
    """Ensure workspace directory exists and return (workspace_root, temp_root)."""
    workspace_root.mkdir(parents=True, exist_ok=True)
    temp_root = workspace_root / temp_subdir
    temp_root.mkdir(parents=True, exist_ok=True)
    return workspace_root, temp_root


def ensure_batch_workspace(
    workspace_root: Path,
    *,
    batch_id: str,
    prefix: str = "pipeline",
    temp_subdir: str = "tmp",
) -> tuple[Path, Path]:
    """Return (batch_workspace, temp_root) as a subdirectory under workspace_root."""
    batch_workspace = workspace_root / f"{prefix}_{batch_id}"
    return ensure_workspace_dirs(batch_workspace, temp_subdir=temp_subdir)
