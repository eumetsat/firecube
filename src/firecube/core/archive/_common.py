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

"""Clear a local restore target; the ZIP and Tensogram restores share it."""

from __future__ import annotations

import shutil
from pathlib import Path

from firecube.core.controlplane.types import (
    CLAIMS_DIRNAME,
    CONTROL_DIRNAME,
    RUNS_DIRNAME,
    SCHEMA_FILENAME,
)
from firecube.core.errors import ConfigurationError


def clear_target(target_path: Path, *, keep_run_id: str | None) -> None:
    """Empty a restore target directory but keep the restore's own control-plane state.

    The kept entries are ``.firecube/claims/``, ``.firecube/schema.json`` and, when
    ``keep_run_id`` is given, ``.firecube/runs/<keep_run_id>/``.

    Raises:
        ConfigurationError: If the target, a kept entry or a directory holding one
            is a symlink; nothing is removed.
    """
    control = target_path / CONTROL_DIRNAME
    kept = {control / CLAIMS_DIRNAME, control / SCHEMA_FILENAME}
    if keep_run_id is not None:
        kept.add(control / RUNS_DIRNAME / keep_run_id)
    _refuse_symlinked_kept_paths(target_path, kept)
    _remove_all_except(target_path, kept)


def _refuse_symlinked_kept_paths(target_path: Path, kept: set[Path]) -> None:
    """Raise ConfigurationError when a kept path or a directory above it is a symlink.

    The check stops at the target. Clearing walks into those directories, so a symlink would
    lead it outside the target.
    """
    for path in sorted(kept):
        for node in (path, *path.parents):
            if node.is_symlink():
                raise ConfigurationError(
                    f"Restore target {target_path} holds a symlink at {node}; refusing to "
                    "clear it, because the restore would follow it outside the target"
                )
            if node == target_path:
                break


def _remove_all_except(directory: Path, kept: set[Path]) -> None:
    """Remove every entry under ``directory`` except the paths in ``kept`` and their ancestors.

    A symlink is unlinked, never followed.
    """
    for entry in directory.iterdir():
        if entry in kept:
            continue
        if entry.is_symlink() or not entry.is_dir():
            entry.unlink()
        elif any(entry in path.parents for path in kept):
            _remove_all_except(entry, kept)
        else:
            shutil.rmtree(entry)
