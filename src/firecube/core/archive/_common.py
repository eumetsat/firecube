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

"""Restore-target helpers the ZIP and Tensogram restores share.

`clear_target` empties a local restore target. `refuse_other_product_label` refuses a
target that records another product label than the archive, and `seed_product_label`
gives an unstamped target the archive's stamp through the control plane's stamp writer.
"""

from __future__ import annotations

import shutil
from pathlib import Path
from typing import TYPE_CHECKING, cast

from firecube.core.controlplane.types import (
    CLAIMS_DIRNAME,
    CONTROL_DIRNAME,
    PRODUCT_LABEL_FILENAME,
    RUNS_DIRNAME,
    SCHEMA_FILENAME,
)
from firecube.core.errors import ConfigurationError

if TYPE_CHECKING:
    from firecube.core.filesystem import StorageFilesystemFull
    from firecube.core.storage.session import StorageSession

PRODUCT_LABEL_KEY = f"{CONTROL_DIRNAME}/{PRODUCT_LABEL_FILENAME}"
"""The product key of the label stamp, relative to the product root."""


def refuse_other_product_label(session: StorageSession, label: str) -> None:
    """Refuse `label` unless the session's product records it or records no label.

    Reads only.

    Raises:
        ProductLabelMismatchError: The product records another label.
        ProductLabelConflictError: An unstamped product's records name several labels.
        ManifestError: The stamp or a control-plane record cannot be read.
    """
    # Call-time imports: the control-plane modules stay out of the archive modules' import.
    from firecube.core.controlplane.product_label import check_product_label

    check_product_label(
        cast("StorageFilesystemFull", session.fs()), session.product.control_root_uri, label
    )


def seed_product_label(session: StorageSession, label: str, stamp: bytes) -> None:
    """Stamp the product with an archive's stamp bytes through `stamp_product_label`.

    A product that already records `label` keeps its own stamp.

    Raises:
        ProductLabelMismatchError: The product records another label.
        ProductLabelConflictError: An unstamped product's records name several labels.
        ManifestError: A control-plane record cannot be read, or `stamp` does not record `label`.
    """
    from firecube.core.controlplane.product_label import stamp_product_label

    fs = cast("StorageFilesystemFull", session.fs())
    stamp_product_label(fs, session.product.control_root_uri, label, content=stamp)


def clear_target(target_path: Path, *, keep_run_id: str | None) -> None:
    """Empty a restore target directory but keep the restore's own control-plane state.

    The kept entries are ``.firecube/claims/``, ``.firecube/schema.json``,
    ``.firecube/product.json`` and, when ``keep_run_id`` is given,
    ``.firecube/runs/<keep_run_id>/``.

    Raises:
        ConfigurationError: If the target, a kept entry or a directory holding one
            is a symlink; nothing is removed.
    """
    control = target_path / CONTROL_DIRNAME
    kept = {control / CLAIMS_DIRNAME, control / SCHEMA_FILENAME, control / PRODUCT_LABEL_FILENAME}
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
