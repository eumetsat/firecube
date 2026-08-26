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

"""Pure control-plane path derivations."""

from __future__ import annotations

from collections.abc import Callable

from firecube.core.controlplane.types import RUNS_DIRNAME
from firecube.core.storage.uri import StorageUri


def _validate_run_id(run_id: str) -> str:
    candidate = str(run_id)
    if not candidate:
        raise ValueError("run_id must be non-empty")

    segments = candidate.split("/")
    if any(segment == "" for segment in segments):
        raise ValueError("run_id must be a single path segment")
    if any(segment in {".", ".."} for segment in segments):
        raise ValueError("run_id must not contain path traversal segments")
    return candidate


def run_dir_for(
    resolver: Callable[[str], tuple[StorageUri, StorageUri]],
    product: str,
    run_id: str,
) -> tuple[StorageUri, str]:
    """Return the run directory and run URI for ``product`` and ``run_id``.

    The derivation is pure. It performs no filesystem I/O. ``run_id`` must be a
    single path segment, so ``/``, ``.`` and ``..`` path components are rejected.
    """

    control_path, control_uri = resolver(product)
    safe_run_id = _validate_run_id(run_id)
    run_dir = control_path.join(RUNS_DIRNAME).join(safe_run_id)
    run_uri = control_uri.join(RUNS_DIRNAME, safe_run_id).to_str()
    return run_dir, run_uri
