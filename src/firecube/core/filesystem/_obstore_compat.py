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

"""Optional obstore dependency with graceful fallback."""

# pyright: reportMissingImports=false

from __future__ import annotations

from typing import Any

PutMode: Any

try:
    import obstore
    from obstore.exceptions import AlreadyExistsError
    from obstore.store import LocalStore, S3Store, from_url

    _put_mode = getattr(obstore, "PutMode", None)
    if _put_mode is None:

        class _PutModeCompat:
            """Compatibility facade for obstore versions where PutMode is a type alias."""

            # Expose PutMode.Create for call sites even when runtime obstore uses a type alias.
            Create = "create"
            Overwrite = "overwrite"

        PutMode = _PutModeCompat
    else:
        PutMode = _put_mode

    HAS_OBSTORE = True
except ImportError:
    HAS_OBSTORE = False
    AlreadyExistsError = FileExistsError  # type: ignore[assignment,misc]
    S3Store = None  # type: ignore[assignment,misc]
    LocalStore = None  # type: ignore[assignment,misc]
    PutMode = None  # type: ignore[assignment,misc]
    from_url = None  # type: ignore[assignment]


def require_obstore() -> None:
    """Raise ImportError with install instructions if obstore is not available."""
    if not HAS_OBSTORE:
        raise ImportError(
            "obstore is required for --storage-driver obstore. "
            "Install it with: uv pip install 'firecube[obstore]'"
        )
