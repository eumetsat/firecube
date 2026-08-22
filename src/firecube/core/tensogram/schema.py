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

"""Archive schema constants and metadata helpers for Firecube .tgm archives."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import firecube

ARCHIVE_VERSION: str = "v1"

# Message role constants (stored in firecube.role within message metadata)
ROLE_DATA: str = "data"
ROLE_CONTROLPLANE: str = "controlplane"

# Metadata key names (for documentation / avoiding magic strings)
KEY_GROUP: str = "group"
KEY_ROLE: str = "role"
KEY_ARCHIVE_VERSION: str = "archive_version"


def make_data_meta(
    group: str,
    base: list[dict[str, Any]],
    *,
    source_uri: str,
    compression: str,
    coordinates: list[str] | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    extra_attrs: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build a complete metadata dict for a data message.

    Returns a dict suitable for passing to TensogramFile.append().
    """
    meta: dict[str, Any] = {
        "base": base,
        "firecube": {
            "version": getattr(firecube, "__version__", "unknown"),
            "archive_version": ARCHIVE_VERSION,
            "role": ROLE_DATA,
            "group": group,
            "source_uri": source_uri,
            "archived_at": datetime.now(UTC).isoformat(),
            "compression": compression,
        },
    }
    if coordinates is not None:
        meta["firecube"]["coordinates"] = coordinates
    if start_date is not None:
        meta["firecube"]["start_date"] = start_date
    if end_date is not None:
        meta["firecube"]["end_date"] = end_date
    if extra_attrs:
        meta.update(extra_attrs)
    return meta


def make_controlplane_meta(product: str) -> dict[str, Any]:
    """Build a metadata dict for a control-plane message.

    Returns a dict suitable for passing to TensogramFile.append().
    The control-plane message has no group — it covers all groups for the product.
    """
    return {
        "base": [{"name": "controlplane", "dim_names": ["bytes"]}],
        "firecube": {
            "version": getattr(firecube, "__version__", "unknown"),
            "archive_version": ARCHIVE_VERSION,
            "role": ROLE_CONTROLPLANE,
            "product": product,
            "archived_at": datetime.now(UTC).isoformat(),
        },
    }
