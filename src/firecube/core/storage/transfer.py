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

"""Cross-endpoint file transfer primitives.

StorageSession owns one endpoint binding. Transfers between two endpoints
(e.g., local filesystem → remote S3) require explicit per-endpoint
sessions. This module provides stateless transfer primitives that open
each endpoint with its own driver-correct filesystem.
"""

from __future__ import annotations

import shutil
from typing import TYPE_CHECKING, Any, cast

from firecube.core.storage.uri import StorageUri

if TYPE_CHECKING:
    from firecube.core.storage.driver_config import StorageDriverConfig
    from firecube.core.storage.session import StorageSession


def session_for_uri(
    uri: StorageUri,
    driver_config: StorageDriverConfig,
    *,
    format: str = "tensogram",
) -> StorageSession:
    """Construct a minimal StorageSession bound to ``uri``'s endpoint.

    Used by callers (e.g., CLI archive create/restore) that need to
    transfer to/from a URI that is NOT the same endpoint as their
    primary product session. The returned session is single-purpose:
    its product_uri is the input ``uri`` itself, and credentials/region
    come from ``driver_config``.

    For obstore (which is bucket-scoped), this guarantees the right
    S3Store is constructed. For fsspec, it ensures consistent driver
    configuration even when bucket differs from the product session.

    ``format`` defaults to ``"tensogram"`` (the primary archive transfer
    artifact). Caller may override with any value in
    ``ProductIdentity.VALID_FORMATS`` if the transfer target is a
    different format. Format is metadata-only and does not affect
    transfer behavior; it satisfies ``ProductIdentity`` validation.
    """
    from firecube.core.product.identity import ProductIdentity
    from firecube.core.storage.binding import StorageBinding
    from firecube.core.storage.session import StorageSession

    identity = ProductIdentity(
        product_uri=uri,
        control_root_uri=uri.parent().join(".firecube"),
        product_name=uri.path.rstrip("/").rsplit("/", 1)[-1] or "transfer",
        format=format,
    )
    return StorageSession(StorageBinding(identity=identity, driver=driver_config))


def copy_file(
    src: StorageUri,
    dst: StorageUri,
    *,
    source_session: StorageSession | None = None,
    target_session: StorageSession | None = None,
) -> None:
    """Copy a single file between two storage endpoints.

    Strict semantics:
    - If src is remote: source_session is REQUIRED and must be bound to src's endpoint
    - If src is local: source_session is ignored; opened via local Path
    - If dst is remote: target_session is REQUIRED and must be bound to dst's endpoint
    - If dst is local: target_session is ignored; opened via local Path

    Raises ValueError if a remote endpoint lacks its required session.
    Raises ValueError if a provided session's binding doesn't match the URI's endpoint.

    Validation order: ALL session/binding checks complete BEFORE any file
    handles are opened. This guarantees mismatched-binding failures are
    surfaced loudly without leaking file handles or partial writes.
    """
    from firecube.core.uris import local_path_from_target

    # PHASE 1: validate sessions+bindings BEFORE any I/O
    if src.is_remote():
        if source_session is None:
            raise ValueError(f"remote source {src.to_str()} requires source_session")
        _validate_session_binding(source_session, src, role="source")
    if dst.is_remote():
        if target_session is None:
            raise ValueError(f"remote destination {dst.to_str()} requires target_session")
        _validate_session_binding(target_session, dst, role="target")

    # PHASE 2: open both handles (validation already passed)
    if src.is_remote():
        assert source_session is not None  # narrowed by Phase 1
        src_handle = cast(Any, source_session.fs()).open(src, "rb")
    else:
        src_path = local_path_from_target(src.to_str())
        src_handle = src_path.open("rb")

    if dst.is_remote():
        assert target_session is not None  # narrowed by Phase 1
        dst_handle = cast(Any, target_session.fs()).open(dst, "wb")
    else:
        dst_path = local_path_from_target(dst.to_str())
        dst_path.parent.mkdir(parents=True, exist_ok=True)
        dst_handle = dst_path.open("wb")

    # PHASE 3: copy
    try:
        shutil.copyfileobj(src_handle, dst_handle)
    finally:
        src_handle.close()
        dst_handle.close()


def _validate_session_binding(
    session: StorageSession,
    uri: StorageUri,
    *,
    role: str,
) -> None:
    """Ensure session is bound to the same protocol+authority as the URI."""
    bound_uri = session.product.product_uri
    if bound_uri.protocol != uri.protocol or bound_uri.authority != uri.authority:
        raise ValueError(
            f"{role}_session is bound to {bound_uri.protocol}://{bound_uri.authority}, "
            f"but URI is {uri.protocol}://{uri.authority}"
        )
