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

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path

from firecube.core.product.identity import ProductIdentity, ensure_product_uri
from firecube.core.storage.uri import StorageUri
from firecube.core.uris import (
    is_remote_target,
    is_windows_absolute_path,
)


class CompletionRoute(StrEnum):
    DIRECT = "direct"
    STAGED = "staged"


@dataclass(frozen=True, slots=True)
class WriteModePolicy:
    name: str
    resolves_to_workspace: bool
    seeds_staged_metadata: bool
    storage_handled_by_engine: bool
    completion_route: CompletionRoute


STAGED_WRITE_MODE = WriteModePolicy(
    name="staged",
    resolves_to_workspace=True,
    seeds_staged_metadata=True,
    storage_handled_by_engine=False,
    completion_route=CompletionRoute.STAGED,
)
DIRECT_WRITE_MODE = WriteModePolicy(
    name="direct",
    resolves_to_workspace=False,
    seeds_staged_metadata=False,
    storage_handled_by_engine=True,
    completion_route=CompletionRoute.DIRECT,
)

_WRITE_MODE_POLICIES = {
    STAGED_WRITE_MODE.name: STAGED_WRITE_MODE,
    DIRECT_WRITE_MODE.name: DIRECT_WRITE_MODE,
}


def write_mode_policy(write_mode: str | WriteModePolicy) -> WriteModePolicy:
    if isinstance(write_mode, WriteModePolicy):
        return write_mode
    try:
        return _WRITE_MODE_POLICIES[str(write_mode)]
    except KeyError as exc:
        valid = ", ".join(sorted(_WRITE_MODE_POLICIES))
        raise ValueError(f"Unknown write mode {write_mode!r}; expected one of: {valid}") from exc


class ProductResolver:
    @staticmethod
    def resolve(target: str, format: str, product_name: str) -> ProductIdentity:
        """Boundary-only: parse target string into typed ProductIdentity."""
        try:
            uri = StorageUri.parse(target)
        except ValueError as e:
            msg = str(e).lower()
            if "scheme" in msg or "file://" in msg or "bare paths" in msg:
                raise ValueError(
                    "--target must be a full URI like 's3://bucket/path/x.zarr' or "
                    "'file:///abs/path/x.zarr'. Bare names and relative paths are rejected. "
                    f"Original error: {e}"
                ) from e
            raise
        return ProductIdentity.from_uri(uri, format=format, product_name=product_name)


def resolve_dataset_target(
    target: str,
    *,
    write_mode: str | WriteModePolicy = "staged",
    temp_root: Path | None = None,
    direct_base_uri: str | None = None,
) -> str:
    """Resolve the URI for a dataset target (Dataset Directory).

    This determines the "Package Root" for Zarr/Parquet output, considering:
    1. Absolute/Full URIs (s3://, /abs/path) -> Used as-is.
    2. Relative paths + Staged mode -> Anchored to temporary workspace.
    3. Relative paths + Direct mode -> Anchored to the typed product URI's parent.
    """
    if (
        is_remote_target(target)
        or Path(target).is_absolute()  # firecube: STORAGE-URI
        or target.startswith("file://")
        or is_windows_absolute_path(target)
    ):
        return target

    if write_mode_policy(write_mode).resolves_to_workspace:
        workspace = temp_root if temp_root else Path.cwd()
        return str(workspace / target)

    if direct_base_uri:
        return ensure_product_uri(direct_base_uri, target)

    return target
