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

"""Filesystem helpers and instrumentation facade.

Public imports remain stable at ``firecube.core.filesystem``.
"""

from firecube.core.filesystem.fsspec_backend import FsspecFilesystem
from firecube.core.filesystem.instrumentation import collect_filesystem_metrics
from firecube.core.filesystem.obstore_backend import ObstoreFilesystem
from firecube.core.filesystem.ops import (
    create_filesystem,
    create_filesystem_for_uri,
    create_session_zarr_store,
    delete_path,
    ensure_directory,
    fs_kwargs_for_uri,
    path_stats,
    safe_exists,
    safe_open,
)
from firecube.core.filesystem.protocol import (
    Multipart,
    RangedRead,
    Signer,
    StorageFilesystem,
    StorageFilesystemFull,
)

__all__ = [
    "FsspecFilesystem",
    "Multipart",
    "ObstoreFilesystem",
    "RangedRead",
    "Signer",
    "StorageFilesystem",
    "StorageFilesystemFull",
    "collect_filesystem_metrics",
    "create_filesystem",
    "create_filesystem_for_uri",
    "create_session_zarr_store",
    "delete_path",
    "ensure_directory",
    "fs_kwargs_for_uri",
    "path_stats",
    "safe_exists",
    "safe_open",
]
