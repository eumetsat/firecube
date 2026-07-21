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

"""Workspace management and file materialization service."""

from __future__ import annotations

import hashlib
import logging
import os
import shutil
import tempfile
import threading
import uuid
from pathlib import Path
from typing import IO, Any, Protocol


# Re-export LocalSourceFile so it remains available via this module
# and can be re-exported by base.py for compatibility.
class SourceFile(Protocol):
    """Protocol for source files compatible with materialization."""

    @property
    def uri(self) -> str: ...
    def open(self) -> IO[bytes]: ...
    def local_path(self) -> Path | None: ...


class LocalSourceFile:
    """Implementation of SourceFile for local filesystem."""

    def __init__(self, path: Path | str):
        self._path = Path(path).resolve()

    @property
    def uri(self) -> str:
        return self._path.as_uri()

    def open(self) -> IO[bytes]:
        return self._path.open("rb")

    def local_path(self) -> Path | None:
        return self._path


class WorkspaceManager:
    """Manages temporary workspace and file materialization."""

    def __init__(self, prefix: str, *, storage_config: Any | None = None):
        self.prefix = prefix
        self._log = logging.getLogger(f"firecube.ingestor.workspace.{prefix}")
        self._temp_root: Path | None = None
        self._storage_config: Any | None = storage_config
        self._lock = threading.Lock()

    @property
    def temp_root(self) -> Path | None:
        """Return the current temporary root directory."""
        return self._temp_root

    def setup(self, ctx: Any) -> Path:
        """Setup the workspace temporary root using the context."""
        from firecube.core.workspaces import resolve_workspace_root

        # Resolve root using core logic
        workspace_root, _ = resolve_workspace_root(ctx.options, prefix=self.prefix)
        workspace_root.mkdir(parents=True, exist_ok=True)

        self._configure_temp_root(workspace_root)
        return workspace_root

    def _configure_temp_root(self, root: Path | None) -> None:
        if root is None:
            return
        root.mkdir(parents=True, exist_ok=True)
        self._temp_root = root

    def teardown(self, *, cleanup_dir: bool) -> None:
        """Reset workspace state and optionally delete the workspace directory."""
        workspace_to_clean = self._temp_root
        self._reset_temp_root()

        if not cleanup_dir:
            return

        if workspace_to_clean and workspace_to_clean.exists():
            try:
                self._log.info("Cleaning up workspace: %s", workspace_to_clean)
                shutil.rmtree(workspace_to_clean, ignore_errors=True)
            except Exception as exc:
                self._log.warning("Failed to clean up workspace: %s", exc)

    def cleanup(self) -> None:
        """Backward-compatible alias: reset and delete the workspace directory."""
        self.teardown(cleanup_dir=True)

    def _reset_temp_root(self) -> None:
        if self._temp_root is None:
            return
        self._temp_root = None

    def temporary_directory(self) -> tempfile.TemporaryDirectory:
        """Create a new temporary directory within the workspace."""
        if self._temp_root is None:
            return tempfile.TemporaryDirectory()
        self._temp_root.mkdir(parents=True, exist_ok=True)
        return tempfile.TemporaryDirectory(dir=str(self._temp_root))

    def materialize(self, source: Any) -> Path:
        """Materialize a source file to local disk (cached) safely."""
        from firecube.core.uris import is_remote_target

        # Handle remote URI strings — download to local cache
        if isinstance(source, str) and is_remote_target(source):
            return self._materialize_remote_uri(source)

        # 1. Resolve to SourceFile-like
        src_file: Any = source
        if isinstance(source, (str, Path)):
            src_file = LocalSourceFile(source)

        # 2. Check for local path (optimization)
        if hasattr(src_file, "local_path"):
            lp = src_file.local_path()
            if lp:
                return lp

        # 3. Cache Logic
        if not hasattr(src_file, "uri") or not hasattr(src_file, "open"):
            raise ValueError(f"Cannot materialize invalid source: {source}")

        if not self._temp_root:
            raise RuntimeError("Cannot materialize without a workspace/temp_root")

        cache_dir = self._temp_root / "materialized_cache"
        cache_dir.mkdir(exist_ok=True, parents=True)

        # Use hash of URI as a unique cache key. usedforsecurity=False marks
        # this MD5 use as a non-cryptographic content fingerprint.
        uri_hash = hashlib.md5(src_file.uri.encode("utf-8"), usedforsecurity=False).hexdigest()
        # Preserve extension if possible
        ext = Path(src_file.uri).suffix.split("?")[0]
        if len(ext) > 10:
            ext = ""

        final_path = cache_dir / f"{uri_hash}{ext}"
        partial_path = cache_dir / f".tmp.{uri_hash}.{uuid.uuid4().hex}{ext}"

        # Fast path: already exists
        if final_path.exists():
            return final_path

        # Thread-safe download logic using lock
        with self._lock:
            # Double-check inside lock
            if final_path.exists():
                return final_path

            self._log.info("Materializing remote file %s to %s", src_file.uri, final_path.name)
            try:
                with src_file.open() as src, open(partial_path, "wb") as dst:
                    shutil.copyfileobj(src, dst)

                os.replace(partial_path, final_path)
            except Exception as exc:
                if partial_path.exists():
                    os.remove(partial_path)
                raise exc

        return final_path

    def _materialize_remote_uri(self, uri: str) -> Path:
        """Download a remote URI to local cache and return the local Path."""
        from firecube.core.filesystem.ops import _open_fsspec_url

        if not self._temp_root:
            raise RuntimeError("Cannot materialize remote URI without a workspace/temp_root")

        uri_hash = hashlib.sha256(uri.encode()).hexdigest()[:16]
        last_segment = uri.rsplit("/", 1)[-1]
        suffix = last_segment.rsplit(".", 1)[-1] if "." in last_segment else ""
        cache_name = f"{uri_hash}.{suffix}" if suffix else uri_hash

        cache_dir = self._temp_root / "_remote_cache"
        final_path = cache_dir / cache_name

        if final_path.exists():
            return final_path

        with self._lock:
            if final_path.exists():
                return final_path

            cache_dir.mkdir(parents=True, exist_ok=True)
            partial_path = cache_dir / f".tmp.{uri_hash}.{uuid.uuid4().hex}"

            self._log.info("Downloading remote file %s to cache", uri)
            try:
                fs, root = _open_fsspec_url(uri, storage_config=self._storage_config)
                with fs.open(root, "rb") as src_f, open(partial_path, "wb") as dst_f:
                    shutil.copyfileobj(src_f, dst_f)
                os.replace(partial_path, final_path)
            except Exception as exc:
                if partial_path.exists():
                    os.remove(partial_path)
                raise exc

        return final_path
