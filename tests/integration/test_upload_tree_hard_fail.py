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

"""RED test — ``upload_tree`` multipart failure must hard-fail without fallback.

Confirms the bug at ``src/firecube/core/storage/session.py:359-369`` where
``except Exception: pass`` silently swallows multipart-upload errors and then
falls back to ``fs.open(dst, "wb")``. Policy (notepads HIGH-2) is the opposite:
the original exception must propagate unwrapped and ``fs.open`` must not be
invoked. This file goes GREEN after T10 hardens the code path.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from firecube.core.config import StorageConfig
from firecube.core.errors import StorageError
from firecube.core.filesystem.fsspec_backend import FsspecFilesystem
from firecube.core.storage.uri import StorageUri
from tests.helpers.storage import make_test_session


class _MultipartFailingFs:
    def __init__(self, base: Path, root: str) -> None:
        self.base = base
        self.root = root.strip("/")
        self.put_file_calls: list[tuple[str, str]] = []
        self.open_calls: list[tuple[str, str]] = []

    def _local_path(self, path: Any) -> Path:
        rel_str = path.to_str() if hasattr(path, "to_str") else str(path)
        if "://" in rel_str:
            rel_str = rel_str.split("://", 1)[1]
        rel = rel_str.strip("/")
        if self.root and rel.startswith(self.root):
            rel = rel[len(self.root) :].strip("/")
        return self.base / rel

    def put_file(self, local_path: str, remote_path: str) -> None:
        self.put_file_calls.append((local_path, remote_path))
        raise RuntimeError("simulated multipart failure")

    def open(self, path: Any, mode: str = "rb") -> Any:
        self.open_calls.append((str(path), mode))
        raise AssertionError(
            f"fallback fs.open invoked unexpectedly (path={path!r}, mode={mode!r}); "
            "multipart upload failure must propagate, not silently fall back to fs.open"
        )

    def makedirs(self, path: Any, exist_ok: bool = False) -> None:
        self._local_path(path).mkdir(parents=True, exist_ok=exist_ok)


def test_upload_tree_hard_fails_when_multipart_raises(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Multipart failure must propagate unwrapped; no silent ``fs.open`` fallback."""
    source = tmp_path / "source.zarr"
    source.mkdir()
    large = source / "large.bin"
    large.write_bytes(b"largepayload-exceeds-multipart-threshold")

    remote_root = tmp_path / "remote"
    fake_fs = _MultipartFailingFs(remote_root, "bucket/prefix")

    def _fake_create_filesystem(binding: Any) -> _MultipartFailingFs:
        return fake_fs

    monkeypatch.setattr("firecube.core.storage.session.create_filesystem", _fake_create_filesystem)

    storage_config = StorageConfig(storage_type="s3", storage_driver="fsspec")
    storage_config.bucket = "bucket"  # type: ignore[attr-defined]
    session = make_test_session(
        tmp_path,
        product="prefix",
        protocol="s3",
        authority="bucket",
    )

    with pytest.raises(StorageError, match="simulated multipart failure"):
        session.upload_tree(
            StorageUri.from_local_path(source),
            StorageUri.parse("s3://bucket/prefix"),
            multipart_threshold=1,
            parallel_workers=1,
        )

    assert fake_fs.put_file_calls, "put_file must have been attempted for large file"
    assert fake_fs.open_calls == [], (
        "fs.open must NOT be invoked after a multipart-upload failure; "
        f"observed fallback calls: {fake_fs.open_calls!r}"
    )


class _MultipartCapableFs(FsspecFilesystem):
    """Mock adapter exposing ``multipart_upload`` (the canonical capability API).

    Shape mirrors ``FsspecFilesystem``/``ObstoreFilesystem``: multipart is
    advertised via the ``Multipart`` capability set and routed through
    ``multipart_upload(local_path, remote_path)``. ``put_file`` is intentionally
    absent so ``_upload_tree_multipart`` is forced past the fsspec-style escape
    hatch into the buggy ``FsspecFilesystem(fs)`` branch when the proxy is not
    unwrapped first.
    """

    def __init__(self, base: Path, root: str) -> None:
        self.base = base
        self.root = root.strip("/")
        self.open_calls: list[tuple[str, str]] = []
        self.multipart_upload_calls: list[tuple[str, str]] = []

    def _local_path(self, path: Any) -> Path:
        rel_str = path.to_str() if hasattr(path, "to_str") else str(path)
        if "://" in rel_str:
            rel_str = rel_str.split("://", 1)[1]
        rel = rel_str.strip("/")
        if self.root and rel.startswith(self.root):
            rel = rel[len(self.root) :].strip("/")
        return self.base / rel

    def open(self, uri: Any, mode: str = "rb") -> Any:
        local_path = self._local_path(uri)
        self.open_calls.append((str(uri), mode))
        if any(flag in mode for flag in ("w", "a", "+")):
            local_path.parent.mkdir(parents=True, exist_ok=True)
        return local_path.open(mode)

    def makedirs(self, uri: Any, exist_ok: bool = False) -> None:
        self._local_path(uri).mkdir(parents=True, exist_ok=exist_ok)

    def multipart_upload(
        self,
        local_path: str,
        remote_path: str,
        *,
        part_size: int = 64 * 1024 * 1024,
    ) -> None:
        self.multipart_upload_calls.append((local_path, remote_path))
        target = self._local_path(remote_path)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(Path(local_path).read_bytes())

    def capabilities(self) -> set[type]:
        from firecube.core.filesystem.protocol import (
            Multipart,  # pyright: ignore[reportAttributeAccessIssue]
        )

        return {Multipart}


def test_upload_tree_multipart_unwraps_instrumented_filesystem(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``InstrumentedFilesystem`` must be unwrapped before multipart routing.

    Current code (T7 RED) — ``_upload_tree_multipart`` at
    ``src/firecube/core/storage/session.py:372-389``:

    - ``getattr(instrumented_fs, "put_file", None)`` returns ``None`` because
      the underlying adapter exposes streaming via ``multipart_upload`` rather
      than fsspec-style ``put_file``.
    - ``isinstance(fs, (FsspecFilesystem, ObstoreFilesystem))`` is ``False``
      because ``fs`` is the proxy, so the code reaches ``FsspecFilesystem(fs)``.
    - ``FsspecFilesystem.__init__`` requires a ``StorageBinding`` and raises
      ``AttributeError`` on ``binding.identity`` when handed a proxy.
    - ``except Exception: pass`` at ``session.py:364-365`` swallows the error
      and silently falls back to ``fs.open(dst, "wb")``. The multipart
      streaming path is bypassed; ``multipart_upload`` is never invoked.

    After T10 GREEN: the proxy is unwrapped before multipart routing, so the
    underlying adapter's ``multipart_upload`` is invoked and ``fs.open("wb")``
    is never used.
    """
    from firecube.core.filesystem.instrumentation import (
        InstrumentedFilesystem,
        collect_filesystem_metrics,
    )

    multipart_threshold = 1024
    payload = b"x" * (multipart_threshold * 4)

    source = tmp_path / "source.zarr"
    source.mkdir()
    large = source / "large.bin"
    large.write_bytes(payload)

    remote_root = tmp_path / "remote"
    raw_fs = _MultipartCapableFs(remote_root, "bucket/prefix")

    def _fake_create_filesystem(binding: Any) -> _MultipartCapableFs:
        return raw_fs

    monkeypatch.setattr("firecube.core.storage.session.create_filesystem", _fake_create_filesystem)

    session = make_test_session(
        tmp_path,
        product="prefix",
        protocol="s3",
        authority="bucket",
    )

    with collect_filesystem_metrics():
        dst_fs = session.fs()
        assert isinstance(dst_fs, InstrumentedFilesystem), (
            "precondition: session.fs() must return InstrumentedFilesystem when "
            f"metrics collection is active; got {type(dst_fs).__name__}"
        )
        session.upload_tree(
            StorageUri.from_local_path(source),
            StorageUri.parse("s3://bucket/prefix"),
            multipart_threshold=multipart_threshold,
            parallel_workers=1,
        )

    assert (remote_root / "large.bin").read_bytes() == payload, (
        "uploaded file content must match source (regardless of which upload path was used)"
    )

    assert raw_fs.multipart_upload_calls, (
        "multipart_upload must be invoked on the unwrapped adapter; "
        f"observed multipart_upload_calls={raw_fs.multipart_upload_calls!r}, "
        f"open_calls={raw_fs.open_calls!r} — InstrumentedFilesystem was not "
        "unwrapped before _upload_tree_multipart routed through "
        "FsspecFilesystem(fs), so the AttributeError was silently swallowed "
        "and fs.open('wb') was used instead."
    )

    write_opens = [
        call for call in raw_fs.open_calls if any(flag in call[1] for flag in ("w", "a", "+"))
    ]
    assert write_opens == [], (
        "fs.open(write) fallback must NOT be invoked when InstrumentedFilesystem "
        f"wraps a multipart-capable adapter; observed write_opens={write_opens!r}"
    )
