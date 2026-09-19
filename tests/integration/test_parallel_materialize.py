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

"""RED test — locks the FUTURE contract of _materialize_remote_uri after T10 (per-URI lock dict).

Fails against current global-lock implementation.
"""

from __future__ import annotations

import io
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any

import pytest

from firecube.core.storage.uri import StorageUri
from firecube.ingestor.runtime.workspace import WorkspaceManager

pytestmark = [pytest.mark.integration, pytest.mark.concurrency]

_THREADS = 4
_DOWNLOAD_DELAY_S = 0.20
_FUTURE_TIMEOUT_S = 10.0


class _FakeRemoteFilesystem:
    def __init__(self, store: _FakeRemoteStore) -> None:
        self._store = store

    def open(self, root: StorageUri | str, mode: str = "rb") -> io.BytesIO:
        if mode != "rb":
            raise ValueError(f"unexpected mode {mode!r}")
        if isinstance(root, StorageUri):
            root = root.to_str()
        return io.BytesIO(self._store.download(root))


class _FakeRemoteStore:
    def __init__(self, *, delay_s: float = 0.0) -> None:
        self._delay_s = delay_s
        self._lock = threading.Lock()
        self._calls: dict[str, int] = {}

    @property
    def calls(self) -> dict[str, int]:
        with self._lock:
            return dict(self._calls)

    def open_source_filesystem(
        self, uri: str, storage_config: Any | None = None
    ) -> tuple[_FakeRemoteFilesystem, StorageUri]:
        _ = storage_config
        return _FakeRemoteFilesystem(self), StorageUri.parse(uri)

    def download(self, uri: str) -> bytes:
        with self._lock:
            self._calls[uri] = self._calls.get(uri, 0) + 1
        if self._delay_s:
            time.sleep(self._delay_s)
        return f"payload for {uri}".encode()


class _FailOnceRemoteStore(_FakeRemoteStore):
    def __init__(self, first_download_started: threading.Event) -> None:
        super().__init__()
        self._first_download_started = first_download_started
        self._attempt_lock = threading.Lock()
        self._attempt = 0

    def download(self, uri: str) -> bytes:
        with self._attempt_lock:
            self._attempt += 1
            attempt = self._attempt
        if attempt == 1:
            self._first_download_started.set()
            raise OSError("injected first download failure")
        return super().download(uri)


def _workspace(tmp_path: Path) -> WorkspaceManager:
    workspace = WorkspaceManager("parallel-materialize-test")
    workspace._configure_temp_root(tmp_path / "workspace")
    return workspace


def _assert_no_uri_lock_leaks(workspace: WorkspaceManager) -> None:
    assert vars(workspace)["_uri_locks"] == {}, (
        "per-URI lock map must be empty after materialization"
    )


def test_parallel_distinct_remote_uris_materialize_concurrently(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = _FakeRemoteStore(delay_s=_DOWNLOAD_DELAY_S)
    monkeypatch.setattr(
        "firecube.ingestor.runtime.workspace.open_source_filesystem",
        store.open_source_filesystem,
    )
    workspace = _workspace(tmp_path)
    uris = [f"s3://bucket/source-{idx}.bin" for idx in range(_THREADS)]
    barrier = threading.Barrier(_THREADS)

    def materialize(uri: str) -> Path:
        barrier.wait(timeout=_FUTURE_TIMEOUT_S)
        return workspace.materialize(uri)

    start = time.perf_counter()
    with ThreadPoolExecutor(max_workers=_THREADS) as pool:
        futures = [pool.submit(materialize, uri) for uri in uris]
        paths = [future.result(timeout=_FUTURE_TIMEOUT_S) for future in futures]
    elapsed = time.perf_counter() - start

    assert len(set(paths)) == _THREADS
    assert store.calls == dict.fromkeys(uris, 1)
    assert elapsed < 0.50, (
        f"distinct URIs should download concurrently under per-URI locks; elapsed={elapsed:.3f}s "
        "looks serialized by a global workspace lock"
    )
    _assert_no_uri_lock_leaks(workspace)


def test_parallel_same_remote_uri_deduplicates_in_flight_download(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = _FakeRemoteStore(delay_s=_DOWNLOAD_DELAY_S)
    monkeypatch.setattr(
        "firecube.ingestor.runtime.workspace.open_source_filesystem",
        store.open_source_filesystem,
    )
    workspace = _workspace(tmp_path)
    uri = "s3://bucket/shared.bin"
    barrier = threading.Barrier(_THREADS)

    def materialize() -> Path:
        barrier.wait(timeout=_FUTURE_TIMEOUT_S)
        return workspace.materialize(uri)

    with ThreadPoolExecutor(max_workers=_THREADS) as pool:
        futures = [pool.submit(materialize) for _ in range(_THREADS)]
        paths = [future.result(timeout=_FUTURE_TIMEOUT_S) for future in futures]

    assert store.calls == {uri: 1}
    assert len(set(paths)) == 1
    _assert_no_uri_lock_leaks(workspace)


def test_failed_remote_materialization_releases_uri_lock_for_retry(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    first_download_started = threading.Event()
    store = _FailOnceRemoteStore(first_download_started)
    monkeypatch.setattr(
        "firecube.ingestor.runtime.workspace.open_source_filesystem",
        store.open_source_filesystem,
    )
    workspace = _workspace(tmp_path)
    uri = "s3://bucket/flaky.bin"
    first_error: list[BaseException] = []

    def first_attempt() -> None:
        try:
            workspace.materialize(uri)
        except BaseException as exc:
            first_error.append(exc)

    def retry_after_first_download_begins() -> Path:
        assert first_download_started.wait(timeout=_FUTURE_TIMEOUT_S)
        return workspace.materialize(uri)

    with ThreadPoolExecutor(max_workers=2) as pool:
        failing_future = pool.submit(first_attempt)
        retry_future = pool.submit(retry_after_first_download_begins)
        failing_future.result(timeout=_FUTURE_TIMEOUT_S)
        retry_path = retry_future.result(timeout=_FUTURE_TIMEOUT_S)

    assert len(first_error) == 1
    assert isinstance(first_error[0], OSError)
    assert retry_path.is_file()
    assert retry_path.read_bytes() == f"payload for {uri}".encode()
    assert store.calls == {uri: 1}
    cache_dir = tmp_path / "workspace" / "_remote_cache"
    assert not list(cache_dir.glob(".tmp.*"))
    _assert_no_uri_lock_leaks(workspace)


@pytest.mark.integration
@pytest.mark.concurrency
def test_refcount_prevents_race_on_failed_download(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Failed first downloads keep waiters on one refcounted per-URI lock."""
    first_download_started = threading.Event()
    allow_first_failure = threading.Event()
    first_attempt_failed = threading.Event()
    third_attempt_started = threading.Event()
    download_activity_lock = threading.Lock()
    active_downloads = 0
    max_active_downloads = 0

    class _RefcountRaceRemoteStore(_FakeRemoteStore):
        def download(self, uri: str) -> bytes:
            nonlocal active_downloads, max_active_downloads

            with self._lock:
                self._calls[uri] = self._calls.get(uri, 0) + 1
                attempt = self._calls[uri]

            with download_activity_lock:
                active_downloads += 1
                max_active_downloads = max(max_active_downloads, active_downloads)

            try:
                if attempt == 1:
                    first_download_started.set()
                    assert allow_first_failure.wait(timeout=_FUTURE_TIMEOUT_S)
                    raise OSError("injected first download failure")

                assert third_attempt_started.wait(timeout=_FUTURE_TIMEOUT_S)
                return f"payload for {uri}".encode()
            finally:
                with download_activity_lock:
                    active_downloads -= 1

    def wait_for_uri_refcount(workspace: WorkspaceManager, expected: int) -> None:
        waiter = threading.Event()
        deadline = time.monotonic() + _FUTURE_TIMEOUT_S
        while time.monotonic() < deadline:
            with vars(workspace)["_uri_locks_meta"]:
                if any(count >= expected for _, count in vars(workspace)["_uri_locks"].values()):
                    return
            waiter.wait(timeout=0.01)
        pytest.fail(f"timed out waiting for per-URI lock refcount >= {expected}")

    store = _RefcountRaceRemoteStore()
    monkeypatch.setattr(
        "firecube.ingestor.runtime.workspace.open_source_filesystem",
        store.open_source_filesystem,
    )
    workspace = _workspace(tmp_path)
    uri = "s3://bucket/refcount-race.bin"

    def failing_attempt() -> None:
        try:
            workspace.materialize(uri)
        except OSError:
            first_attempt_failed.set()
            raise

    def retry_attempt() -> Path:
        assert first_download_started.wait(timeout=_FUTURE_TIMEOUT_S)
        return workspace.materialize(uri)

    def fast_path_attempt() -> Path:
        assert first_attempt_failed.wait(timeout=_FUTURE_TIMEOUT_S)
        third_attempt_started.set()
        return workspace.materialize(uri)

    with ThreadPoolExecutor(max_workers=3) as pool:
        failing_future = pool.submit(failing_attempt)
        assert first_download_started.wait(timeout=_FUTURE_TIMEOUT_S)
        retry_future = pool.submit(retry_attempt)
        wait_for_uri_refcount(workspace, expected=2)
        allow_first_failure.set()
        with pytest.raises(OSError, match="injected first download failure"):
            failing_future.result(timeout=_FUTURE_TIMEOUT_S)
        fast_path_future = pool.submit(fast_path_attempt)
        retry_path = retry_future.result(timeout=_FUTURE_TIMEOUT_S)
        fast_path = fast_path_future.result(timeout=_FUTURE_TIMEOUT_S)

    assert store.calls == {uri: 2}
    assert retry_path == fast_path
    assert retry_path.read_bytes() == f"payload for {uri}".encode()
    assert max_active_downloads == 1
    _assert_no_uri_lock_leaks(workspace)
