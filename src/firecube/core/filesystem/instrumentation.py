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

"""Low-overhead client-side filesystem metrics for ingestion runs.

These metrics are process-local and measure client-observed filesystem calls.
They do not represent storage backend internals.
"""

from __future__ import annotations

import contextlib
import contextvars
import time
from dataclasses import dataclass
from typing import Any, ClassVar

from firecube.core.observability.metrics import (
    FS_SUMMARY_KEY_BYTES_READ,
    FS_SUMMARY_KEY_BYTES_WRITTEN,
    FS_SUMMARY_KEY_ERRORS,
    FS_SUMMARY_KEY_LATENCY,
    FS_SUMMARY_KEY_REQUESTS,
    FS_SUMMARY_KEY_RETRYABLE_ERRORS,
)


@dataclass(slots=True)
class FilesystemMetrics:
    request_count: int = 0
    error_count: int = 0
    retryable_error_count: int = 0
    latency_s_total: float = 0.0
    bytes_read: int = 0
    bytes_written: int = 0

    def as_summary(self) -> dict[str, int | float]:
        return {
            FS_SUMMARY_KEY_REQUESTS: int(self.request_count),
            FS_SUMMARY_KEY_ERRORS: int(self.error_count),
            FS_SUMMARY_KEY_RETRYABLE_ERRORS: int(self.retryable_error_count),
            FS_SUMMARY_KEY_LATENCY: float(self.latency_s_total),
            FS_SUMMARY_KEY_BYTES_READ: int(self.bytes_read),
            FS_SUMMARY_KEY_BYTES_WRITTEN: int(self.bytes_written),
        }


_ACTIVE_METRICS: contextvars.ContextVar[FilesystemMetrics | None] = contextvars.ContextVar(
    "firecube_active_filesystem_metrics",
    default=None,
)


def _is_retryable_error(exc: Exception) -> bool:
    text = str(exc).lower()
    markers = ("retry", "throttl", "timeout", "temporar", "slowdown", "try again")
    return any(marker in text for marker in markers)


def active_filesystem_metrics() -> FilesystemMetrics | None:
    """Return currently active filesystem metrics collector for this context."""
    return _ACTIVE_METRICS.get()


@contextlib.contextmanager
def collect_filesystem_metrics():
    """Collect filesystem metrics for the current context."""
    metrics = FilesystemMetrics()
    token = _ACTIVE_METRICS.set(metrics)
    try:
        yield metrics
    finally:
        _ACTIVE_METRICS.reset(token)


def record_fs_call(
    *,
    duration_s: float,
    success: bool,
    bytes_read: int = 0,
    bytes_written: int = 0,
    error: Exception | None = None,
) -> None:
    """Record one filesystem operation into the active collector."""
    metrics = _ACTIVE_METRICS.get()
    if metrics is None:
        return

    metrics.request_count += 1
    metrics.latency_s_total += max(float(duration_s), 0.0)
    metrics.bytes_read += max(int(bytes_read), 0)
    metrics.bytes_written += max(int(bytes_written), 0)
    if not success:
        metrics.error_count += 1
        if error is not None and _is_retryable_error(error):
            metrics.retryable_error_count += 1


class InstrumentedFile:
    """Proxy for file-like objects that records read/write bytes and latency."""

    def __init__(self, wrapped: Any):
        self._wrapped = wrapped

    def __enter__(self):
        entered = self._wrapped.__enter__()
        # Preserve context-manager semantics while keeping a single adapter instance.
        self._wrapped = entered
        return self

    def __exit__(self, exc_type, exc, tb):
        return self._wrapped.__exit__(exc_type, exc, tb)

    def read(self, *args, **kwargs):
        start = time.perf_counter()
        try:
            data = self._wrapped.read(*args, **kwargs)
            size = _payload_size(data)
            record_fs_call(duration_s=time.perf_counter() - start, success=True, bytes_read=size)
            return data
        except Exception as exc:
            record_fs_call(duration_s=time.perf_counter() - start, success=False, error=exc)
            raise

    def readline(self, *args, **kwargs):
        start = time.perf_counter()
        try:
            line = self._wrapped.readline(*args, **kwargs)
            size = _payload_size(line)
            record_fs_call(duration_s=time.perf_counter() - start, success=True, bytes_read=size)
            return line
        except Exception as exc:
            record_fs_call(duration_s=time.perf_counter() - start, success=False, error=exc)
            raise

    def readlines(self, *args, **kwargs):
        start = time.perf_counter()
        try:
            lines = self._wrapped.readlines(*args, **kwargs)
            size = sum(_payload_size(line) for line in lines)
            record_fs_call(duration_s=time.perf_counter() - start, success=True, bytes_read=size)
            return lines
        except Exception as exc:
            record_fs_call(duration_s=time.perf_counter() - start, success=False, error=exc)
            raise

    def write(self, data, *args, **kwargs):
        start = time.perf_counter()
        try:
            written = self._wrapped.write(data, *args, **kwargs)
            size = _payload_size(data)
            if size <= 0:
                size = int(written or 0)
            record_fs_call(duration_s=time.perf_counter() - start, success=True, bytes_written=size)
            return written
        except Exception as exc:
            record_fs_call(duration_s=time.perf_counter() - start, success=False, error=exc)
            raise

    def __iter__(self):
        return self

    def __next__(self):
        start = time.perf_counter()
        try:
            item = next(self._wrapped)
            size = _payload_size(item)
            record_fs_call(duration_s=time.perf_counter() - start, success=True, bytes_read=size)
            return item
        except StopIteration:
            raise
        except Exception as exc:
            record_fs_call(duration_s=time.perf_counter() - start, success=False, error=exc)
            raise

    def __getattr__(self, name: str):
        return getattr(self._wrapped, name)


def _payload_size(value: Any) -> int:
    if value is None:
        return 0
    if isinstance(value, (bytes, bytearray, memoryview, str)):
        return len(value)
    try:
        return len(value)
    except Exception:
        return 0


class InstrumentedFilesystem:
    """Filesystem proxy that records latency/errors for common operations."""

    _timed_methods: ClassVar[set[str]] = {
        "exists",
        "info",
        "ls",
        "find",
        "rm",
        "mkdir",
        "makedirs",
        "glob",
        "get",
        "put",
    }

    def __init__(self, wrapped: Any):
        self._wrapped = wrapped

    def open(self, *args, **kwargs):
        start = time.perf_counter()
        try:
            handle = self._wrapped.open(*args, **kwargs)
            record_fs_call(duration_s=time.perf_counter() - start, success=True)
            return InstrumentedFile(handle)
        except Exception as exc:
            record_fs_call(duration_s=time.perf_counter() - start, success=False, error=exc)
            raise

    def read_bytes(self, *args, **kwargs):
        start = time.perf_counter()
        try:
            data = self._wrapped.read_bytes(*args, **kwargs)
            record_fs_call(
                duration_s=time.perf_counter() - start,
                success=True,
                bytes_read=_payload_size(data),
            )
            return data
        except Exception as exc:
            record_fs_call(duration_s=time.perf_counter() - start, success=False, error=exc)
            raise

    def __getattr__(self, name: str):
        attr = getattr(self._wrapped, name)
        if not callable(attr) or name not in self._timed_methods:
            return attr

        def _wrapped_call(*args, **kwargs):
            start = time.perf_counter()
            try:
                result = attr(*args, **kwargs)
                record_fs_call(duration_s=time.perf_counter() - start, success=True)
                return result
            except Exception as exc:
                record_fs_call(duration_s=time.perf_counter() - start, success=False, error=exc)
                raise

        return _wrapped_call
