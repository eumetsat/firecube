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

"""In-memory resume cache for Zarr append operations.

This cache is a performance optimization for `append_time_groups()` to avoid
re-reading Zarr metadata (shape/chunks) repeatedly when resuming.

It is process-local and intentionally best-effort: correctness must not depend
on cache hits.
"""

from __future__ import annotations

import threading


class ResumeCacheEntry:
    __slots__ = ("chunk_len", "cursor", "preexisting_values", "state_initialized")

    def __init__(
        self,
        *,
        cursor: int,
        chunk_len: int | None,
        state_initialized: bool,
        preexisting_values: frozenset[object] | None = None,
    ) -> None:
        self.cursor = int(cursor)
        self.chunk_len = int(chunk_len) if chunk_len is not None else None
        self.state_initialized = bool(state_initialized)
        self.preexisting_values = preexisting_values


_RESUME_CACHE: dict[tuple[str, str, str], ResumeCacheEntry] = {}
_RESUME_CACHE_LOCK = threading.Lock()
_RESUME_CACHE_MAXSIZE = 512


def _evict_cache_if_needed() -> None:
    """Evict the oldest entry when the cache exceeds _RESUME_CACHE_MAXSIZE.

    Must be called with _RESUME_CACHE_LOCK held.
    Insertion order is preserved in Python 3.7+ dicts, so `next(iter(...))` gives
    the oldest key.
    """
    while len(_RESUME_CACHE) >= _RESUME_CACHE_MAXSIZE:
        oldest = next(iter(_RESUME_CACHE))
        del _RESUME_CACHE[oldest]


def get_resume_cache_entry(key: tuple[str, str, str]) -> ResumeCacheEntry | None:
    with _RESUME_CACHE_LOCK:
        return _RESUME_CACHE.get(key)


def put_resume_cache_entry(key: tuple[str, str, str], entry: ResumeCacheEntry) -> None:
    with _RESUME_CACHE_LOCK:
        _evict_cache_if_needed()
        _RESUME_CACHE[key] = entry


def clear_resume_cache(run_id: str | None = None) -> int:
    """Remove entries from the resume cache.

    Args:
        run_id: If given, only entries whose cache key contains this run_id are
            removed. If ``None``, the entire cache is cleared.

    Returns:
        Number of entries removed.
    """
    with _RESUME_CACHE_LOCK:
        if run_id is None:
            count = len(_RESUME_CACHE)
            _RESUME_CACHE.clear()
            return count
        keys_to_delete = [k for k in _RESUME_CACHE if run_id in k]
        for k in keys_to_delete:
            del _RESUME_CACHE[k]
        return len(keys_to_delete)
