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

"""Zarr write-session context manager.

Encapsulates Dask scheduler validation, ``dask.config.set`` context,
``zarr.config`` async-concurrency settings, and write-lock acquisition
into a single reusable context manager.

Extracted from ``GenericZarrIngestor._process_batch()`` so that any
write strategy can share identical scheduler/lock policy.
"""

from __future__ import annotations

import contextlib
import threading
from typing import Any

from firecube.ingestor.api import ConfigurationError

_VALID_SCHEDULERS: frozenset[str] = frozenset(
    {
        "synchronous",
        "sync",
        "single-threaded",
        "threads",
        "threading",
        "processes",
        "multiprocessing",
        "distributed",
    }
)


class ZarrWriteContext:
    """Context manager for Zarr write sessions.

    Validates Dask scheduler configuration, sets ``zarr.config``
    async-concurrency, activates the appropriate ``dask.config`` scope,
    and acquires the caller-supplied write lock.

    Args:
        write_lock: A ``threading.Lock`` (or compatible) used to serialize
            Zarr writes.
        configured_scheduler: Explicit Dask scheduler name (e.g.
            ``"synchronous"``, ``"threads"``), or *None* to keep the ambient
            default.
        write_threads: Number of Dask worker threads for chunk-level
            parallelism.  ``0`` means "use the configured scheduler as-is".
        async_concurrency: Zarr async pipeline concurrency.  ``10`` is the
            default sentinel value; setting both *write_threads > 0* **and** a
            non-default concurrency raises ``ConfigurationError``.
    """

    def __init__(
        self,
        *,
        write_lock: threading.Lock,
        configured_scheduler: str | None = None,
        write_threads: int = 0,
        async_concurrency: int = 10,
    ) -> None:
        self._write_lock = write_lock
        self._configured_scheduler = configured_scheduler
        self._write_threads = write_threads
        self._async_concurrency = async_concurrency

        self._exit_stack: contextlib.ExitStack | None = None

    def _validate(self) -> None:
        if self._configured_scheduler and self._configured_scheduler not in _VALID_SCHEDULERS:
            raise ConfigurationError(
                f"Invalid dask_scheduler={self._configured_scheduler!r}. "
                f"Valid options: {sorted(_VALID_SCHEDULERS)}"
            )

        if self._write_threads > 0 and self._async_concurrency != 10:
            raise ConfigurationError(
                "dask_write_threads and zarr_async_concurrency are mutually exclusive. "
                "dask_write_threads uses dask threads for chunk-level parallelism; "
                "zarr_async_concurrency uses zarr's internal async pipeline. "
                "Using both causes contention and degrades performance. "
                "Pick one strategy."
            )

    def __enter__(self) -> ZarrWriteContext:
        self._validate()

        import dask.config as dask_config
        import zarr as _zarr

        effective_scheduler = self._configured_scheduler or dask_config.get(
            "scheduler", default=None
        )

        if self._write_threads > 0 or effective_scheduler == "synchronous":
            _zarr.config.set({"async.concurrency": 1})
        else:
            _zarr.config.set({"async.concurrency": self._async_concurrency})

        if self._write_threads > 0:
            dask_ctx: Any = dask_config.set(scheduler="threads", num_workers=self._write_threads)
        elif self._configured_scheduler:
            dask_ctx = dask_config.set(scheduler=self._configured_scheduler)
        else:
            dask_ctx = contextlib.nullcontext()

        stack = contextlib.ExitStack()
        self._exit_stack = stack
        try:
            stack.enter_context(self._write_lock)  # type: ignore[arg-type]
            stack.enter_context(dask_ctx)
        except BaseException:
            stack.close()
            self._exit_stack = None
            raise

        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> bool:
        if self._exit_stack is not None:
            self._exit_stack.__exit__(exc_type, exc_val, exc_tb)
            self._exit_stack = None
        return False
