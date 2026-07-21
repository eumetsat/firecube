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

import threading
from unittest.mock import MagicMock, patch

import pytest

from firecube.ingestor.api import ConfigurationError
from firecube.ingestor.runtime.zarr.write_context import _VALID_SCHEDULERS, ZarrWriteContext


class TestZarrWriteContextValidation:
    def test_rejects_invalid_scheduler(self):
        ctx = ZarrWriteContext(
            write_lock=threading.Lock(),
            configured_scheduler="bogus",
        )
        with pytest.raises(ConfigurationError, match="Invalid dask_scheduler='bogus'"):
            ctx.__enter__()

    def test_rejects_mutually_exclusive_threads_and_async(self):
        ctx = ZarrWriteContext(
            write_lock=threading.Lock(),
            write_threads=4,
            async_concurrency=20,
        )
        with pytest.raises(ConfigurationError, match="mutually exclusive"):
            ctx.__enter__()

    def test_accepts_write_threads_with_default_async(self):
        lock = threading.Lock()
        ctx = ZarrWriteContext(
            write_lock=lock,
            write_threads=4,
            async_concurrency=10,
        )
        with ctx:
            assert not lock.acquire(blocking=False), "lock should be held"
        assert lock.acquire(blocking=False), "lock should be released"
        lock.release()

    @pytest.mark.parametrize("scheduler", sorted(_VALID_SCHEDULERS))
    def test_accepts_all_valid_schedulers(self, scheduler: str):
        ctx = ZarrWriteContext(
            write_lock=threading.Lock(),
            configured_scheduler=scheduler,
        )
        with ctx:
            pass


class TestZarrWriteContextLocking:
    def test_acquires_and_releases_lock(self):
        lock = threading.Lock()
        ctx = ZarrWriteContext(write_lock=lock)
        with ctx:
            assert not lock.acquire(blocking=False)
        assert lock.acquire(blocking=False)
        lock.release()

    def test_releases_lock_on_exception(self):
        lock = threading.Lock()
        ctx = ZarrWriteContext(write_lock=lock)
        with pytest.raises(RuntimeError, match="boom"), ctx:
            raise RuntimeError("boom")
        assert lock.acquire(blocking=False)
        lock.release()


class TestZarrWriteContextDaskConfig:
    def test_write_threads_sets_dask_threads_scheduler(self):
        with patch("dask.config.set") as mock_set, patch("dask.config.get", return_value=None):
            mock_cm = MagicMock()
            mock_cm.__enter__ = MagicMock(return_value=mock_cm)
            mock_cm.__exit__ = MagicMock(return_value=False)
            mock_set.return_value = mock_cm

            ctx = ZarrWriteContext(
                write_lock=threading.Lock(),
                write_threads=8,
            )
            with ctx:
                pass

            mock_set.assert_called_with(scheduler="threads", num_workers=8)

    def test_configured_scheduler_passed_to_dask(self):
        with patch("dask.config.set") as mock_set, patch("dask.config.get", return_value=None):
            mock_cm = MagicMock()
            mock_cm.__enter__ = MagicMock(return_value=mock_cm)
            mock_cm.__exit__ = MagicMock(return_value=False)
            mock_set.return_value = mock_cm

            ctx = ZarrWriteContext(
                write_lock=threading.Lock(),
                configured_scheduler="synchronous",
            )
            with ctx:
                pass

            mock_set.assert_called_with(scheduler="synchronous")

    def test_no_scheduler_uses_nullcontext(self):
        ctx = ZarrWriteContext(write_lock=threading.Lock())
        with ctx:
            pass


class TestZarrWriteContextZarrConfig:
    def test_synchronous_forces_async_concurrency_1(self):
        with patch("zarr.config.set") as mock_zarr_set:
            ctx = ZarrWriteContext(
                write_lock=threading.Lock(),
                configured_scheduler="synchronous",
            )
            with ctx:
                pass
            mock_zarr_set.assert_called_with({"async.concurrency": 1})

    def test_write_threads_forces_async_concurrency_1(self):
        with patch("zarr.config.set") as mock_zarr_set:
            ctx = ZarrWriteContext(
                write_lock=threading.Lock(),
                write_threads=4,
            )
            with ctx:
                pass
            mock_zarr_set.assert_called_with({"async.concurrency": 1})

    def test_default_uses_configured_async_concurrency(self):
        with patch("zarr.config.set") as mock_zarr_set:
            ctx = ZarrWriteContext(
                write_lock=threading.Lock(),
                async_concurrency=32,
            )
            with ctx:
                pass
            mock_zarr_set.assert_called_with({"async.concurrency": 32})
