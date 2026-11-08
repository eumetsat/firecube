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

from collections.abc import Callable
from typing import Any

import dask.config
import pytest
import zarr

from firecube.ingestor.runtime.zarr.write_context import _VALID_SCHEDULERS, ZarrWriteContext
from firecube.sdk import ConfigurationError
from tests.helpers.rows import table_row

_NO_VALUE = object()
_SCHEDULER_ROW_IDS = (
    "distributed",
    "multiprocessing",
    "processes",
    "single-threaded",
    "sync",
    "synchronous",
    "threading",
    "threads",
)


def _ambient_dask_scheduler() -> Any:
    return dask.config.get("scheduler", default=_NO_VALUE)


def test_zarr_write_context_validation(subtests, tmp_path, monkeypatch) -> None:
    """Invalid scheduler names and threads plus async concurrency are refused on entry."""
    rejected: tuple[tuple[str, dict[str, Any], str], ...] = (
        (
            "test_rejects_invalid_scheduler",
            {"configured_scheduler": "bogus"},
            "Invalid dask_scheduler='bogus'",
        ),
        (
            "test_rejects_mutually_exclusive_threads_and_async",
            {"write_threads": 4, "async_concurrency": 20},
            "mutually exclusive",
        ),
    )
    for row_id, kwargs, message in rejected:
        with (
            table_row(subtests, row_id, tmp_path, monkeypatch),
            pytest.raises(ConfigurationError, match=message),
            ZarrWriteContext(**kwargs),
        ):
            pytest.fail("the context must refuse before the body runs")

    assert set(_SCHEDULER_ROW_IDS) == _VALID_SCHEDULERS, "a scheduler was added or removed"
    accepted: list[tuple[str, dict[str, Any]]] = [
        (
            "test_accepts_write_threads_with_default_async",
            {"write_threads": 4, "async_concurrency": 10},
        ),
        *((name, {"configured_scheduler": name}) for name in _SCHEDULER_ROW_IDS),
    ]
    for row_id, kwargs in accepted:
        with table_row(subtests, row_id, tmp_path, monkeypatch):
            with zarr.config.set({"async.concurrency": 10}), ZarrWriteContext(**kwargs):
                if "configured_scheduler" in kwargs:
                    assert dask.config.get("scheduler") == kwargs["configured_scheduler"]
            assert zarr.config.get("async.concurrency") == 10


def _threads_scheduler_with_8_workers() -> None:
    assert dask.config.get("scheduler") == "threads"
    assert dask.config.get("num_workers") == 8


def _synchronous_scheduler() -> None:
    assert dask.config.get("scheduler") == "synchronous"


def test_zarr_write_context_dask_config(subtests, tmp_path, monkeypatch) -> None:
    """The context activates the requested Dask scope and restores the ambient one."""
    ambient = _ambient_dask_scheduler()

    def ambient_scheduler_untouched() -> None:
        assert _ambient_dask_scheduler() == ambient

    rows: tuple[tuple[str, dict[str, Any], Callable[[], None]], ...] = (
        (
            "test_write_threads_sets_dask_threads_scheduler",
            {"write_threads": 8},
            _threads_scheduler_with_8_workers,
        ),
        (
            "test_configured_scheduler_passed_to_dask",
            {"configured_scheduler": "synchronous"},
            _synchronous_scheduler,
        ),
        ("test_no_scheduler_uses_nullcontext", {}, ambient_scheduler_untouched),
    )
    for row_id, kwargs, check in rows:
        with table_row(subtests, row_id, tmp_path, monkeypatch):
            with zarr.config.set({"async.concurrency": 10}), ZarrWriteContext(**kwargs):
                check()
            ambient_scheduler_untouched()


def test_zarr_write_context_zarr_config(subtests, tmp_path, monkeypatch) -> None:
    """Zarr async concurrency and write_empty_chunks inside the context follow the options."""
    rows: tuple[tuple[str, dict[str, Any], int], ...] = (
        ("test_synchronous_forces_async_concurrency_1", {"configured_scheduler": "synchronous"}, 1),
        ("test_write_threads_forces_async_concurrency_1", {"write_threads": 4}, 1),
        ("test_default_uses_configured_async_concurrency", {"async_concurrency": 32}, 32),
    )
    for row_id, kwargs, concurrency in rows:
        with (
            table_row(subtests, row_id, tmp_path, monkeypatch),
            zarr.config.set({"array.write_empty_chunks": True, "async.concurrency": 10}),
        ):
            with ZarrWriteContext(**kwargs):
                assert zarr.config.get("async.concurrency") == concurrency
                assert zarr.config.get("array.write_empty_chunks") is False
            assert zarr.config.get("array.write_empty_chunks") is True
