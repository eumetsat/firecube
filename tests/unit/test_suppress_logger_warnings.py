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

"""Unit tests for ``firecube.cli.zarr._slots._suppress_logger_warnings``.

The suppressor is used by CLI commands that emit JSON on stdout, where a
stray WARNING would corrupt the parseable output. The implementation
uses a per-context ``logging.Filter`` that only blocks WARNING and above
instead of ``logger.disabled = True``, which races between threads and
hides every level. These tests protect that behavior.
"""

from __future__ import annotations

import logging
import threading

import pytest

from firecube.cli.zarr._slots import _suppress_logger_warnings

pytestmark = pytest.mark.unit


def _target_logger_name(request: pytest.FixtureRequest) -> str:
    return f"firecube.tests.suppress.{request.node.name}"


def test_suppress_warnings_blocks_target_logger_warnings(
    request: pytest.FixtureRequest, caplog: pytest.LogCaptureFixture
) -> None:
    target_name = _target_logger_name(request)
    target = logging.getLogger(target_name)
    target.setLevel(logging.DEBUG)
    caplog.set_level(logging.DEBUG, logger=target_name)

    with _suppress_logger_warnings(target_name):
        target.warning("blocked-warning")
        target.error("blocked-error")

    for record in caplog.records:
        assert record.getMessage() not in {"blocked-warning", "blocked-error"}, record.getMessage()


def test_suppress_warnings_blocks_descendant_warnings(
    request: pytest.FixtureRequest, caplog: pytest.LogCaptureFixture
) -> None:
    target_name = _target_logger_name(request)
    child_name = f"{target_name}.child"
    target = logging.getLogger(target_name)
    child = logging.getLogger(child_name)
    target.setLevel(logging.DEBUG)
    child.setLevel(logging.DEBUG)
    caplog.set_level(logging.DEBUG, logger=child_name)

    with _suppress_logger_warnings(target_name):
        child.warning("descendant-warning")

    for record in caplog.records:
        assert record.getMessage() != "descendant-warning", record


def test_suppress_warnings_preserves_debug_and_info(
    request: pytest.FixtureRequest, caplog: pytest.LogCaptureFixture
) -> None:
    target_name = _target_logger_name(request)
    target = logging.getLogger(target_name)
    target.setLevel(logging.DEBUG)
    caplog.set_level(logging.DEBUG, logger=target_name)

    with _suppress_logger_warnings(target_name):
        target.debug("kept-debug")
        target.info("kept-info")

    messages = [record.getMessage() for record in caplog.records]
    assert "kept-debug" in messages, messages
    assert "kept-info" in messages, messages


def test_suppress_warnings_no_leak_after_context_exit(
    request: pytest.FixtureRequest, caplog: pytest.LogCaptureFixture
) -> None:
    target_name = _target_logger_name(request)
    target = logging.getLogger(target_name)
    target.setLevel(logging.DEBUG)
    caplog.set_level(logging.DEBUG, logger=target_name)
    filters_before = list(target.filters)

    with _suppress_logger_warnings(target_name):
        pass

    assert list(target.filters) == filters_before, (
        f"filter must be removed on exit; before={filters_before!r} after={list(target.filters)!r}"
    )
    target.warning("after-context-warning")
    messages = [record.getMessage() for record in caplog.records]
    assert "after-context-warning" in messages, messages


def test_suppress_warnings_thread_safe(
    request: pytest.FixtureRequest, caplog: pytest.LogCaptureFixture
) -> None:
    target_name = _target_logger_name(request)
    target = logging.getLogger(target_name)
    target.setLevel(logging.DEBUG)
    caplog.set_level(logging.DEBUG, logger=target_name)

    iterations = 40
    barrier = threading.Barrier(4)
    errors: list[BaseException] = []
    errors_lock = threading.Lock()

    def suppress_and_log() -> None:
        barrier.wait(timeout=5)
        try:
            for _ in range(iterations):
                with _suppress_logger_warnings(target_name):
                    target.warning("inside-suppress")
        except BaseException as exc:  # pragma: no cover - reported below
            with errors_lock:
                errors.append(exc)

    def free_warn() -> None:
        barrier.wait(timeout=5)
        try:
            for _ in range(iterations):
                target.warning("outside-suppress")
        except BaseException as exc:  # pragma: no cover
            with errors_lock:
                errors.append(exc)

    threads = [
        threading.Thread(target=suppress_and_log, name="suppressor-1"),
        threading.Thread(target=suppress_and_log, name="suppressor-2"),
        threading.Thread(target=free_warn, name="free-warner-1"),
        threading.Thread(target=free_warn, name="free-warner-2"),
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=15)

    assert all(not thread.is_alive() for thread in threads), "threads must complete"
    assert errors == [], errors
    assert target.filters == [], (
        "no _WarnFilter must leak on the target logger after all contexts exit; "
        f"leaked filters: {target.filters!r}"
    )
