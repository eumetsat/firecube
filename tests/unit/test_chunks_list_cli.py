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

import json

from click.testing import CliRunner

from firecube.cli.chunks._common import parse_time_range
from firecube.cli.chunks._list import list_cmd
from firecube.core.controlplane.types import ChunkInfo


class _FakeManager:
    def __init__(self, entries: list[ChunkInfo]):
        self._entries = entries
        self.calls: list[dict] = []

    def list_chunks(self, **kwargs):  # pragma: no cover - exercised via click command
        self.calls.append(dict(kwargs))
        return list(self._entries)


def _span_chunk() -> ChunkInfo:
    return ChunkInfo(
        key="span_test_product_run_batch_F120",
        product="TEST_PRODUCT.zarr",
        chunk_type="span",
        size=0,
        timestamp=1735689600.0,
        manifest_path="s3://firecube/TEST_PRODUCT.zarr/.firecube",
        meta={"group": "F120"},
        record={
            "span": {
                "arrays": ["F120/FWI"],
                "time_index_ranges": [[0, 39], [40, 79]],
                "timestamps_written": 80,
                "aligned": True,
                "state_array": "F120/firecube_timestamp_state",
                "state_deleted_value": 2,
            }
        },
    )


def test_chunks_list_json_omits_span_by_default(monkeypatch):
    runner = CliRunner()
    monkeypatch.setattr(
        "firecube.cli.chunks._list.resolve_manager",
        lambda *args, **kwargs: _FakeManager([_span_chunk()]),
    )

    result = runner.invoke(list_cmd, ["--format", "json"], obj={})

    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert len(payload) == 1
    assert "span" not in payload[0]


def test_chunks_list_json_includes_span_with_flag(monkeypatch):
    runner = CliRunner()
    monkeypatch.setattr(
        "firecube.cli.chunks._list.resolve_manager",
        lambda *args, **kwargs: _FakeManager([_span_chunk()]),
    )

    result = runner.invoke(list_cmd, ["--format", "json", "--include-span"], obj={})

    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert len(payload) == 1
    assert payload[0]["span"]["time_index_ranges"] == [[0, 39], [40, 79]]


def test_chunks_list_table_includes_span_column_with_flag(monkeypatch):
    runner = CliRunner()
    monkeypatch.setattr(
        "firecube.cli.chunks._list.resolve_manager",
        lambda *args, **kwargs: _FakeManager([_span_chunk()]),
    )

    result = runner.invoke(list_cmd, ["--include-span"], obj={})

    assert result.exit_code == 0
    assert "Span" in result.output
    assert "80" in result.output


def test_chunks_list_help_shows_time_range():
    runner = CliRunner()

    result = runner.invoke(list_cmd, ["--help"], obj={})

    assert result.exit_code == 0
    assert "--time-range" in result.output


def test_chunks_list_passes_time_range_to_manager(monkeypatch):
    runner = CliRunner()
    manager = _FakeManager([_span_chunk()])
    monkeypatch.setattr(
        "firecube.cli.chunks._list.resolve_manager", lambda *args, **kwargs: manager
    )

    result = runner.invoke(list_cmd, ["--time-range", "2024-01-01:2024-01-31"], obj={})

    assert result.exit_code == 0
    assert manager.calls[0]["time_overlaps"] == ("2024-01-01", "2024-01-31")


def test_parse_time_range_accepts_iso_datetimes_with_colons():
    assert parse_time_range("2024-03-15T00:00:00:2024-03-15T23:59:59") == (
        "2024-03-15T00:00:00",
        "2024-03-15T23:59:59",
    )


def test_chunks_list_json_calls_span_payload_once_per_row(monkeypatch):
    class _CountingChunk:
        def __init__(self, inner: ChunkInfo) -> None:
            self._inner = inner
            self.record_reads = 0

        @property
        def record(self):
            self.record_reads += 1
            return self._inner.record

        def __getattr__(self, name):
            return getattr(self._inner, name)

    counting = _CountingChunk(_span_chunk())
    monkeypatch.setattr(
        "firecube.cli.chunks._list.resolve_manager",
        lambda *args, **kwargs: _FakeManager([counting]),  # type: ignore[list-item]
    )

    result = CliRunner().invoke(list_cmd, ["--format", "json", "--include-span"], obj={})

    assert result.exit_code == 0, result.output
    assert counting.record_reads == 1, (
        f"_span_payload should read chunk.record once per row, got {counting.record_reads}"
    )


def test_chunks_list_include_replaced_requests_history_from_manager(monkeypatch):
    """The flag is forwarded to the control plane so failed-run spans are listed."""
    seen: dict[str, object] = {}

    class _RecordingManager(_FakeManager):
        def list_chunks(self, **kwargs):
            seen.update(kwargs)
            return super().list_chunks(**kwargs)

    runner = CliRunner()
    monkeypatch.setattr(
        "firecube.cli.chunks._list.resolve_manager",
        lambda *args, **kwargs: _RecordingManager([_span_chunk()]),
    )

    result = runner.invoke(list_cmd, ["--format", "json", "--include-replaced"], obj={})

    assert result.exit_code == 0, result.output
    assert seen.get("include_replaced") is True

    result = runner.invoke(list_cmd, ["--format", "json"], obj={})
    assert result.exit_code == 0, result.output
    assert seen.get("include_replaced") is False
