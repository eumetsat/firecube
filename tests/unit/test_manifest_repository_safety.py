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
from pathlib import Path
from typing import Any

import pytest

from firecube.core.controlplane.repo import ManifestRepository
from firecube.core.controlplane.types import SpanCoverage
from firecube.core.errors import ControlPlaneCorruptionError, ManifestError
from tests.helpers.storage import make_test_binding


def _record_completed_span(
    repo: ManifestRepository,
    *,
    product: str,
    run_id: str,
    group: str,
    batch_id: str,
) -> str:
    repo.record_run_started(
        product=product,
        run_id=run_id,
        output_path=f"/tmp/{product}",
        output_format="zarr",
        size=0,
        meta={"plugin": "test"},
    )
    repo.record_span_event(
        product=product,
        run_id=run_id,
        batch_id=batch_id,
        group=group,
        status="active",
        coverage=SpanCoverage(
            group=group,
            arrays=[f"{group}/FWI"],
            time_index_ranges=[[0, 1]],
            time_min="2024-01-01T00:00:00",
            time_max="2024-01-02T00:00:00",
        ),
        meta={"plugin": "test", "group": group},
    )
    repo.record_run_terminal(
        product=product,
        run_id=run_id,
        output_path=f"/tmp/{product}",
        output_format="zarr",
        size=1,
        meta={"plugin": "test"},
        status="complete",
    )
    return f"span_{run_id}_{batch_id}_{group}"


def _record_nonterminal_span(
    repo: ManifestRepository,
    *,
    product: str,
    run_id: str,
    group: str,
    batch_id: str,
) -> None:
    repo.record_run_started(
        product=product,
        run_id=run_id,
        output_path=f"/tmp/{product}",
        output_format="zarr",
        size=0,
        meta={"plugin": "test"},
    )
    repo.record_span_event(
        product=product,
        run_id=run_id,
        batch_id=batch_id,
        group=group,
        status="active",
        coverage=SpanCoverage(
            group=group,
            arrays=[f"{group}/FWI"],
            time_index_ranges=[[0, 1]],
        ),
        meta={"plugin": "test", "group": group},
    )
    repo.record_span_event(
        product=product,
        run_id=run_id,
        batch_id=f"{batch_id}-extra",
        group=f"{group}-extra",
        status="active",
        coverage=SpanCoverage(
            group=f"{group}-extra",
            arrays=[f"{group}-extra/FWI"],
            time_index_ranges=[[2, 3]],
        ),
        meta={"plugin": "test", "group": f"{group}-extra"},
    )
    repo.close()


def _truncate_last_segment(
    temp_workspace, product: str, run_id: str, *, trim_bytes: int = 5
) -> None:
    run_dir = temp_workspace / product / ".firecube" / "runs" / run_id
    event_files = sorted(run_dir.glob("events-*.jsonl"))
    payload = event_files[-1].read_bytes()
    event_files[-1].write_bytes(payload[:-trim_bytes])


def test_mark_chunks_replaced_records_replacement_event_and_hides_key(temp_workspace):
    product = "product"
    repo = ManifestRepository(binding=make_test_binding(temp_workspace), workspace=temp_workspace)
    key = _record_completed_span(
        repo, product=product, run_id="run-001", group="F024", batch_id="b001"
    )

    result = repo.mark_chunks_replaced([key], product, 123.0)

    assert result["marked_count"] == 1
    active = repo.list_chunks(product=product, include_replaced=False)
    assert active == []
    history = repo.list_chunks(product=product, include_replaced=True)
    assert [chunk.status for chunk in history if chunk.key == key] == ["active", "replaced"]


def test_parse_manifest_returns_projected_history_from_control_root(temp_workspace):
    product = "product"
    repo = ManifestRepository(binding=make_test_binding(temp_workspace), workspace=temp_workspace)
    key = _record_completed_span(
        repo, product=product, run_id="run-001", group="F024", batch_id="b001"
    )

    chunks = list(repo.parse_manifest((temp_workspace / product / ".firecube").as_uri()))

    assert len(chunks) == 1
    assert chunks[0].key == key
    assert chunks[0].manifest_path.endswith("/.firecube")


def test_torn_tail_in_last_segment_of_non_terminal_run_is_recovered(temp_workspace):
    product = "product"
    repo = ManifestRepository(binding=make_test_binding(temp_workspace), workspace=temp_workspace)
    _record_nonterminal_span(repo, product=product, run_id="run-001", group="F024", batch_id="b001")
    _truncate_last_segment(temp_workspace, product, "run-001")

    history = repo.list_chunks(product=product, include_replaced=True)

    assert len(history) == 1
    assert history[0].meta is not None
    assert history[0].meta["group"] == "F024"


def test_abandon_run_appends_after_existing_segments_even_if_parts_is_stale(temp_workspace):
    product = "product"
    repo = ManifestRepository(binding=make_test_binding(temp_workspace), workspace=temp_workspace)
    _record_nonterminal_span(repo, product=product, run_id="run-001", group="F024", batch_id="b001")

    control_root = temp_workspace / product / ".firecube"
    run_dir = control_root / "runs" / "run-001"
    run_meta_path = run_dir / "run.json"
    run_meta = json.loads(run_meta_path.read_text(encoding="utf-8"))
    run_meta["parts"] = 0
    run_meta_path.write_text(json.dumps(run_meta), encoding="utf-8")

    repo.abandon_run(product=product, run_id="run-001", reason="worker-crash")

    event_files = sorted(path.name for path in run_dir.glob("events-*.jsonl"))
    assert event_files == ["events-00000.jsonl", "events-00001.jsonl", "events-00002.jsonl"]


def test_torn_tail_followed_by_abandon_terminal_segment_is_recovered(temp_workspace):
    product = "product"
    repo = ManifestRepository(binding=make_test_binding(temp_workspace), workspace=temp_workspace)
    _record_nonterminal_span(repo, product=product, run_id="run-001", group="F024", batch_id="b001")
    _truncate_last_segment(temp_workspace, product, "run-001")

    result = repo.abandon_run(product=product, run_id="run-001", reason="worker-crash")

    assert result["abandoned"] is True
    history = repo.list_chunks(product=product, include_replaced=True)
    assert len(history) == 1
    assert history[0].meta is not None
    assert history[0].meta["group"] == "F024"
    assert repo.list_runs(product=product)[0].status == "abandoned"
    assert repo.list_chunks(product=product) == []


def test_malformed_middle_line_fails_closed(temp_workspace):
    product = "product"
    repo = ManifestRepository(binding=make_test_binding(temp_workspace), workspace=temp_workspace)
    _record_completed_span(repo, product=product, run_id="run-001", group="F024", batch_id="b001")

    run_dir = temp_workspace / product / ".firecube" / "runs" / "run-001"
    event_file = sorted(run_dir.glob("events-*.jsonl"))[0]
    payload = event_file.read_text(encoding="utf-8").splitlines()
    payload[0] = "{not-json}"
    event_file.write_text("\n".join(payload) + "\n", encoding="utf-8")

    with pytest.raises(ControlPlaneCorruptionError):
        repo.list_chunks(product=product, include_replaced=True)


def test_missing_snapshot_falls_back_to_wal(temp_workspace):
    product = "product"
    repo = ManifestRepository(binding=make_test_binding(temp_workspace), workspace=temp_workspace)
    key = _record_completed_span(
        repo, product=product, run_id="run-001", group="F024", batch_id="b001"
    )
    repo.rebuild_snapshot(product)

    control_root = temp_workspace / product / ".firecube"
    latest = json.loads((control_root / "LATEST.json").read_text(encoding="utf-8"))
    Path(latest["snapshot_path"]).unlink()

    active = repo.list_chunks(product=product)

    assert len(active) == 1
    assert active[0].key == key


def test_invalid_snapshot_line_falls_back_to_wal(temp_workspace):
    product = "product"
    repo = ManifestRepository(binding=make_test_binding(temp_workspace), workspace=temp_workspace)
    key = _record_completed_span(
        repo, product=product, run_id="run-001", group="F024", batch_id="b001"
    )
    repo.rebuild_snapshot(product)

    control_root = temp_workspace / product / ".firecube"
    latest = json.loads((control_root / "LATEST.json").read_text(encoding="utf-8"))
    snapshot_path = Path(latest["snapshot_path"])
    snapshot_path.write_text("{not-json}\n", encoding="utf-8")

    active = repo.list_chunks(product=product)

    assert len(active) == 1
    assert active[0].key == key


def test_missing_snapshot_falls_back_to_wal_with_parallel_replay(temp_workspace):
    """With 2+ runs, snapshot fallback exercises the parallel WAL replay path."""
    product = "product"
    repo = ManifestRepository(binding=make_test_binding(temp_workspace), workspace=temp_workspace)
    key1 = _record_completed_span(
        repo, product=product, run_id="run-001", group="F024", batch_id="b001"
    )
    key2 = _record_completed_span(
        repo, product=product, run_id="run-002", group="F048", batch_id="b002"
    )
    repo.rebuild_snapshot(product)

    control_root = temp_workspace / product / ".firecube"
    latest = json.loads((control_root / "LATEST.json").read_text(encoding="utf-8"))
    Path(latest["snapshot_path"]).unlink()

    active = repo.list_chunks(product=product)

    assert len(active) == 2
    returned_keys = {c.key for c in active}
    assert key1 in returned_keys
    assert key2 in returned_keys


def test_missing_run_json_is_surfaced_as_orphan_and_blocks_rebuild(temp_workspace):
    product = "product"
    repo = ManifestRepository(binding=make_test_binding(temp_workspace), workspace=temp_workspace)
    _record_nonterminal_span(repo, product=product, run_id="run-001", group="F024", batch_id="b001")

    run_dir = temp_workspace / product / ".firecube" / "runs" / "run-001"
    (run_dir / "run.json").unlink()

    runs = repo.list_runs(product=product)
    assert len(runs) == 1
    assert runs[0].status == "orphaned"
    assert runs[0].error == "missing_run_meta"

    with pytest.raises(ManifestError, match="orphaned"):
        repo.rebuild_snapshot(product)


def test_unreadable_run_json_is_surfaced_as_orphan(temp_workspace):
    product = "product"
    repo = ManifestRepository(binding=make_test_binding(temp_workspace), workspace=temp_workspace)
    _record_nonterminal_span(repo, product=product, run_id="run-001", group="F024", batch_id="b001")

    run_dir = temp_workspace / product / ".firecube" / "runs" / "run-001"
    (run_dir / "run.json").write_text("{not-json}", encoding="utf-8")

    runs = repo.list_runs(product=product)
    assert len(runs) == 1
    assert runs[0].status == "orphaned"
    assert runs[0].error == "unreadable_run_meta"


def test_non_terminal_runs_block_rebuild_until_abandoned(temp_workspace):
    product = "product"
    repo = ManifestRepository(binding=make_test_binding(temp_workspace), workspace=temp_workspace)
    _record_nonterminal_span(repo, product=product, run_id="run-001", group="F024", batch_id="b001")

    with pytest.raises(ManifestError, match="non-terminal runs"):
        repo.rebuild_snapshot(product)

    repo.abandon_run(product=product, run_id="run-001", reason="stale")
    result = repo.rebuild_snapshot(product)

    assert result["records"] == 0


def test_no_auto_compaction_during_wal_writes(temp_workspace):
    product = "product"
    repo = ManifestRepository(binding=make_test_binding(temp_workspace), workspace=temp_workspace)
    _record_completed_span(repo, product=product, run_id="run-001", group="F024", batch_id="b001")

    control_root = temp_workspace / product / ".firecube"
    assert not (control_root / "LATEST.json").exists()

    repo.rebuild_snapshot(product)
    assert (control_root / "LATEST.json").exists()


def test_parallel_replay_propagates_corrupt_run_error(temp_workspace):
    product = "product"
    repo = ManifestRepository(binding=make_test_binding(temp_workspace), workspace=temp_workspace)
    _record_completed_span(repo, product=product, run_id="run-001", group="F024", batch_id="b001")
    _record_completed_span(repo, product=product, run_id="run-002", group="F048", batch_id="b002")

    run_dir = temp_workspace / product / ".firecube" / "runs" / "run-002"
    event_file = sorted(run_dir.glob("events-*.jsonl"))[0]
    payload = event_file.read_text(encoding="utf-8").splitlines()
    payload[0] = "{not-json}"
    event_file.write_text("\n".join(payload) + "\n", encoding="utf-8")

    with pytest.raises(ControlPlaneCorruptionError, match="Corrupt WAL event"):
        repo.list_chunks(product=product)


def test_parallel_replay_deterministic_key_projection_order(temp_workspace):
    """Later completed run wins when two runs produce the same span key.

    Verifies the invariant from _sorted_complete_runs() + _apply_events():
    runs are sorted by (completed_at, run_id) in ascending order, and
    _apply_events() does current[key] = record (last write wins).
    """
    import time as _time

    product = "product"
    repo = ManifestRepository(binding=make_test_binding(temp_workspace), workspace=temp_workspace)

    # Record 2 completed runs normally (they produce different keys)
    _record_completed_span(repo, product=product, run_id="run-001", group="F024", batch_id="b001")
    _record_completed_span(repo, product=product, run_id="run-002", group="F048", batch_id="b002")

    shared_key = "span_shared_conflict_key"
    now = _time.time()

    # Monkeypatch _read_run_events to return synthetic events with the SAME key
    original_read = repo._read_run_events

    def patched_read(product: str, run_entry: dict[str, Any]) -> list[dict[str, Any]]:
        run_id = run_entry.get("run_id", "")
        if run_id == "run-001":
            return [
                {
                    "event_type": "span",
                    "record": {
                        "key": shared_key,
                        "type": "span",
                        "size": 0,
                        "timestamp": now,
                        "status": "active",
                        "meta": {"version": 1, "group": "F024"},
                        "span": {
                            "arrays": ["F024/FWI"],
                            "time_index_ranges": [[0, 1]],
                            "timestamps_written": 2,
                            "aligned": True,
                        },
                        "schema_version": "v2",
                    },
                }
            ]
        elif run_id == "run-002":
            return [
                {
                    "event_type": "span",
                    "record": {
                        "key": shared_key,
                        "type": "span",
                        "size": 0,
                        "timestamp": now + 1,
                        "status": "active",
                        "meta": {"version": 2, "group": "F024"},
                        "span": {
                            "arrays": ["F024/FWI"],
                            "time_index_ranges": [[0, 1]],
                            "timestamps_written": 2,
                            "aligned": True,
                        },
                        "schema_version": "v2",
                    },
                }
            ]
        return original_read(product, run_entry)

    repo._read_run_events = patched_read

    chunks = repo.list_chunks(product=product)

    # Should have exactly 1 chunk for the shared key (later run wins)
    shared_chunks = [c for c in chunks if c.key == shared_key]
    assert len(shared_chunks) == 1, f"Expected 1 chunk for shared key, got {len(shared_chunks)}"
    assert shared_chunks[0].meta is not None
    assert shared_chunks[0].meta["version"] == 2, (
        f"Expected version=2 (later run wins), got {shared_chunks[0].meta}"
    )


def test_snapshot_cutoff_tie_does_not_miss_run(temp_workspace):
    """A run completing at exactly the snapshot cutoff timestamp must still be visible."""
    product = "product"
    repo = ManifestRepository(binding=make_test_binding(temp_workspace), workspace=temp_workspace)

    _record_completed_span(repo, product=product, run_id="run-001", group="F024", batch_id="b001")
    repo.rebuild_snapshot(product)

    control_root = temp_workspace / product / ".firecube"
    latest = json.loads((control_root / "LATEST.json").read_text(encoding="utf-8"))
    cutoff = latest["completed_before"]

    _record_completed_span(repo, product=product, run_id="run-002", group="F048", batch_id="b002")
    # Patch run-002 completed_at to exactly equal the snapshot cutoff
    run_dir = control_root / "runs" / "run-002"
    run_meta = json.loads((run_dir / "run.json").read_text(encoding="utf-8"))
    run_meta["completed_at"] = cutoff
    (run_dir / "run.json").write_text(json.dumps(run_meta), encoding="utf-8")

    chunks = repo.list_chunks(product=product)
    assert len(chunks) == 2, (
        f"Expected 2 chunks (snapshot + tied run), got {len(chunks)}: {[c.key for c in chunks]}"
    )


def test_custom_stale_threshold_persisted_to_run_json(temp_workspace):
    """Non-default run_stale_threshold_s must be written to run.json, not the default."""
    product = "product"
    custom_threshold = 7200
    repo = ManifestRepository(
        binding=make_test_binding(temp_workspace),
        workspace=temp_workspace,
        run_stale_threshold_s=custom_threshold,
    )
    _record_completed_span(repo, product=product, run_id="run-001", group="F024", batch_id="b001")

    run_dir = temp_workspace / product / ".firecube" / "runs" / "run-001"
    run_meta = json.loads((run_dir / "run.json").read_text(encoding="utf-8"))
    assert run_meta["run_stale_threshold_s"] == custom_threshold, (
        f"Expected {custom_threshold}, got {run_meta['run_stale_threshold_s']}"
    )
