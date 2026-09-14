# Copyright 2025-2026 EUMETSAT
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Guard the real 0.1.5 WAL golden fixture against silent format drift.

The companion backward-compat suite (``test_wal_schema_backward_compat.py``)
exercises the fixture through the reader and projection engine to prove old
segments still parse. This file guards a different invariant: that the fixture
bytes themselves remain a real firecube==0.1.5 capture and never regain the
optional fields added by later changes — so the compat tests keep testing what
they claim to test.

If a maintainer regenerates the fixture with a newer firecube release, this
test fails and forces the maintainer to either capture from real 0.1.5 again or
justify the drift explicitly.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

pytestmark = pytest.mark.architecture

_FIXTURE = Path(__file__).parent.parent / "fixtures" / "wal_v0.1.5" / "events_sample.jsonl"

# Optional fields added by post-0.1.5 changes. A real 0.1.5 segment must NOT
# carry them; if they appear, the fixture has been regenerated from a newer
# firecube and no longer proves backward compatibility.
_FORBIDDEN_SPAN_FIELDS = ("write_strategy", "timestamps_skipped", "chunk_len_used")
_FORBIDDEN_META_FIELDS = ("overwrites_index_ranges",)


def _iter_events() -> list[dict]:
    text = _FIXTURE.read_text(encoding="utf-8")
    return [json.loads(line) for line in text.splitlines() if line.strip()]


def test_real_015_segment_lacks_new_fields() -> None:
    """Every event in the real 0.1.5 fixture omits post-0.1.5 optional fields."""
    events = _iter_events()
    assert events, f"Fixture {_FIXTURE} is empty"

    for event in events:
        event_id = event.get("event_id", "<unknown>")
        record = event.get("record") or {}
        span_payload = record.get("span") or {}
        record_meta = record.get("meta") or {}
        top_meta = event.get("meta") or {}

        for field in _FORBIDDEN_SPAN_FIELDS:
            assert field not in span_payload, (
                f"Event {event_id}: forbidden post-0.1.5 field '{field}' found in "
                f"record.span. Fixture has drifted from real 0.1.5 capture."
            )

        for field in _FORBIDDEN_META_FIELDS:
            assert field not in record_meta, (
                f"Event {event_id}: forbidden post-0.1.5 field '{field}' found in "
                f"record.meta. Fixture has drifted from real 0.1.5 capture."
            )
            assert field not in top_meta, (
                f"Event {event_id}: forbidden post-0.1.5 field '{field}' found in "
                f"event.meta. Fixture has drifted from real 0.1.5 capture."
            )


def test_fixture_is_real_015_capture() -> None:
    """Fixture retains the real 0.1.5 capture identifiers.

    The concatenated segment came from a real 0.1.5 ingestion run: store
    ``C1_ts_plain.zarr``, run
    ``precip_daily-host-8ad9d4a2db284ceeaf7b186ba3e61320``. Losing these
    identifiers means the fixture was replaced with something synthetic and
    the backward-compat guarantee is only as strong as the author's intuition.
    """
    events = _iter_events()

    assert len(events) == 5, f"Expected 5 events in real 0.1.5 capture, got {len(events)}"

    for event in events:
        assert event.get("product") == "precip_daily", (
            f"Event {event.get('event_id')} product drifted from real 0.1.5 capture"
        )
        assert event.get("run_id") == "precip_daily-host-8ad9d4a2db284ceeaf7b186ba3e61320", (
            f"Event {event.get('event_id')} run_id drifted from real 0.1.5 capture"
        )
        assert event.get("schema_version") == "v2", (
            f"Event {event.get('event_id')} schema_version drifted from v2"
        )
