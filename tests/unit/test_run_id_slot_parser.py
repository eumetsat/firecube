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

import pytest

from firecube.core.controlplane.repo_utils import parse_pod_run_id_slot
from firecube.ingestor.runtime.parallel_run_id import derive_pod_run_id


@pytest.mark.parametrize(
    ("base_id", "slot_start", "slot_end", "slot_group", "expected"),
    [
        pytest.param(
            "base-run-id",
            526080,
            526128,
            "SEVIRI_L15",
            ((526080, 526128), "SEVIRI_L15"),
            id="with_group",
        ),
        pytest.param(
            "base-run-id",
            100,
            200,
            None,
            ((100, 200), None),
            id="without_group",
        ),
        pytest.param(
            "base-run-id",
            0,
            1,
            "X",
            ((0, 1), "X"),
            id="boundary",
        ),
    ],
)
def test_parse_pod_run_id_slot_round_trip(
    base_id: str,
    slot_start: int,
    slot_end: int,
    slot_group: str | None,
    expected: tuple[tuple[int, int] | None, str | None],
) -> None:
    run_id = derive_pod_run_id(base_id, slot_start, slot_end, slot_group)

    assert parse_pod_run_id_slot(run_id) == expected


@pytest.mark.parametrize(
    ("run_id", "expected"),
    [
        pytest.param("plain-run-id-no-suffix", (None, None), id="no_suffix"),
        pytest.param("...__slot=garbage", (None, None), id="garbage_slot"),
        pytest.param("...__slot=10-", (None, None), id="slot_missing_end"),
        pytest.param("...__slot=-10", (None, None), id="slot_missing_start"),
        pytest.param("...__group=__slot=1-2", (None, None), id="empty_group"),
        pytest.param("", (None, None), id="empty_string"),
    ],
)
def test_parse_pod_run_id_slot_malformed(
    run_id: str,
    expected: tuple[tuple[int, int] | None, str | None],
) -> None:
    assert parse_pod_run_id_slot(run_id) == expected
