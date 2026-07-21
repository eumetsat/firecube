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

import pytest

from firecube.ingestor.runtime.parallel_run_id import derive_pod_run_id

pytestmark = pytest.mark.unit


def test_slot_group_with_slash_encoded() -> None:
    assert (
        derive_pod_run_id("base", 0, 100, "multires/0.5deg")
        == "base__group=multires%2F0.5deg__slot=0-100"
    )


def test_slot_group_with_equals_encoded() -> None:
    run_id = derive_pod_run_id("base", 0, 100, "key=value")

    assert run_id == "base__group=key%3Dvalue__slot=0-100"
    assert "%3D" in run_id


def test_slot_group_with_unicode_encoded() -> None:
    assert derive_pod_run_id("base", 0, 100, "ñoño") == "base__group=%C3%B1o%C3%B1o__slot=0-100"


def test_slot_group_simple_unchanged() -> None:
    assert derive_pod_run_id("base", 0, 100, "group_a") == "base__group=group_a__slot=0-100"


def test_slot_group_none_format_unchanged() -> None:
    assert derive_pod_run_id("base", 0, 100, None) == "base__slot=0-100"


def test_encoded_run_id_is_path_segment_safe() -> None:
    run_id = derive_pod_run_id("base", 0, 100, "grp/sub?#")

    assert "/" not in run_id
    assert "?" not in run_id
    assert "#" not in run_id
    assert "grp%2Fsub%3F%23" in run_id
