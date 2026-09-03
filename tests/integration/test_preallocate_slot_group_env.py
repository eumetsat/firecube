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

"""Group-scoped preallocate window + env round-trip.

``firecube zarr slots`` emits per-shard ``env`` blocks that pin each shard
to one group via ``FIRECUBE_SLOT_GROUP``. Historically the preallocate
command read the env-derived slot range but dropped the group name, so the
window silently narrowed *every* group's coord array to the shard's slice.
For a multi-group plugin that meant a shard "owned" by ``group_a`` would
also carve a foreign window out of ``group_b``, leaving 90% of ``group_b``
as NaT after the first shard ran and blocking any co-scheduled ``group_b``
shard from advancing past its per-group parallel gate.

These tests lock in that the window applies ONLY to the named group and
that mis-typed / unbounded / absent group names fail loudly at the CLI
boundary instead of silently narrowing an unrelated group.
"""

from __future__ import annotations

import importlib
import json
from collections.abc import Iterator
from pathlib import Path
from typing import Any, cast

import numpy as np
import pytest
import zarr
from click.testing import CliRunner

from firecube.cli.main import cli
from firecube.ingestor.registry import loader as _loader

pytestmark = pytest.mark.integration

_PLUGIN = "multi_group_capable_test_plugin"
_PRODUCT = "multi_group_capable_test_product"
_SLOT_COUNT = 1000


@pytest.fixture(autouse=True)
def reset_plugin_registry() -> Iterator[None]:
    original_loaded = _loader._LOADED
    original_registry = dict(_loader.AVAILABLE_INGESTORS)
    _loader._LOADED = False
    _loader.AVAILABLE_INGESTORS.clear()
    importlib.reload(importlib.import_module(_PLUGIN))
    yield
    _loader._LOADED = original_loaded
    _loader.AVAILABLE_INGESTORS.clear()
    _loader.AVAILABLE_INGESTORS.update(original_registry)


def _clear_slot_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for key in (
        "FIRECUBE_SLOT_START",
        "FIRECUBE_SLOT_END",
        "FIRECUBE_SLOT_GROUP",
        "FIRECUBE_SLOT_SIZE",
        "JOB_COMPLETION_INDEX",
    ):
        monkeypatch.delenv(key, raising=False)


def _preallocate_args(target: Path) -> list[str]:
    return [
        "zarr",
        "preallocate",
        _PLUGIN,
        "--target",
        f"file://{target}",
        "--product-name",
        _PRODUCT,
        "--storage-type",
        "local",
        "--storage-driver",
        "fsspec",
        "--write-mode",
        "direct",
        "--option",
        "no_progress=true",
    ]


def _slots_args(target: Path) -> list[str]:
    return [
        "zarr",
        "slots",
        _PLUGIN,
        "--target",
        f"file://{target}",
        "--product-name",
        _PRODUCT,
        "--storage-type",
        "local",
        "--storage-driver",
        "fsspec",
        "--write-mode",
        "direct",
    ]


def _read_timestamps(target: Path, group: str, coord: str = "timestamp") -> np.ndarray:
    root = zarr.open_group(store=str(target), mode="r", zarr_format=3)
    arr = cast(Any, root[f"{group}/{coord}"])
    return np.asarray(arr[:])


def test_env_slot_group_scopes_window_to_named_group_only(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A ``group_a``-scoped shard must NOT narrow ``group_b``'s coord array.

    Regression guard: the historic bug windowed every group to
    ``[FIRECUBE_SLOT_START, FIRECUBE_SLOT_END)`` regardless of
    ``FIRECUBE_SLOT_GROUP``.
    """
    _clear_slot_env(monkeypatch)
    monkeypatch.setenv("FIRECUBE_SLOT_START", "0")
    monkeypatch.setenv("FIRECUBE_SLOT_END", "200")
    monkeypatch.setenv("FIRECUBE_SLOT_GROUP", "group_a")

    target = tmp_path / "slot-group-env.zarr"
    result = CliRunner().invoke(cli, _preallocate_args(target))
    assert result.exit_code == 0, (
        f"preallocate must succeed with a valid group-scoped env; "
        f"output={result.output!r}, exception={result.exception!r}"
    )

    ts_a = _read_timestamps(target, "group_a")
    ts_b = _read_timestamps(target, "group_b")

    assert ts_a.shape == (_SLOT_COUNT,)
    assert ts_b.shape == (_SLOT_COUNT,)

    assert not np.any(np.isnat(ts_a[:200])), (
        "group_a slots [0, 200) must be materialized by the shard window"
    )
    assert np.all(np.isnat(ts_a[200:])), (
        "group_a slots [200, 1000) must remain NaT outside the shard window"
    )
    assert not np.any(np.isnat(ts_b)), (
        "group_b coord must be fully materialized: the shard window belongs "
        "to group_a only. Any NaT slot in group_b is the all-groups-windowed regression "
        "(window bled into a foreign group)."
    )


def test_env_nonexistent_slot_group_raises_usage_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``FIRECUBE_SLOT_GROUP`` naming an unknown group must fail loudly.

    Silent acceptance would either apply the shard window to no group
    (defeating parallelism) or fall back to the previous all-group
    narrowing bug. The CLI must instead reject the env and print the
    plugin's real group list so the operator can fix the deployment.
    """
    _clear_slot_env(monkeypatch)
    monkeypatch.setenv("FIRECUBE_SLOT_GROUP", "definitely_not_a_real_group")

    target = tmp_path / "bogus-group.zarr"
    result = CliRunner().invoke(cli, _preallocate_args(target))

    assert result.exit_code != 0, (
        f"preallocate must reject an unknown FIRECUBE_SLOT_GROUP; output={result.output!r}"
    )
    combined = (result.output or "") + (str(result.exception) if result.exception else "")
    assert "definitely_not_a_real_group" in combined, (
        f"error must name the bogus group; got output={result.output!r}, "
        f"exception={result.exception!r}"
    )
    assert "group_a" in combined and "group_b" in combined, (
        f"error must list the plugin's real groups so the operator can "
        f"correct the env; got output={result.output!r}, "
        f"exception={result.exception!r}"
    )


def test_env_empty_slot_group_treated_as_unset(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``FIRECUBE_SLOT_GROUP=""`` is unset — shell convention parity.

    A K8s manifest that conditionally injects an empty
    ``FIRECUBE_SLOT_GROUP`` for a single-group deployment must not be
    interpreted as a request to scope the window to a group literally
    named ``""``.
    """
    _clear_slot_env(monkeypatch)
    monkeypatch.setenv("FIRECUBE_SLOT_GROUP", "")

    target = tmp_path / "empty-group-env.zarr"
    result = CliRunner().invoke(cli, _preallocate_args(target))
    assert result.exit_code == 0, (
        f"empty FIRECUBE_SLOT_GROUP must be treated as unset and preallocate "
        f"the full extent; output={result.output!r}, exception={result.exception!r}"
    )

    ts_a = _read_timestamps(target, "group_a")
    ts_b = _read_timestamps(target, "group_b")
    assert not np.any(np.isnat(ts_a)), (
        "empty FIRECUBE_SLOT_GROUP must fall back to full-extent materialization"
    )
    assert not np.any(np.isnat(ts_b)), (
        "empty FIRECUBE_SLOT_GROUP must fall back to full-extent materialization"
    )


def test_zarr_slots_env_block_roundtrips_into_preallocate(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """End-to-end: ``zarr slots`` env block feeds ``zarr preallocate`` cleanly.

    Simulates the operator's per-shard Job: read the env dict from
    ``zarr slots`` output, set it in the environment, run preallocate.
    The named group is windowed to the shard slice; every other group
    is left at full extent.
    """
    _clear_slot_env(monkeypatch)

    slots_target = tmp_path / "roundtrip-plan.zarr"
    slots_result = CliRunner().invoke(cli, _slots_args(slots_target))
    assert slots_result.exit_code == 0, slots_result.output
    payload = json.loads(slots_result.output)

    ranges = payload["ranges"]
    assert ranges, "multi_group_capable plugin must produce at least one range"
    first_group_a = next((r for r in ranges if r["group"] == "group_a"), None)
    assert first_group_a is not None, f"expected a group_a range; got {ranges!r}"

    env_block = first_group_a["env"]
    assert env_block["FIRECUBE_SLOT_GROUP"] == "group_a"

    for key, value in env_block.items():
        monkeypatch.setenv(key, value)

    preallocate_target = tmp_path / "roundtrip-out.zarr"
    prealloc_result = CliRunner().invoke(cli, _preallocate_args(preallocate_target))
    assert prealloc_result.exit_code == 0, (
        f"preallocate must accept the env block emitted by `zarr slots`; "
        f"output={prealloc_result.output!r}, exception={prealloc_result.exception!r}"
    )

    ts_a = _read_timestamps(preallocate_target, "group_a")
    ts_b = _read_timestamps(preallocate_target, "group_b")
    slot_start = int(env_block["FIRECUBE_SLOT_START"])
    slot_end = int(env_block["FIRECUBE_SLOT_END"])

    assert not np.any(np.isnat(ts_a[slot_start:slot_end])), (
        f"group_a shard window [{slot_start}, {slot_end}) must be filled"
    )
    if slot_end < _SLOT_COUNT:
        assert np.all(np.isnat(ts_a[slot_end:])), (
            "group_a slots outside the shard window must remain NaT"
        )
    assert not np.any(np.isnat(ts_b)), (
        "group_b must be fully materialized: the shard env belongs to group_a"
    )


def test_env_unbounded_slot_group_raises_usage_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``FIRECUBE_SLOT_GROUP`` naming an unbounded group must fail loudly.

    A window cannot be scoped to a group with no resolvable extent; silently
    widening to other groups would repeat the all-group narrowing bug.
    """
    # The registry-reset fixture reloads only the multi-group module; the
    # mixed plugin's registration decorator must re-run for this test.
    importlib.reload(importlib.import_module("mixed_bounded_unbounded_test_plugin"))
    _clear_slot_env(monkeypatch)
    monkeypatch.setenv("FIRECUBE_SLOT_START", "0")
    monkeypatch.setenv("FIRECUBE_SLOT_END", "10")
    monkeypatch.setenv("FIRECUBE_SLOT_GROUP", "aux")

    target = tmp_path / "unbounded-group.zarr"
    args = [
        "zarr",
        "preallocate",
        "mixed_bounded_unbounded_test",
        "--target",
        f"file://{target}",
        "--product-name",
        "mixed_bounded_unbounded_test",
        "--storage-type",
        "local",
        "--storage-driver",
        "fsspec",
        "--write-mode",
        "direct",
        "--option",
        "no_progress=true",
    ]
    result = CliRunner().invoke(cli, args)

    assert result.exit_code != 0, (
        f"preallocate must reject FIRECUBE_SLOT_GROUP on an unbounded group; "
        f"output={result.output!r}"
    )
    combined = (result.output or "") + (str(result.exception) if result.exception else "")
    assert "aux" in combined, combined
    assert "resolvable extent" in combined or "unbounded" in combined, (
        f"error must explain the group has no resolvable extent; got {combined!r}"
    )
