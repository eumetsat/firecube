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

"""``firecube snapshot`` and ``release`` on Zarr v2 and v3 stores; new, no module covers them."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any
from unittest.mock import Mock

import numpy as np
import pytest
import xarray as xr
from click.testing import CliRunner

from firecube.cli.main import cli
from firecube.core.controlplane import ChunkManager
from tests.helpers.storage import make_test_session

pytestmark = [pytest.mark.integration, pytest.mark.contract]

FORMATS = pytest.mark.parametrize("fmt", [2, 3], ids=["v2", "v3"])
ARRAY_DOC = {2: "temp/.zattrs", 3: "temp/zarr.json"}
RELEASING = "cli-test:release"  # the fixture step whose `to_release` is 0.1.1


def _store(base: Path, fmt: int) -> Path:
    store = base / "p.zarr"
    data = {"temp": (("time", "x"), np.arange(12.0).reshape(4, 3), {"units": "K"})}
    xr.Dataset(data, attrs={"source": "orig"}).to_zarr(store, zarr_format=fmt, consolidated=True)
    _cli(store, "migrate", "adopt")
    return store


def _cli(store: Path, *args: str, exit_code: int = 0):
    out = CliRunner().invoke(cli, [*args, "--target", store.as_uri(), "--product-name", "p.zarr"])
    assert out.exit_code == exit_code, out.output
    return out


def _json(store: Path, *args: str):
    return json.loads(_cli(store, *args, "-f", "json").stdout)


def _hashes(store: Path, *parts: str) -> dict[str, str]:
    return {
        p.relative_to(store).as_posix(): hashlib.sha256(p.read_bytes()).hexdigest()
        for p in sorted(store.joinpath(*parts).rglob("*"))
        if p.is_file() and p.parent.name != "refs"  # refs are the one thing rewritten in place
    }


def _record_run(store: Path, run_id: str, producer: dict | None) -> None:
    manager = make_test_session(store.parent, product="p.zarr").control_plane()
    where: dict[str, Any] = {"product": "p.zarr", "run_id": run_id, "output_path": str(store)}
    where |= {"output_format": "zarr", "size": 0}
    manager.record_run_started(**where, meta={"producer": producer} if producer else {})
    manager.record_run_terminal(**where, meta={}, status="complete")
    manager.close()


def _hand_edit(store: Path, relative: str) -> None:
    doc = json.loads((store / relative).read_text())
    doc.get("attributes", doc)["hand_edit"] = 1
    (store / relative).write_text(json.dumps(doc))


@FORMATS
def test_snapshot_diff_names_exactly_the_hand_edited_attribute(tmp_path, fmt):
    store = _store(tmp_path, fmt)
    taken = _json(store, "snapshot", "take")
    assert (taken["consistency"], taken["run_id"]) == ("observation", None)
    assert taken["snapshot_id"] in _cli(store, "snapshot", "list").output
    assert ARRAY_DOC[fmt] in _cli(store, "snapshot", "show", "latest").output
    _hand_edit(store, ARRAY_DOC[fmt])
    known = {s["snapshot_id"] for s in _json(store, "snapshot", "list")}
    printed = _cli(store, "snapshot", "take").output  # text form: it prints the new id
    (after,) = {s["snapshot_id"] for s in _json(store, "snapshot", "list")} - known
    assert after in printed
    nodes = _json(store, "snapshot", "diff", taken["snapshot_id"], after)["nodes"]
    assert {n["path"]: dict(n["attributes_added"]) for n in nodes} == {"temp": {"hand_edit": 1}}
    assert "hand_edit" in _cli(store, "snapshot", "diff", taken["snapshot_id"], after).output
    assert ARRAY_DOC[fmt] in json.dumps(_json(store, "snapshot", "show", after))
    assert "not a snapshot id" in _cli(store, "snapshot", "show", "zz", exit_code=1).output
    _cli(store, "snapshot", "show", "0" * 64, exit_code=1)  # well formed, but none exists


@FORMATS
@pytest.mark.parametrize("ingested", [False, True], ids=["no-producer-anywhere", "real-ingest"])
def test_release_producer_is_the_ingests_across_migrate(tmp_path, subtests, fmt, ingested):
    store = _store(tmp_path, fmt)
    earlier = {"v": "8", "dirty": True} if ingested else None
    _record_run(store, "ingest-1", earlier)
    expected = None
    if ingested:  # one real, completed ingest; its producer is read back from its own run_started
        opts = ["--write-mode", "direct", "--input-data", tmp_path.as_uri()]
        _cli(store, "ingest", "cli_test_plugin", *opts, "--option", "allow_empty_source=true")
        paths = (store / ".firecube/runs").glob("*/events-*.jsonl")  # read, not selected
        events = [json.loads(ln) for p in paths if p.parent.name != "ingest-1" for ln in p.open()]
        (expected,) = [e["meta"]["producer"] for e in events if e["event_type"] == "run_started"]
        assert "source" in expected and expected != earlier
    _cli(store, "migrate", "run", "cli-test:retitle")  # a newer run that records no producer
    release = _json(store, "release", "create", "1.0")["release"]
    status = "recorded" if expected else "unknown"
    assert (release["producer"], release["producer_status"]) == (expected, status)
    assert "ingest-1" in release["runs_since_previous"]
    assert _json(store, "release", "show", "1.0")["producer"] == expected
    for text in ("show", "list"):  # a producer-bearing release renders in text as well
        with subtests.test(msg=text):
            assert status in _cli(store, "release", text).output


@FORMATS
def test_migrate_to_release_reuses_the_after_snapshot_and_verifies(tmp_path, fmt):
    store = _store(tmp_path, fmt)
    _cli(store, "migrate", "run", RELEASING)
    (after,) = [s for s in _json(store, "snapshot", "list") if s["trigger"] == "migrate-after"]
    assert _json(store, "release", "show")["snapshot"] == after["snapshot_id"]
    _cli(store, "release", "verify")
    before = _hashes(store)
    assert "already applied" in _cli(store, "migrate", "run", RELEASING).output
    assert _hashes(store) == before


@FORMATS
def test_release_create_stamps_root_and_image_verify_sees_metadata_only(tmp_path, subtests, fmt):
    store = _store(tmp_path, fmt)
    _cli(store, "release", "create", "1.0")
    for name in {2: (".zattrs", ".zmetadata"), 3: ("zarr.json",)}[fmt]:  # live root, then image
        with subtests.test(msg=name):
            assert '"firecube_release": "1.0"' in json.dumps(json.loads((store / name).read_text()))
    _cli(store, "release", "verify")
    (store / {2: "temp/0.0", 3: "temp/c/0/0"}[fmt]).write_bytes(b"not a chunk")
    _cli(store, "release", "verify")  # P20: a chunk-byte edit is not seen
    _hand_edit(store, ARRAY_DOC[fmt])
    assert ARRAY_DOC[fmt] in _cli(store, "release", "verify", exit_code=1).output


@FORMATS
@pytest.mark.parametrize("mode", ["standalone", "migrate"])
@pytest.mark.parametrize("failing", ["create_release", "publish_current_release"])
def test_release_resume_rewrites_nothing(tmp_path, monkeypatch, subtests, fmt, mode, failing):
    store = _store(tmp_path, fmt)
    args = ("release", "create", "0.1.1") if mode == "standalone" else ("migrate", "run", RELEASING)
    monkeypatch.setattr(ChunkManager, failing, Mock(side_effect=RuntimeError("injected")))
    _cli(store, *args, exit_code=1)
    monkeypatch.undo()
    kept = {part: _hashes(store, ".firecube", part) for part in ("releases", "metadata")}
    _cli(store, *args)
    assert _json(store, "release", "show")["version"] == "0.1.1"
    _cli(store, "release", "verify")
    for part, files in kept.items():
        now = _hashes(store, ".firecube", part)
        with subtests.test(msg=part):
            assert {p: now[p] for p in files} == files  # nothing already stored is rewritten
            assert failing != "publish_current_release" or set(now) == set(files)  # only the ref


def test_versions_with_slash_percent_and_dot_round_trip_under_escaped_keys(tmp_path, subtests):
    store = _store(tmp_path, 3)
    keys = {"a/b": "a%2Fb", "a%b": "a%25b", ".hidden": "%2Ehidden"}
    for version, key in keys.items():
        _cli(store, "release", "create", version)
        with subtests.test(msg=version):
            assert (store / ".firecube" / "releases" / f"{key}.json").is_file()
            assert _json(store, "release", "show", version)["version"] == version
    assert [r["version"] for r in _json(store, "release", "list")["releases"]] == list(keys)


@pytest.mark.parametrize("version", ["a\nb", "x" * 230], ids=["newline", "230-bytes"])
def test_version_the_storage_cannot_hold_is_refused_before_anything_is_written(tmp_path, version):
    store = _store(tmp_path, 3)
    before = _hashes(store)
    _cli(store, "release", "create", version, exit_code=1)
    assert _hashes(store) == before
