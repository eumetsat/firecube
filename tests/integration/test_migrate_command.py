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

"""``firecube migrate`` on real Zarr v2 and v3 stores written by xarray, through the CLI.

New module because no existing test module covers the migrate command. The steps come from
the ``cli_test_plugin`` fixture module, registered under ``firecube.migrations``.
"""

from __future__ import annotations

import dataclasses
import importlib.metadata
import json
import shutil
from pathlib import Path

import cli_test_plugin
import numpy as np
import pytest
import xarray as xr
import zarr
from click.testing import CliRunner

import firecube.cli.migrate._run as migrate_run_module
import firecube.ingestor.runtime.migrate as migrate_module
from firecube.cli.main import cli
from firecube.core.controlplane import ChunkManager
from firecube.core.controlplane.claims import ClaimHandle
from firecube.core.controlplane.types import exclusive_maintenance_domain
from firecube.core.filesystem.conditional import GUARD_FILE_NAME
from firecube.core.zarr.guard import metadata_authority
from tests.helpers.cli_hints import run_hint
from tests.helpers.storage import make_test_session

pytestmark = [pytest.mark.integration, pytest.mark.contract]

RETITLE = "cli-test:retitle"
DOCUMENT_NAMES = {".zarray", ".zattrs", ".zgroup", ".zmetadata", "zarr.json"}
FORMATS = pytest.mark.parametrize("fmt", [2, 3], ids=["v2", "v3"])
BOTH_ALLOWS = ("--allow", "interpretation", "--allow", "unknown")
IMAGE = {2: ".zmetadata", 3: "zarr.json"}  # the root consolidated image, written last
ARRAY_DOC = {2: "temp/.zattrs", 3: "temp/zarr.json"}


def _dataset(applied: bool) -> xr.Dataset:
    attrs = {"units": "K", **({"long_name": "Air temperature"} if applied else {})}
    root = {"source": "orig", "comment": "old"}
    if applied:
        root |= {"title": "Retitled", "institution": "Fixture Institute"}
    return xr.Dataset(
        {"temp": (("time", "x"), np.arange(12.0).reshape(4, 3), attrs)},
        coords={"time": np.arange(4), "x": np.arange(3)},
        attrs=root,
    )


def _store(base: Path, fmt: int, *, applied: bool = False, adopt: bool = True) -> Path:
    store = base / "p.zarr"
    _dataset(applied).to_zarr(store, zarr_format=fmt, consolidated=True)
    if adopt:  # a real run refuses a store with no control plane
        _cli(store, "migrate", "adopt")
    return store


def _cli(store: Path, *args: str, exit_code: int = 0):
    label = ["--product-name", "p.zarr"] if args[:2] == ("migrate", "adopt") else []
    asks = args[:2] in {("migrate", "adopt"), ("migrate", "run"), ("release", "create")}
    confirm = ["--yes-i-really-mean-it"] if asks and "--dry-run" not in args else []
    result = CliRunner().invoke(cli, [*args, "--target", store.as_uri(), *label, *confirm])
    assert result.exit_code == exit_code, result.output
    return result


def _migrate(store: Path, step: str, *args: str, exit_code: int = 0):
    return _cli(store, "migrate", "run", step, *args, exit_code=exit_code)


def _tree(store: Path, *, firecube: bool = True) -> dict[str, bytes | None]:
    """Every path under the store (None for a directory), optionally without `.firecube/`."""
    return {
        p.relative_to(store).as_posix(): p.read_bytes() if p.is_file() else None
        for p in sorted(store.rglob("*"))
        if firecube or ".firecube" not in p.relative_to(store).parts
    }


def _chunks(store: Path) -> dict[str, bytes | None]:
    return {
        path: data
        for path, data in _tree(store, firecube=False).items()
        if data is not None and Path(path).name not in DOCUMENT_NAMES
    }


def _claim_files(store: Path) -> list[Path]:
    """The claim records of a store, the permanent guard file left out."""
    claims = store / ".firecube" / "claims"
    return [p for p in claims.iterdir() if p.name != GUARD_FILE_NAME]


def _runs(store: Path, prefix: str = "migrate-") -> dict[str, dict]:
    """Run id to the meta of its last event, for the runs whose id starts with `prefix`."""
    found = {}
    for run_dir in sorted((store / ".firecube" / "runs").glob(f"{prefix}*")):
        events = [
            json.loads(line)
            for path in sorted(run_dir.glob("events-*.jsonl"))
            for line in path.read_text().splitlines()
        ]
        found[run_dir.name] = {**events[-1]["meta"], "status": events[-1]["record"]["status"]}
    return found


def _snapshot_ids(store: Path) -> list[str]:
    return [
        s["snapshot_id"] for s in json.loads(_cli(store, "snapshot", "list", "-f", "json").output)
    ]


def _attrs(store: Path, node: str = "") -> dict:
    group = zarr.open_group(store, mode="r", use_consolidated=False)
    return dict(group[node].attrs if node else group.attrs)


def _hand_edit(store: Path, relative: str) -> None:
    path = store / relative
    doc = json.loads(path.read_text())
    doc.get("attributes", doc)["hand_edit"] = 1
    path.write_text(json.dumps(doc))


def _image(store: Path, fmt: int) -> dict:
    doc = json.loads((store / IMAGE[fmt]).read_text())
    return doc if fmt == 2 else doc["consolidated_metadata"]


def _assert_reads_agree(store: Path, fmt: int) -> None:
    """Plain zarr and xarray read the same data and attributes with and without the image."""
    live = zarr.open_group(store, mode="r", use_consolidated=False)
    image = zarr.open_group(store, mode="r", use_consolidated=True)
    assert dict(image.attrs) == dict(live.attrs)
    assert dict(image["temp"].attrs) == dict(live["temp"].attrs)
    opened = [xr.open_zarr(store, consolidated=flag, zarr_format=fmt) for flag in (True, False)]
    xr.testing.assert_identical(*opened)
    np.testing.assert_array_equal(opened[0]["temp"].values, np.arange(12.0).reshape(4, 3))
    # The oracle: zarr's own consolidation of a copy without `.firecube/` gives the same image.
    oracle = store.parent / "oracle"
    shutil.rmtree(oracle, ignore_errors=True)
    shutil.copytree(store, oracle, ignore=shutil.ignore_patterns(".firecube"))
    zarr.consolidate_metadata(str(oracle))
    assert _image(store, fmt) == _image(oracle, fmt)


@FORMATS
def test_migrate_run_applies_the_step_once_over_untouched_chunks(tmp_path, fmt, subtests):
    store = _store(tmp_path, fmt)
    chunks = _chunks(store)
    assert "executed" in _migrate(store, RETITLE).output
    with subtests.test("chunk keys are byte-identical"):
        assert _chunks(store) == chunks
    with subtests.test("image is consistent with the live documents"):
        assert _attrs(store)["title"] == "Retitled"
        _assert_reads_agree(store, fmt)
    ids = _snapshot_ids(store)
    ((run_id, meta),) = _runs(store).items()
    with subtests.test("one run records the step, its distribution and two snapshots"):
        assert (meta["step_id"], meta["distribution"]) == (RETITLE, "firecube-test-plugins")
        assert meta["version"] == importlib.metadata.version("firecube-test-plugins")
        assert [meta["before_snapshot"], meta["after_snapshot"]] == ids
        status = json.loads(_cli(store, "migrate", "status", "-f", "json").output)
        assert [(r["run_id"], r["step_id"], r["outcome"]) for r in status] == [
            (run_id, RETITLE, "executed")
        ]
    with subtests.test("snapshot diff lists exactly the step's attributes and the managed stamp"):
        diff = json.loads(_cli(store, "snapshot", "diff", *ids, "-f", "json").output)
        nodes = {n["path"]: n for n in diff["nodes"]}
        assert set(nodes) == {"", "temp"}
        assert all(
            not n["attributes_changed"] and not n["attributes_removed"] for n in nodes.values()
        )
        root_added = dict(nodes[""]["attributes_added"])
        stamp = root_added.pop("firecube_last_modifier")  # P19: separate from the step's own
        assert (stamp["operation"], stamp["step_id"]) == ("migrate", RETITLE)
        assert stamp["run_id"] == run_id
        assert root_added == {"title": "Retitled", "institution": "Fixture Institute"}
        assert nodes["temp"]["attributes_added"] == {"long_name": "Air temperature"}
        # On v3 the root diff also lists the consolidated_metadata extension change.
        assert set(nodes[""]["extensions"]) == ({"consolidated_metadata"} if fmt == 3 else set())
    with subtests.test("a second run is already applied and records no run"):
        before = _tree(store)
        assert "already applied" in _migrate(store, RETITLE).output
        assert _tree(store) == before
        assert list(_runs(store)) == [run_id]
    with subtests.test("a run after a hand edit of a planned document is refused as a third state"):
        _hand_edit(store, ARRAY_DOC[fmt])
        assert ARRAY_DOC[fmt] in _migrate(store, RETITLE, exit_code=1).output
        assert list(_runs(store)) == [run_id]


@FORMATS
def test_to_release_step_creates_the_release_inside_the_migration(tmp_path, fmt):
    store = _store(tmp_path, fmt)
    _migrate(store, "cli-test:release")
    assert list((store / ".firecube" / "releases").glob("0.1.1.json"))
    assert _attrs(store)["firecube_release"]
    assert [m["release"] for m in _runs(store).values()] == ["0.1.1"]
    _assert_reads_agree(store, fmt)


@FORMATS
@pytest.mark.parametrize(
    "checkpoint",
    ["journal", "one-document", "before-image", "before-after-snapshot", "hand-edit", "claim-swap"],
)
def test_interrupted_migration_resumes_under_its_run_or_refuses_a_hand_edit(
    tmp_path, monkeypatch, fmt, checkpoint
):
    store = _store(tmp_path, fmt)
    chunks, before = _chunks(store), _tree(store, firecube=False)
    writes = []
    real_publish, real_snapshot = metadata_authority.publish_raw, ChunkManager.take_snapshot

    def publish(authority, target, path, *args, **kwargs):
        # before-image stops at the image write; the others stop at the second write.
        if path == IMAGE[fmt] if checkpoint == "before-image" else len(writes) == 1:
            raise KeyboardInterrupt
        writes.append(path)
        written = real_publish(authority, target, path, *args, **kwargs)
        if checkpoint == "claim-swap":  # another owner takes the claim after the first document
            (claim,) = _claim_files(store)
            claim.write_text(json.dumps({**json.loads(claim.read_text()), "owner_id": "other"}))
        return written

    def snapshot(self, product, **kwargs):
        if kwargs["trigger"] == "migrate-after":
            raise KeyboardInterrupt
        return real_snapshot(self, product, **kwargs)

    def no_target(**_kwargs):
        raise KeyboardInterrupt

    with monkeypatch.context() as crash:
        if checkpoint == "journal":
            crash.setattr(migrate_module, "open_metadata_target", no_target)
        elif checkpoint == "before-after-snapshot":
            crash.setattr(ChunkManager, "take_snapshot", snapshot)
        else:
            crash.setattr(metadata_authority, "publish_raw", publish)
        # The injected KeyboardInterrupt ends the run with 130; a lost claim (claim-swap) is a refusal, 1.
        _migrate(store, RETITLE, exit_code=1 if checkpoint == "claim-swap" else 130)
    ((run_id, meta),) = _runs(store).items()
    assert meta["status"] == ("failed" if checkpoint == "claim-swap" else "started")
    assert _chunks(store) == chunks
    # The documents written so far; the consolidated image is published after the attribute documents.
    attribute_docs = {2: {".zattrs", "temp/.zattrs"}, 3: {"temp/zarr.json"}}[fmt]
    written = {p for p, data in _tree(store, firecube=False).items() if data != before.get(p)}
    if checkpoint == "journal":
        assert not written
    elif checkpoint in {"one-document", "hand-edit", "claim-swap"}:
        assert len(written) == 1 and written <= attribute_docs
    elif checkpoint == "before-image":
        assert written == attribute_docs  # every attribute document, the image still as it was
    else:
        assert written == attribute_docs | {IMAGE[fmt]}

    if checkpoint == "claim-swap":
        return  # the second document was refused, so the first is the only one written
    if checkpoint == "hand-edit":
        planned = {2: "temp/.zattrs", 3: "zarr.json"}[fmt]
        _hand_edit(store, planned)
        edited = _tree(store, firecube=False)
        refusal = _migrate(store, RETITLE, exit_code=1).output
        assert planned in refusal
        run_hint(refusal, "firecube chunks runs abandon", "--dry-run")
        assert _runs(store)[run_id]["status"] == "started"
        assert _tree(store, firecube=False) == edited
        return
    journaled = {p: _tree(store, firecube=False)[p] for p in written}
    with monkeypatch.context() as resume:
        resume.setattr(cli_test_plugin, "REPLAN_FORBIDDEN", True)  # P17: at every checkpoint
        _migrate(store, RETITLE)
    after = _tree(store, firecube=False)
    assert {p: after[p] for p in written} == journaled  # the rerun replays, never rewrites them
    assert {k: v["status"] for k, v in _runs(store).items()} == {run_id: "complete"}
    assert _attrs(store)["firecube_last_modifier"]["run_id"] == run_id
    assert _attrs(store, "temp")["long_name"] == "Air temperature"
    assert len(_snapshot_ids(store)) == 2
    assert _chunks(store) == chunks
    _assert_reads_agree(store, fmt)


def test_crash_left_claim_blocks_the_rerun_until_claims_clear(tmp_path, monkeypatch):
    store = _store(tmp_path, 2)
    real_publish, writes = metadata_authority.publish_raw, []

    def publish(authority, target, path, *args, **kwargs):
        if writes:
            raise KeyboardInterrupt
        writes.append(path)
        return real_publish(authority, target, path, *args, **kwargs)

    with monkeypatch.context() as crash:
        crash.setattr(metadata_authority, "publish_raw", publish)
        crash.setattr(ClaimHandle, "release", lambda self: self._stop.set())  # a killed process
        _migrate(store, RETITLE, exit_code=130)
    ((run_id, _),) = _runs(store).items()
    assert len(_claim_files(store)) == 1

    _migrate(store, RETITLE, exit_code=1)
    assert {k: v["status"] for k, v in _runs(store).items()} == {run_id: "started"}
    domain = exclusive_maintenance_domain("p.zarr").identifier
    clear = ["chunks", "claims", "clear", "--target", store.as_uri(), "--domain", domain]
    result = CliRunner().invoke(cli, [*clear, "--force", "--yes-i-really-mean-it"])
    assert result.exit_code == 0, result.output
    _migrate(store, RETITLE)
    assert {k: v["status"] for k, v in _runs(store).items()} == {run_id: "complete"}


@FORMATS
@pytest.mark.parametrize(
    ("args", "node", "present", "absent"),
    [
        (["--set", "title=T", "--set", "summary=S", "--remove", "comment"], "", {"title": "T", "summary": "S", "source": "orig"}, ["comment"]),
        (["--node", "temp", "--set", "long_name=Air"], "temp", {"long_name": "Air", "units": "K"}, []),
        (["--option", "set=title=T"], "", {"title": "T", "comment": "old"}, []),
    ],
    ids=["set-and-remove", "node", "option-singleton"],
)  # fmt: skip
def test_set_attrs_applies_its_options_once_per_store(tmp_path, fmt, args, node, present, absent):
    store = _store(tmp_path, fmt)
    _migrate(store, "firecube:set-attrs", *args)
    attrs = _attrs(store, node)
    assert {k: attrs[k] for k in present} == present
    assert not set(absent) & set(attrs)
    _assert_reads_agree(store, fmt)

    before = _tree(store)
    again = _migrate(store, "firecube:set-attrs", "--set", "title=Other")
    assert "already applied" in again.output  # identity is (step id, distribution, version)
    assert _tree(store) == before


@pytest.mark.parametrize(
    ("key", "allow", "applied"),
    [
        ("title", (), True),
        ("units", (), False),
        ("scale_factor", (), False),
        ("units", ("--allow", "interpretation"), True),
        ("firecube_x", BOTH_ALLOWS, False),
        ("_FillValue", BOTH_ALLOWS, False),
        ("_ARRAY_DIMENSIONS", BOTH_ALLOWS, False),
        ("some_custom_key", (), False),
        ("some_custom_key", ("--allow", "unknown"), True),
    ],
)
def test_attribute_classes_are_refused_unless_their_allow_flag_is_given(
    tmp_path, key, allow, applied
):
    store = _store(tmp_path, 2)
    before = _tree(store, firecube=False)
    args = ("firecube:set-attrs", "--set", f"{key}=1", *allow)
    result = _migrate(store, *args, exit_code=0 if applied else 1)
    if not applied:
        assert key in result.output
        assert _tree(store, firecube=False) == before
        assert {m["status"] for m in _runs(store).values()} <= {"failed"}  # recorded, not applied
        return
    assert _attrs(store)[key] == "1"
    ((_, meta),) = _runs(store).items()
    assert [o["key"] for o in meta["overrides"]] == ([] if key == "title" else [key])  # recorded


@FORMATS
def test_dry_run_prints_the_plan_and_writes_nothing(tmp_path, fmt):
    store = _store(tmp_path, fmt, adopt=False)
    before = _tree(store)
    result = _migrate(store, RETITLE, "--dry-run", exit_code=2)
    assert "records no product label" in result.output and "firecube migrate adopt" in result.output
    assert _tree(store) == before  # no .firecube/ directory is created either
    _cli(store, "migrate", "adopt")
    before = _tree(store)
    planned = _migrate(store, RETITLE, "--dry-run")
    assert planned.output.rstrip().splitlines()[-1].startswith("planned outcome:")
    assert _tree(store) == before  # no claim, no run, no snapshot
    assert _runs(store) == {}


@pytest.mark.parametrize(
    ("args", "adopted"),
    [
        (("snapshot", "take"), True),
        (("migrate", "adopt"), False),
        (("release", "create", "1.0.0"), True),
        (("release", "create", "1.0.0", "--archive", "{archive}"), True),
    ],
    ids=["snapshot-take", "migrate-adopt", "release-create", "release-archive"],
)
def test_maintenance_dry_run_leaves_store_claims_runs_and_archive_untouched(
    tmp_path, args, adopted
):
    store, archive = _store(tmp_path, 3, adopt=adopted), tmp_path / "a.zip"

    def state():  # byte map with `.firecube/`, then claims and runs listed through ChunkManager
        if not adopted:  # opening a control plane would itself create `.firecube/`
            return _tree(store), []
        manager = make_test_session(tmp_path, product="p.zarr").control_plane()
        try:
            return _tree(store), [
                manager.list_claims(product="p.zarr"),
                manager.list_runs(product="p.zarr"),
            ]
        finally:
            manager.close()

    before = state()
    _cli(store, *(a.format(archive=archive.as_uri()) for a in args), "--dry-run")
    assert state() == before and not archive.exists()


def test_migrate_run_off_a_tty_without_confirmation_exits_2_and_changes_nothing(tmp_path):
    store = _store(tmp_path, 3)
    before = _tree(store)
    result = CliRunner().invoke(cli, ["migrate", "run", RETITLE, "--target", store.as_uri()])
    assert result.exit_code == 2 and "--yes-i-really-mean-it" in result.output
    assert _tree(store) == before and _runs(store) == {}


@FORMATS
def test_step_already_applied_by_hand_is_adopted_not_executed(tmp_path, fmt):
    store = _store(tmp_path, fmt, applied=True)
    chunks, attrs = _chunks(store), _attrs(store)
    assert "adopted" in _migrate(store, RETITLE).output
    status = json.loads(_cli(store, "migrate", "status", "-f", "json").output)
    assert [r["outcome"] for r in status if r["step_id"] == RETITLE] == ["adopted"]
    ((run_id, meta),) = _runs(store).items()
    assert meta["outcome"] == "adopted"
    assert _chunks(store) == chunks
    now = _attrs(store)
    stamp = now.pop("firecube_last_modifier")  # P19: the adopted step is stamped too
    assert (stamp["operation"], stamp["step_id"], stamp["run_id"]) == ("migrate", RETITLE, run_id)
    assert now == attrs


def _installed(monkeypatch, **changes):
    real = migrate_run_module.load_migration_steps
    monkeypatch.setattr(
        migrate_run_module,
        "load_migration_steps",
        lambda: [dataclasses.replace(s, **changes) if s.id == RETITLE else s for s in real()],
    )


def test_step_rerun_at_another_distribution_version_is_refused_naming_both(tmp_path, monkeypatch):
    store = _store(tmp_path, 2)
    _migrate(store, RETITLE)
    recorded = importlib.metadata.version("firecube-test-plugins")
    _installed(monkeypatch, version="9.9.9")  # the entry point now reports another version
    refused = _migrate(store, RETITLE, exit_code=1).output
    assert recorded in refused and "9.9.9" in refused
    assert len(_runs(store)) == 1


def test_step_without_a_distribution_is_refused_before_anything_is_written(tmp_path, monkeypatch):
    store = _store(tmp_path, 2)
    before = _tree(store)
    _installed(monkeypatch, distribution=None)
    assert "no distribution" in _migrate(store, RETITLE, exit_code=1).output
    assert _tree(store) == before


@pytest.mark.parametrize(
    ("step", "named"),
    [("cli-test:needs-retitle", RETITLE), ("cli-test:from-release", "9.9.9")],
    ids=["depends_on", "from_release"],
)
def test_unmet_step_precondition_is_refused_naming_what_is_missing(tmp_path, step, named):
    store = _store(tmp_path, 2)
    before = _tree(store, firecube=False)
    assert named in _migrate(store, step, exit_code=1).output
    assert _tree(store, firecube=False) == before
    assert "complete" not in {m["status"] for m in _runs(store).values()}


def test_depends_on_is_met_once_the_dependency_is_applied(tmp_path):
    store = _store(tmp_path, 2)
    _migrate(store, RETITLE)
    _migrate(store, "cli-test:needs-retitle")
    assert _attrs(store)["comment"] == "after retitle"


@FORMATS
def test_foreign_store_refuses_a_real_run_until_adopted(tmp_path, fmt):
    store = _store(tmp_path, fmt, adopt=False)
    before = _tree(store)
    refused = _migrate(store, RETITLE, exit_code=2).output
    assert "records no product label" in refused and "firecube migrate adopt --target" in refused
    assert _tree(store) == before  # the refusal writes nothing, no .firecube/ either
    result = _cli(store, "migrate", "adopt")  # stdout: report; stderr: warnings; output: both
    warning = "is Zarr format 2. Firecube cannot append to it"
    assert f"Zarr format {fmt})" in result.stdout and warning not in result.stdout
    assert (warning in result.stderr) == (fmt == 2)
    assert fmt == 3 or result.output.index("adopted the store") < result.output.index(warning)
    ((_, meta),) = _runs(store, "adopt-").items()
    assert (meta["imported"], meta["chunks"], meta["zarr_format"]) == (True, "unverified", fmt)
    assert _tree(store, firecube=False) == before  # no Zarr document or chunk is written
    assert (store / ".firecube" / "schema.json").exists()
    again = _cli(store, "migrate", "adopt")
    assert "nothing to adopt" in again.output and warning not in again.output
    assert len(_runs(store, "adopt-")) == 1


def test_adopt_stamps_the_label_and_refuses_a_different_one(tmp_path):
    store = _store(tmp_path, 3)  # adopted with --product-name p.zarr
    assert "p.zarr" in (store / ".firecube" / "product.json").read_text()
    _migrate(store, RETITLE)  # no --product-name: the recorded label is read
    before = _tree(store)
    other = ["migrate", "adopt", "--target", store.as_uri(), "--product-name", "other"]
    result = CliRunner().invoke(cli, other)
    assert result.exit_code == 2 and "records the label 'p.zarr'" in result.output
    assert _tree(store) == before  # the refusal writes nothing


def test_ingest_during_a_running_migration_is_refused_and_the_migration_finishes(
    tmp_path, monkeypatch
):
    store = _store(tmp_path, 2)
    chunks = _chunks(store)
    args = ["ingest", "cli_test_plugin", "--storage-type", "local", "--storage-driver", "fsspec"]
    args += ["--write-mode", "direct", "--input-data", tmp_path.as_uri()]
    args += ["--option", "allow_empty_source=true", "--product-name", "p.zarr"]
    ingests, real_publish = [], metadata_authority.publish_raw

    def publish(*args_, **kwargs):  # the migration is paused here, before its first document
        if not ingests:
            ingests.append(CliRunner().invoke(cli, [*args, "--target", store.as_uri()]))
        return real_publish(*args_, **kwargs)

    monkeypatch.setattr(metadata_authority, "publish_raw", publish)
    _migrate(store, RETITLE)
    ((run_id, meta),) = _runs(store).items()
    (ingest,) = ingests
    assert ingest.exit_code == 1
    assert run_id in ingest.output  # refused because the migration run is not terminal
    assert meta["status"] == "complete"
    assert _chunks(store) == chunks  # the ingest wrote no payload
