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

"""``firecube snapshot`` and ``release`` on Zarr v2 and v3 stores: snapshot diffs, release
producers, resume, version escaping, zip-archive retention (full, incremental, verify, backup chain)
and object-version retention on a versioned bucket served by moto (marked ``s3``)."""

from __future__ import annotations

import hashlib
import json
import zipfile
from collections.abc import Iterator
from pathlib import Path
from typing import Any
from unittest.mock import Mock

import numpy as np
import pytest
import xarray as xr
from click.testing import CliRunner

from firecube.cli.main import cli
from firecube.core.controlplane import ChunkManager
from firecube.core.filesystem import fsspec_backend
from tests.helpers.storage import make_test_session
from tests.integration._staged_helpers import s3_env

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


def _archived(store: Path, version: str, archive: Path, *flags: str) -> dict[str, Any]:
    return _json(store, "release", "create", version, "--archive", archive.as_uri(), *flags)[
        "release"
    ]


def _inventory(store: Path, release: dict[str, Any]) -> dict[str, str]:
    path = store / ".firecube/releases/inventories" / f"{release['retention']['inventory']}.json"
    return {o["key"]: o["digest"] for o in json.loads(path.read_text())["objects"]}


def _entries(archive: Path) -> dict[str, str]:
    with zipfile.ZipFile(archive) as zf:
        return {n: "sha256:" + hashlib.sha256(zf.read(n)).hexdigest() for n in zf.namelist()}


def _rewrite_zip(archive: Path, name: str) -> None:
    with zipfile.ZipFile(archive) as zf:
        members = [(i, zf.read(i)) for i in zf.infolist()]
    with zipfile.ZipFile(archive, "w") as zf:
        for info, data in members:
            zf.writestr(info, data + b"x" if info.filename == name else data)


def test_release_archive_holds_every_object_and_verify_reports_it_present(tmp_path):
    store = _store(tmp_path, 3)
    archive = tmp_path / "full.zip"
    release = _archived(store, "1.0", archive)
    assert release["retention"]["mode"] == "full"
    inventory = _inventory(store, release)
    assert {"zarr.json", "temp/c/0/0"} <= set(inventory)
    entries = _entries(archive)
    assert entries.pop("firecube-archive.json")  # the reserved marker is the one extra entry
    assert entries == inventory
    assert _json(store, "release", "verify")["retention"] == {
        "kind": "zip",
        "holds": True,
        "archive": "present",
        "backup_chain": "intact",
    }


def test_release_verify_reports_a_rewritten_archive_as_changed(tmp_path):
    store = _store(tmp_path, 3)
    archive = tmp_path / "full.zip"
    _archived(store, "1.0", archive)
    _rewrite_zip(archive, "temp/c/0/0")  # marker kept, valid CRC, one entry's bytes differ
    out = _cli(store, "release", "verify", exit_code=1).output
    assert "archive: changed" in out


def test_interrupted_release_archive_is_refused_on_rerun_and_rebuilt_from_one_run(
    tmp_path, monkeypatch
):
    store = _store(tmp_path, 3)
    archive = tmp_path / "full.zip"
    args = ("release", "create", "1.0", "--archive", archive.as_uri())
    monkeypatch.setattr(ChunkManager, "create_release", Mock(side_effect=RuntimeError("injected")))
    _cli(store, *args, exit_code=1)
    monkeypatch.undo()
    assert archive.is_file()  # the failure came after the archive write

    def tree() -> dict[str, bytes]:  # the whole product, refs included, claims excluded
        return {
            p.relative_to(store).as_posix(): p.read_bytes()
            for p in sorted(store.rglob("*"))
            if p.is_file() and ".firecube/claims" not in p.as_posix()
        }

    before, kept = tree(), archive.read_bytes()
    assert str(archive) in _cli(store, *args, exit_code=1).output
    assert tree() == before
    assert archive.read_bytes() == kept
    archive.unlink()
    release = _json(store, *args, "-f", "json")["release"]
    entries = _entries(archive)
    entries.pop("firecube-archive.json")
    assert entries == _inventory(store, release)
    _cli(store, "release", "verify")


def test_archive_full_on_a_later_release_writes_a_full_archive_not_an_incremental_backup(tmp_path):
    store = _store(tmp_path, 3)
    _archived(store, "1.0", tmp_path / "one.zip")
    second = _archived(store, "2.0", tmp_path / "two.zip", "--archive-full")
    assert (second["retention"]["mode"], second["retention"]["chain_length"]) == ("full", 1)
    assert set(_entries(tmp_path / "two.zip")) - {"firecube-archive.json"} == set(
        _inventory(store, second)
    )


def test_second_release_archive_is_an_incremental_backup_of_changed_objects_and_metadata_documents(
    tmp_path,
):
    store = _store(tmp_path, 3)
    _archived(store, "1.0", tmp_path / "one.zip")
    (store / "temp/c/0/0").write_bytes(b"re-ingested chunk")  # the one object that changes
    (store / "temp/c/1").mkdir()
    (store / "temp/c/1/0").write_bytes(b"new chunk")  # the one object that is added
    second = _archived(store, "2.0", tmp_path / "two.zip")
    assert (second["retention"]["mode"], second["retention"]["chain_length"]) == ("incremental", 2)
    entries = _entries(tmp_path / "two.zip")
    marker = json.loads(zipfile.ZipFile(tmp_path / "two.zip").read("firecube-archive.json"))
    del entries["firecube-archive.json"]
    # The fixture is a Zarr v3 store: its metadata documents are these two files, named by hand.
    product = {k for k in entries if not k.startswith(".firecube/")}
    assert product == {"zarr.json", "temp/zarr.json", "temp/c/0/0", "temp/c/1/0"}
    # Control-plane objects of this fixture. The release records and pointer are new; the
    # run directories, snapshot blobs and inventory carry generated ids, so by directory.
    control = {k for k in entries if k.startswith(".firecube/")}
    assert {".firecube/releases/1.0.json", ".firecube/releases/refs/current.json"} <= control
    assert {k.split("/")[1] for k in control} <= {"metadata", "releases", "runs"}
    # Unchanged objects are absent by name: the schema and the first adopt run.
    assert ".firecube/schema.json" not in entries
    assert not [k for k in entries if k.startswith(".firecube/runs/adopt-")]
    live = {  # each entry against the live bytes read from disk
        name: "sha256:" + hashlib.sha256((store / name).read_bytes()).hexdigest()
        for name in product
    }
    assert {name: entries[name] for name in product} == live
    assert (marker["mode"], marker["previous_release"], marker["inventory"]) == (
        "incremental",
        "1.0",
        second["retention"]["inventory"],
    )
    assert marker["previous_backup_uri"] == (tmp_path / "one.zip").as_uri()
    assert "Previous release:" in _cli(store, "release", "show").output


def _retention_files(store: Path) -> list[str]:
    return sorted(p.name for p in (store / ".firecube/releases").rglob("*.json"))


def test_obstore_release_refuses_a_name_its_listing_hides_and_writes_nothing(tmp_path):
    store = _store(tmp_path, 3)
    (store / "payload#12").write_text("hidden from the obstore listing")
    before, archive = _retention_files(store), tmp_path / "one.zip"
    flags = ["--archive", archive.as_uri(), "--storage-driver", "obstore"]
    _cli(store, "release", "create", "1.0", *flags, exit_code=1)
    assert not archive.exists()
    assert _retention_files(store) == before  # no inventory, no release record


@pytest.mark.parametrize("name", ["a#b", "#tag", "x#1y"])
def test_obstore_release_keeps_a_hash_name_its_listing_always_showed(tmp_path, name):
    store = _store(tmp_path, 3)
    (store / name).write_text("listed")
    args = ("--archive", (tmp_path / "one.zip").as_uri(), "--storage-driver", "obstore")
    release = _json(store, "release", "create", "1.0", *args)["release"]
    assert name in _inventory(store, release)
    assert name in _entries(tmp_path / "one.zip")


def test_release_whose_incremental_backup_would_delete_a_newline_named_key_is_refused_with_archive_full(
    tmp_path,
):
    store = _store(tmp_path, 3)
    driver = ("--storage-driver", "fsspec")
    (store / "line\nbreak").write_text("x")
    _archived(store, "1.0", tmp_path / "one.zip", *driver)
    (store / "line\nbreak").unlink()
    before = _retention_files(store)
    flags = ["--archive", (tmp_path / "two.zip").as_uri(), *driver]
    assert "--archive-full" in _cli(store, "release", "create", "2.0", *flags, exit_code=1).output
    assert not (tmp_path / "two.zip").exists()
    assert _retention_files(store) == before


def test_release_verify_reports_a_deleted_previous_backup_as_a_broken_backup_chain(tmp_path):
    store = _store(tmp_path, 3)
    _archived(store, "1.0", tmp_path / "one.zip")
    _archived(store, "2.0", tmp_path / "two.zip")
    (tmp_path / "one.zip").unlink()
    check = json.loads(_cli(store, "release", "verify", "-f", "json", exit_code=1).stdout)
    assert (check["retention"]["archive"], check["retention"]["backup_chain"]) == (
        "present",
        "broken",
    )
    assert check["retention"]["broken_at"] == "1.0"
    assert "backup chain: broken at 1.0" in _cli(store, "release", "verify", exit_code=1).output


@FORMATS
def test_release_record_without_retention_still_shows_and_verifies(tmp_path, fmt):
    store = _store(tmp_path, fmt)
    _cli(store, "release", "create", "1.0")
    record = next(p for p in (store / ".firecube/releases").glob("*.json"))
    assert "retention" not in json.loads(record.read_text())  # a record as v1 wrote it
    assert _json(store, "release", "show").get("retention") is None
    assert _json(store, "release", "verify")["matches"] is True
    assert _json(store, "release", "list")["current"] == "1.0"


_BUCKET = "retain-bucket"
_ROOT = b'{"zarr_format":3,"node_type":"group","attributes":{}}'
_S3 = pytest.mark.s3


@pytest.fixture(scope="module")
def moto_endpoint() -> Iterator[str]:
    from moto.server import ThreadedMotoServer  # S3 lane only: the default lane needs no moto

    server = ThreadedMotoServer(port=0)
    server.start()
    host, port = server.get_host_and_port()
    try:
        yield f"http://{host}:{port}"
    finally:
        server.stop()


@pytest.fixture
def s3_client(moto_endpoint: str) -> Iterator[Any]:
    """An unversioned bucket; teardown deletes every version and delete marker first."""
    import boto3

    keys = {"aws_access_key_id": "testing", "aws_secret_access_key": "testing"}
    client = boto3.client("s3", endpoint_url=moto_endpoint, region_name="us-east-1", **keys)
    client.create_bucket(Bucket=_BUCKET)
    try:
        yield client
    finally:  # moto cannot drop a suspended bucket's delete markers, so re-enable versioning
        client.put_bucket_versioning(Bucket=_BUCKET, VersioningConfiguration={"Status": "Enabled"})
        resource: Any = boto3.resource("s3", endpoint_url=moto_endpoint, **keys)
        resource.Bucket(_BUCKET).object_versions.delete()
        client.delete_bucket(Bucket=_BUCKET)


@pytest.fixture
def versioned(s3_client: Any) -> Any:
    s3_client.put_bucket_versioning(Bucket=_BUCKET, VersioningConfiguration={"Status": "Enabled"})
    return s3_client


def _s3_cli(client: Any, *args: str, exit_code: int = 0):
    target = ["--target", f"s3://{_BUCKET}/p.zarr", "--product-name", "p.zarr"]
    env = s3_env(
        client.meta.endpoint_url, access_key="testing", secret_key="testing", region="us-east-1"
    )
    out = CliRunner().invoke(cli, [*args, *target], env=env)
    assert out.exit_code == exit_code, out.output
    return out


def _put(client: Any, key: str, body: bytes = b"x") -> str:
    return client.put_object(Bucket=_BUCKET, Key=f"p.zarr/{key}", Body=body)["VersionId"]


def _adopted(client: Any) -> None:
    client.put_object(Bucket=_BUCKET, Key="p.zarr/zarr.json", Body=_ROOT)
    _s3_cli(client, "migrate", "adopt")


def _tree(client: Any) -> set[tuple[str, str, bool]]:
    """Every (key, version id, is delete marker) of the bucket, read with boto3."""
    pages = client.get_paginator("list_object_versions").paginate(Bucket=_BUCKET)
    return {
        (e["Key"], e["VersionId"], kind == "DeleteMarkers")
        for page in pages
        for kind in ("Versions", "DeleteMarkers")
        for e in page.get(kind, [])
    }


_TMP = ".firecube-publish-0123456789abcdef.tmp"  # a literal publication-temporary key name


def _latest(client: Any) -> dict[str, tuple[str, bool]]:
    """(version id, is delete marker) of the latest entry of every key under the product."""
    pages = client.get_paginator("list_object_versions").paginate(Bucket=_BUCKET, Prefix="p.zarr/")
    return {
        e["Key"].removeprefix("p.zarr/"): (e["VersionId"], kind == "DeleteMarkers")
        for page in pages
        for kind in ("Versions", "DeleteMarkers")
        for e in page.get(kind, [])
        if e["IsLatest"]
    }


@pytest.fixture
def retained(versioned: Any, monkeypatch) -> tuple[dict[str, tuple[str, bool]], str]:
    """The expected inventory (read with boto3 as `write_inventory` is entered, exclusions written
    out) and a superseded id; listing pages of 4. Two literal publication-temporary keys sit in
    the bucket."""
    monkeypatch.setattr(fsspec_backend, "LIST_VERSIONS_PAGE_SIZE", 4)
    cut: dict[str, tuple[str, bool]] = {}
    real = ChunkManager.write_inventory

    def spy(self: ChunkManager, product: str, inventory: Any) -> str:
        cut.update(
            {
                k: v
                for k, v in _latest(versioned).items()
                if not k.startswith(".firecube/claims/") and not k.endswith(_TMP)
            }
        )
        return real(self, product, inventory)

    monkeypatch.setattr(ChunkManager, "write_inventory", spy)
    _adopted(versioned)
    superseded = _put(versioned, "c/0", b"one")
    _put(versioned, "c/0", b"two")
    _put(versioned, "c/1", b"soon gone")
    versioned.delete_object(Bucket=_BUCKET, Key="p.zarr/c/1")
    _put(versioned, "c/2", b"three")
    _put(versioned, _TMP)
    _put(versioned, f"c/{_TMP}")
    _s3_cli(versioned, "release", "create", "1.0", "--retain-versions")
    monkeypatch.setattr(ChunkManager, "write_inventory", real)
    return cut, superseded


@pytest.fixture
def head_calls(monkeypatch) -> tuple[list[tuple[str, str]], set[str]]:
    """Every (key, VersionId) HEAD sent by the real aiobotocore client; keys in the set fail as a
    dead socket (EndpointConnectionError). Request it after `retained`: creation HEADs too."""
    from aiobotocore.client import AioBaseClient
    from botocore.exceptions import EndpointConnectionError

    calls: list[tuple[str, str]] = []
    failing: set[str] = set()
    real = AioBaseClient._make_api_call

    async def make_api_call(self: Any, operation: str, params: dict[str, Any]) -> Any:
        if operation == "HeadObject" and "VersionId" in params:
            calls.append((params["Key"], params["VersionId"]))
            if params["Key"] in failing:
                raise EndpointConnectionError(endpoint_url="http://unreachable")
        return await real(self, operation, params)

    monkeypatch.setattr(AioBaseClient, "_make_api_call", make_api_call)
    return calls, failing


@_S3
def test_retain_versions_records_the_latest_version_of_each_key_and_a_deleted_key_as_marker(
    versioned, retained
):
    cut, superseded = retained
    release = json.loads(_s3_cli(versioned, "release", "show", "-f", "json").stdout)
    key = f"p.zarr/.firecube/releases/inventories/{release['retention']['inventory']}.json"
    objects = json.loads(versioned.get_object(Bucket=_BUCKET, Key=key)["Body"].read())["objects"]
    recorded = {o["key"]: (o["version_id"], bool(o.get("delete_marker"))) for o in objects}
    # `cut` is the bucket's latest entry of every key, read with boto3 as the inventory was handed
    # to `write_inventory`, minus claims and publication temporaries. The whole mapping is compared.
    assert recorded == cut
    assert {".firecube/metadata/refs/latest.json", "zarr.json"} <= cut.keys()
    assert any(k.startswith(".firecube/runs/release-") for k in cut)
    assert superseded not in {vid for vid, _ in recorded.values()}
    assert release["retention"]["object_count"] == sum(not dm for _, dm in cut.values())


@_S3
def test_verify_reports_every_retained_version_intact_and_all_checked(versioned, retained):
    out = _s3_cli(versioned, "release", "verify").output
    assert {"retention: unchanged", "inventory: intact", "checked: all"} <= set(out.splitlines())


@_S3
def test_verify_reports_a_deleted_retained_version_as_lost_and_exits_1(versioned, retained):
    versioned.delete_object(Bucket=_BUCKET, Key="p.zarr/c/2", VersionId=retained[0]["c/2"][0])
    out = _s3_cli(versioned, "release", "verify", exit_code=1).output
    assert {"inventory: lost 1", "  lost: c/2"} <= set(out.splitlines())


@_S3
def test_verify_reports_a_transport_failure_as_lost_and_still_checks_every_other_version(
    versioned, retained, head_calls
):
    calls, failing = head_calls
    failing.add("p.zarr/c/0")
    out = _s3_cli(versioned, "release", "verify", exit_code=1).output
    assert {"inventory: lost 1", "  lost: c/0"} <= set(out.splitlines())
    listed = {(f"p.zarr/{k}", vid) for k, (vid, marker) in retained[0].items() if not marker}
    assert len(listed) > 3 and listed <= set(calls)


@_S3
def test_verify_reports_suspended_bucket_versioning_as_retention_changed(versioned, retained):
    versioned.put_bucket_versioning(Bucket=_BUCKET, VersioningConfiguration={"Status": "Suspended"})
    assert "retention: changed" in _s3_cli(versioned, "release", "verify", exit_code=1).output


@_S3
def test_verify_sample_checks_the_number_of_versions_asked_for(versioned, retained, head_calls):
    out = _s3_cli(versioned, "release", "verify", "--sample", "1").output
    assert {"inventory: intact", "checked: sample 1"} <= set(out.splitlines())
    assert len(head_calls[0]) == 1  # one HEAD, not the whole inventory


@_S3
@pytest.mark.parametrize(
    ("case", "text", "exit_code"),
    [
        ("unversioned", "has versioning absent", 1),
        ("null-version", "version id", 1),
        ("obstore", "fsspec", 1),
        ("archive", "mutually exclusive", 2),
    ],
)
def test_retain_versions_refusal_leaves_every_version_and_marker_untouched(
    s3_client, tmp_path, case, text, exit_code
):
    on = {"Bucket": _BUCKET, "VersioningConfiguration": {"Status": "Enabled"}}
    if case == "null-version":
        _adopted(s3_client)  # written before versioning is enabled: version id "null"
    if case != "unversioned":
        s3_client.put_bucket_versioning(**on)
    if case != "null-version":
        _adopted(s3_client)
    flags = {"obstore": ["--storage-driver", "obstore"], "archive": ["--archive", "file:///a.zip"]}
    before = _tree(s3_client)
    out = _s3_cli(
        s3_client,
        "release",
        "create",
        "1.0",
        "--retain-versions",
        *flags.get(case, []),
        exit_code=exit_code,
    )
    assert text in out.output
    assert _tree(s3_client) == before
