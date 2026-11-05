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
import shutil
import sys
import zipfile
from types import SimpleNamespace

import numpy as np
import pytest
import xarray as xr
import zarr
from click.testing import CliRunner

from firecube.cli.main import cli

_LOCAL_STORAGE_FLAGS = ["--storage-type", "local", "--storage-driver", "fsspec"]


def test_create_help_advertises_yes_flag():
    r = CliRunner().invoke(cli, ["archive", "create", "--help"])
    assert r.exit_code == 0
    assert "--overwrite" in r.output
    assert "--yes-i-really-mean-it" in r.output


def test_restore_help_advertises_yes_flag():
    r = CliRunner().invoke(cli, ["archive", "restore", "--help"])
    assert r.exit_code == 0
    assert "--overwrite" in r.output
    assert "--yes-i-really-mean-it" in r.output
    assert "--group" not in r.output


def test_info_help_uses_format_option_not_json_flag():
    r = CliRunner().invoke(cli, ["archive", "info", "--help"])
    assert r.exit_code == 0
    assert "-f, --format" in r.output or "--format" in r.output
    assert "--json" not in r.output


def test_info_rejects_legacy_json_flag(tmp_path):
    target = tmp_path / "missing.tgm"
    target.write_text("")
    r = CliRunner().invoke(cli, ["archive", "info", "--json", str(target)])
    assert r.exit_code != 0
    assert "no such option" in r.output.lower() or "--json" in r.output


class _FakeTensogramMetadata:
    def __init__(self) -> None:
        self.extra = {
            "firecube": {
                "archive_version": "v1",
                "role": "data",
                "group": "F024",
                "compression": "zstd",
                "source_uri": "file:///tmp/product.zarr",
                "archived_at": "2026-06-09T00:00:00+00:00",
            }
        }
        self.base: list[object] = []


class _FakeTensogramArchive:
    def __enter__(self) -> _FakeTensogramArchive:
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        return None

    def message_count(self) -> int:
        return 1

    def file_decode_metadata(self, index: int) -> _FakeTensogramMetadata:
        assert index == 0
        return _FakeTensogramMetadata()

    def file_decode_descriptors(self, index: int) -> dict[str, list[object]]:
        assert index == 0
        return {"descriptors": []}


def _install_fake_tensogram(monkeypatch: pytest.MonkeyPatch) -> None:
    class _FakeTensogramFile:
        @staticmethod
        def open(path: str) -> _FakeTensogramArchive:
            assert path
            return _FakeTensogramArchive()

    monkeypatch.setitem(
        sys.modules,
        "tensogram",
        SimpleNamespace(TensogramFile=_FakeTensogramFile),
    )

    import firecube.core.tensogram._compat as compat

    monkeypatch.setattr(compat, "HAS_TENSOGRAM", True)


def test_info_reports_archive_version_in_table(tmp_path, monkeypatch: pytest.MonkeyPatch):
    _install_fake_tensogram(monkeypatch)
    archive = tmp_path / "archive.tgm"
    archive.write_bytes(b"fake")

    result = CliRunner().invoke(cli, ["archive", "info", "--archive", archive.as_uri()])

    assert result.exit_code == 0, result.output
    assert "Format:         v1 (multi-group)" in result.output


def test_info_reports_archive_version_in_json(tmp_path, monkeypatch: pytest.MonkeyPatch):
    _install_fake_tensogram(monkeypatch)
    archive = tmp_path / "archive.tgm"
    archive.write_bytes(b"fake")

    result = CliRunner().invoke(
        cli, ["archive", "info", "--archive", archive.as_uri(), "-f", "json"]
    )

    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["format"] == "v1"


@pytest.mark.contract
def test_list_prints_earlier_messages_before_a_late_decode_failure(tmp_path, monkeypatch):
    def decode(self, index):
        if index:
            raise ValueError("late decode failure")
        return _FakeTensogramMetadata()

    _install_fake_tensogram(monkeypatch)
    monkeypatch.setattr(_FakeTensogramArchive, "message_count", lambda self: 2)
    monkeypatch.setattr(_FakeTensogramArchive, "file_decode_metadata", decode)
    archive = tmp_path / "archive.tgm"
    archive.write_bytes(b"fake")
    result = CliRunner().invoke(cli, ["archive", "list", "--archive", archive.as_uri()])
    assert (result.exit_code, str(result.exception)) == (1, "late decode failure")
    assert result.stdout == (
        "Messages: 2\n\nGroup: F024\n  Source:  file:///tmp/product.zarr\n"
        "  Codec:   zstd\n  Objects: 0\n"
    )


def test_create_overwrite_non_tty_without_yes_exits_nonzero(tmp_path):
    src = tmp_path / "src.zarr"
    src.mkdir()
    tgt = tmp_path / "out.tgm"
    r = CliRunner().invoke(
        cli,
        [
            "archive",
            "create",
            "--source",
            f"file://{src}",
            "--archive",
            tgt.as_uri(),
            "--overwrite",
            *_LOCAL_STORAGE_FLAGS,
        ],
    )
    assert r.exit_code != 0
    assert (
        "yes-i-really-mean-it" in r.output.lower()
        or "overwritewithoutconfirmation" in r.output.lower()
    )


def test_restore_overwrite_non_tty_without_yes_exits_nonzero(tmp_path):
    src = tmp_path / "archive.tgm"
    src.write_text("")
    tgt = tmp_path / "out.zarr"
    r = CliRunner().invoke(
        cli,
        [
            "archive",
            "restore",
            "--archive",
            src.as_uri(),
            "--target",
            f"file://{tgt}",
            "--overwrite",
            *_LOCAL_STORAGE_FLAGS,
        ],
    )
    assert r.exit_code != 0
    assert (
        "yes-i-really-mean-it" in r.output.lower()
        or "overwritewithoutconfirmation" in r.output.lower()
    )


def test_zip_restore_refuses_entry_outside_target(tmp_path):
    archive = tmp_path / "evil.zip"
    with zipfile.ZipFile(archive, "w") as zf:
        zf.writestr("zarr.json", json.dumps({"zarr_format": 3, "node_type": "group"}))
        zf.writestr("../escape.txt", "evil")
    args = ["archive", "restore", "--archive", archive.as_uri()]
    args += ["--target", f"file://{tmp_path}/r.zarr", *_LOCAL_STORAGE_FLAGS]
    r = CliRunner().invoke(cli, args)
    assert r.exit_code != 0, r.output
    assert "../escape.txt" in r.output + str(r.exception)
    assert not (tmp_path / "escape.txt").exists()


@pytest.mark.parametrize("kind", ["tgm", "zip-root", "zip-consolidated-child"])
def test_overwrite_restore_of_an_invalid_archive_keeps_the_target(tmp_path, kind):
    src = tmp_path / "src"
    zarr.open_group(str(src), mode="w").create_array("a", data=np.arange(6.0))
    zarr.consolidate_metadata(str(src))
    entries = {p.relative_to(src).as_posix(): p.read_bytes() for p in src.rglob("*") if p.is_file()}
    bad = b'{"zarr_format": 3, "node_type": "banana"}'
    archive = tmp_path / ("a.tgm" if kind == "tgm" else "a.zip")
    if kind == "tgm":
        archive.write_bytes(b"not a tensogram archive")
    else:
        entries["zarr.json" if kind == "zip-root" else "a/zarr.json"] = bad
        with zipfile.ZipFile(archive, "w") as zf:
            for key, data in entries.items():
                zf.writestr(key, data)
    target = tmp_path / "t.zarr"
    zarr.open_group(str(target), mode="w").create_array("keep", data=np.arange(3.0))
    (target / ".firecube").mkdir()
    (target / ".firecube" / "schema.json").write_text('{"kept": true}')

    def tree() -> dict[str, bytes]:
        files = (p for p in target.rglob("*") if p.is_file())
        return {p.relative_to(target).as_posix(): p.read_bytes() for p in files}

    before = tree()
    args = ["archive", "restore", "--archive", archive.as_uri(), "--target", target.as_uri()]
    r = CliRunner().invoke(
        cli, [*args, "--overwrite", "--yes-i-really-mean-it", *_LOCAL_STORAGE_FLAGS]
    )
    after = {k: v for k, v in tree().items() if not k.startswith(".firecube/runs/archive-restore-")}
    assert after == before
    assert r.exit_code != 0, r.output


@pytest.mark.parametrize(("snapshot", "child"), [(None, "G"), ("G", "G"), ("G/H", "G/H")])
def test_validate_rejects_a_broken_child_the_snapshot_still_shows_valid(tmp_path, snapshot, child):
    src = tmp_path / "src"
    root = zarr.open_group(str(src), mode="w", zarr_format=3)
    root.require_group(child).create_array("a", data=np.arange(4.0))
    zarr.consolidate_metadata(str(src))
    if snapshot:
        zarr.consolidate_metadata(str(src / snapshot))
    broken = src / child / "a" / "zarr.json"
    broken.write_text(json.dumps({**json.loads(broken.read_text()), "node_type": "banana"}))
    archive = shutil.make_archive(str(tmp_path / "a"), "zip", src)
    r = CliRunner().invoke(cli, ["archive", "validate", "--archive", f"file://{archive}"])
    assert r.exit_code == 1, r.output
    assert "INVALID:" in r.output and "cannot open as Zarr:" in r.output


def test_create_refuses_anonymous_storage(tmp_path, monkeypatch):
    monkeypatch.setenv("FIRECUBE_S3_ANONYMOUS", "false")  # the CLI exports it; teardown unsets
    u = tmp_path.as_uri()
    cmd = f"archive create --source {u}/s --archive {u}/o.zip --storage-anonymous"
    r = CliRunner().invoke(cli, cmd.split())
    assert r.exit_code == 2, r.output
    assert not (tmp_path / "o.zip").exists()


def test_zip_restore_cli_preserves_array_values(tmp_path):
    u = tmp_path.as_uri()
    zarr.open_group(str(tmp_path / "s"), mode="w").create_array("a", data=np.arange(6.0))
    for cmd in (f"create --source {u}/s --archive-format zip", f"restore --target {u}/r"):
        r = CliRunner().invoke(cli, ["archive", *cmd.split(), "--archive", f"{u}/a.zip"])
        assert r.exit_code == 0, r.output
    restored = zarr.open_group(str(tmp_path / "r"))["a"]
    assert isinstance(restored, zarr.Array)
    assert np.asarray(restored[:]).tolist() == np.arange(6.0).tolist()


def test_create_without_overwrite_requires_no_confirmation(tmp_path, monkeypatch):
    monkeypatch.delenv("FIRECUBE_STORAGE_TYPE", raising=False)
    monkeypatch.delenv("FIRECUBE_TARGET_PATH", raising=False)
    src = tmp_path / "missing.zarr"
    tgt = tmp_path / "out.tgm"
    r = CliRunner().invoke(
        cli,
        [
            "archive",
            "create",
            "--source",
            f"file://{src}",
            "--archive",
            tgt.as_uri(),
            *_LOCAL_STORAGE_FLAGS,
        ],
    )
    assert "OverwriteWithoutConfirmation" not in r.output
    assert "yes-i-really-mean-it" not in r.output.lower()


def _release_chain(base):
    """Full archive of release 1.0 and the delta of 1.1, which deletes `gone/*` and `a*b`."""
    store = base / "p.zarr"
    xr.Dataset({"temp": (("x",), np.arange(3.0))}).to_zarr(store, zarr_format=3)
    zarr.open_group(str(store), mode="a").create_array("gone", data=np.arange(3.0))
    (store / "a*b").write_text("star")
    (store / "aXb").write_text("sibling")
    full, delta = base / "a1.zip", base / "a2.zip"
    where = ["--target", store.as_uri(), "--product-name", "p.zarr"]

    def run(*args):
        assert CliRunner().invoke(cli, [*args, *where]).exit_code == 0

    run("migrate", "adopt")
    run("release", "create", "1.0", "--archive", full.as_uri())
    shutil.rmtree(store / "gone")
    (store / "a*b").unlink()
    run("release", "create", "1.1", "--archive", delta.as_uri())
    return full, delta


def _rewrite(src, dst, edit=None, extra=()):
    """Copy zip `src` to `dst`, passing the parsed marker through `edit`, adding `extra` entries."""
    with zipfile.ZipFile(src) as zin, zipfile.ZipFile(dst, "w") as zout:
        for info in zin.infolist():
            data = zin.read(info)
            if edit and info.filename == "firecube-archive.json":
                data = edit(json.loads(data))
                data = data if isinstance(data, bytes) else json.dumps(data).encode()
            zout.writestr(info.filename, data)
        for name in extra:
            zout.writestr(name, b"unchecked")


def _tree(root):
    return {
        p.relative_to(root).as_posix(): p.read_bytes() if p.is_file() else None
        for p in root.rglob("*")
    }


def _restore(archive, target, *flags):
    args = ["archive", "restore", "--archive", archive.as_uri(), "--target", target.as_uri()]
    return CliRunner().invoke(cli, [*args, *flags, *_LOCAL_STORAGE_FLAGS])


_BAD_MARKERS = {
    "mode-missing": (lambda m: {k: v for k, v in m.items() if k != "mode"}, "keys differ"),
    "mode-misspelled": (lambda m: {**m, "mode": "dleta"}, "dleta"),
    "marker-not-json": (lambda m: b"{not json", "not valid JSON"),
    "deleted-parent": (lambda m: {**m, "deleted": ["../x"]}, "'..'"),
    "deleted-claim": (lambda m: {**m, "deleted": [".firecube/claims/a"]}, "claim file"),
}


@pytest.mark.parametrize("case", [*_BAD_MARKERS, "base-missing", "base-changed"])
def test_refused_chain_restore_changes_nothing_not_even_a_run_record(tmp_path, case):
    full, delta = _release_chain(tmp_path)
    if case == "base-missing":
        full.unlink()
    elif case == "base-changed":
        _rewrite(full, tmp_path / "swap.zip", extra=("added",))
        (tmp_path / "swap.zip").replace(full)
    else:
        _rewrite(delta, tmp_path / "bad.zip", edit=_BAD_MARKERS[case][0])
        delta = tmp_path / "bad.zip"
    reason = {"base-missing": "does not exist", "base-changed": "digest"}.get(case)
    target = tmp_path / "t.zarr"
    zarr.open_group(str(target), mode="w").create_array("keep", data=np.arange(3.0))
    (target / ".firecube").mkdir()
    (target / ".firecube" / "schema.json").write_text('{"kept": true}')
    before = _tree(target)
    r = _restore(delta, target, "--overwrite", "--yes-i-really-mean-it")
    assert r.exit_code != 0 and (reason or _BAD_MARKERS[case][1]) in r.output, r.output
    assert _tree(target) == before


def test_delta_deleting_a_glob_named_key_keeps_its_sibling(tmp_path):
    r = _restore(_release_chain(tmp_path)[1], tmp_path / "t.zarr")
    assert r.exit_code == 0, r.output
    assert (tmp_path / "t.zarr" / "aXb").read_text() == "sibling"
    assert not (tmp_path / "t.zarr" / "a*b").exists()


def test_delta_restore_fails_when_a_key_delete_fails(tmp_path, monkeypatch):
    from firecube.core.storage.session import StorageSession

    def refuse(self, uri):
        raise OSError("delete refused")

    monkeypatch.setattr(StorageSession, "delete_literal", refuse)
    r = _restore(_release_chain(tmp_path)[1], tmp_path / "t.zarr")
    assert r.exit_code != 0 and "delete refused" in r.output + str(r.exception), r.output


@pytest.mark.parametrize("linked", ["temp", "gone"], ids=["put", "delete"])
def test_symlinked_ancestor_in_the_target_is_refused_and_the_outside_survives(tmp_path, linked):
    delta = _release_chain(tmp_path)[1]
    (tmp_path / "outside").mkdir()
    (tmp_path / "outside" / "0").write_text("precious")
    (tmp_path / "t.zarr").mkdir()
    (tmp_path / "t.zarr" / linked).symlink_to(tmp_path / "outside")
    r = _restore(delta, tmp_path / "t.zarr")
    assert r.exit_code != 0 and "symlink" in r.output, r.output
    assert _tree(tmp_path / "outside") == {"0": b"precious"}


def test_base_swapped_after_the_chain_was_resolved_lands_no_unchecked_object(tmp_path, monkeypatch):
    import firecube.core.zarr.zip_chain as zip_chain

    full, delta = _release_chain(tmp_path)
    resolve = zip_chain.resolve_chain

    def resolve_then_swap(path):
        links = resolve(path)
        _rewrite(full, tmp_path / "swap.zip", extra=("evil.txt",))
        (tmp_path / "swap.zip").replace(full)
        return links

    monkeypatch.setattr(zip_chain, "resolve_chain", resolve_then_swap)
    r = _restore(delta, tmp_path / "t.zarr")
    assert r.exit_code != 0 and "digest" in r.output, r.output
    assert [k for k in _tree(tmp_path / "t.zarr") if not k.startswith(".firecube")] == []
