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

import zipfile

import pytest
import zarr
from click.testing import CliRunner

from firecube.cli.main import cli
from firecube.core.controlplane.product_label import stamp_product_label
from firecube.core.filesystem import create_filesystem
from tests.helpers.storage import make_test_binding


def _invoke_restore(args: list[str]):
    return CliRunner().invoke(cli, ["archive", "restore", *args])


def _run(*args: str):
    confirm = (
        ["--yes-i-really-mean-it"]
        if args[:2] in {("migrate", "adopt"), ("release", "create")}
        else []
    )
    return CliRunner().invoke(cli, [*args, *confirm])


def _archive(root, fmt="zip", label: str | None = "LBL"):
    root.mkdir(exist_ok=True)
    store, archive = root / "s.zarr", root / ("a.zip" if fmt == "zip" else "a.tgm")
    zarr.open_group(str(store), mode="w").create_array("a", shape=(3,), dtype="f8")
    if label:
        _run("migrate", "adopt", "--target", store.as_uri(), "--product-name", label)
    args = ["--target", store.as_uri(), "--archive", archive.as_uri(), "--archive-format", fmt]
    assert _run("archive", "create", *args).exit_code == 0
    return store, archive


def _rewrite(archive, edit):
    out = archive.with_name("x.zip")
    with zipfile.ZipFile(archive) as zin, zipfile.ZipFile(out, "w") as zout:
        for info in zin.infolist():
            if (data := edit(info.filename, zin.read(info))) is not None:
                zout.writestr(info.filename, data)
    return out


@pytest.mark.contract
@pytest.mark.parametrize(
    ("fmt", "label", "legacy", "code"),
    [("zip", "LBL", 0, 0), ("tensogram", "LBL", 0, 0), ("zip", "LBL", 1, 0), ("zip", None, 0, 2)],
    ids=["zip-stamp", "tensogram-stamp", "legacy-run-labels", "no-label"],
)
def test_restore_resolves_the_label_before_any_write(tmp_path, fmt, label, legacy, code) -> None:
    archive = _archive(tmp_path, fmt, label)[1]
    if legacy:
        archive = _rewrite(archive, lambda n, d: None if n.endswith("product.json") else d)
    target = tmp_path / "r.zarr"
    result = _invoke_restore(["--archive", archive.as_uri(), "--target", target.as_uri()])
    assert result.exit_code == code, result.output
    if code:
        assert not target.exists() and "--product-name" in result.output
    else:
        assert "LBL" in (target / ".firecube/product.json").read_text()


@pytest.mark.contract
def test_restore_onto_a_target_stamped_with_another_label_is_refused_untouched(tmp_path) -> None:
    target, archive = _archive(tmp_path / "o", label="OTHER")[0], _archive(tmp_path)[1]
    files = lambda: {p: p.read_bytes() for p in target.rglob("*") if p.is_file()}  # noqa: E731
    before = files()
    args = ["--archive", archive.as_uri(), "--target", target.as_uri()]
    result = _invoke_restore([*args, "--overwrite", "--yes-i-really-mean-it"])
    assert result.exit_code == 2 and files() == before


@pytest.mark.contract
def test_restore_dry_run_refuses_a_label_conflict_as_the_real_run_does(tmp_path) -> None:
    target, archive = _archive(tmp_path / "o", label="OTHER")[0], _archive(tmp_path)[1]
    files = lambda: {p: p.read_bytes() for p in target.rglob("*") if p.is_file()}  # noqa: E731
    before = files()
    args = ["--archive", archive.as_uri(), "--target", target.as_uri()]
    args += ["--overwrite", "--yes-i-really-mean-it"]

    real = _invoke_restore(args)
    dry = _invoke_restore([*args, "--dry-run"])

    assert real.exit_code == 2 and "OTHER" in real.output
    assert dry.exit_code == real.exit_code, dry.output
    assert "OTHER" in dry.output and "[dry-run] Would restore" not in dry.output
    assert files() == before


@pytest.mark.contract
@pytest.mark.parametrize("fmt", ["zip", "tensogram"])
def test_overwrite_restore_keeps_the_targets_stamp_of_the_same_label(tmp_path, fmt) -> None:
    archive = _archive(tmp_path, fmt)[1]
    binding = make_test_binding(tmp_path, product="r.zarr")
    stamp_product_label(create_filesystem(binding), binding.identity.control_root_uri, "LBL")
    target = tmp_path / "r.zarr"
    zarr.open_group(str(target), mode="a").create_array("old", shape=(1,), dtype="f8")
    before = (target / ".firecube/product.json").read_bytes()
    args = ["--archive", archive.as_uri(), "--target", target.as_uri()]
    result = _invoke_restore([*args, "--overwrite", "--yes-i-really-mean-it"])
    assert result.exit_code == 0, result.output
    assert (target / ".firecube/product.json").read_bytes() == before
    assert not (target / "old").exists() and (target / "a/zarr.json").exists()


@pytest.mark.contract
def test_restore_refuses_a_backup_chain_member_with_another_label(tmp_path) -> None:
    store, member = _archive(tmp_path)[0], tmp_path / "i.zip"
    for version, path in (("1.0", tmp_path / "f.zip"), ("1.1", member)):
        zarr.open_array(store / "a", mode="r+")[0] = float(version[-1]) + 1
        args = ["--target", store.as_uri(), "--archive", path.as_uri()]
        assert _run("release", "create", version, *args).exit_code == 0
    other = _rewrite(member, lambda n, d: d.replace(b'"product":"LBL"', b'"product":"OTHER"'))
    result = _invoke_restore(["--archive", other.as_uri(), "--target", (tmp_path / "r").as_uri()])
    assert result.exit_code == 1 and "'OTHER'" in result.output


def test_restore_rejects_storage_type_target_uri_mismatch() -> None:
    result = _invoke_restore(
        [
            "--archive",
            "file:///tmp/x.tgm",
            "--target",
            "file:///tmp/x.zarr",
            "--storage-type",
            "s3",
            "--storage-driver",
            "fsspec",
        ]
    )

    assert result.exit_code == 2
    assert "incompatible" in result.output
    assert "Traceback" not in result.output


@pytest.mark.contract
def test_restore_rejects_bare_tgm_archive_path() -> None:
    result = _invoke_restore(
        ["--archive", "/tmp/foo.tgm", "--target", "file:///tmp/x.zarr", "--dry-run"]
    )

    assert result.exit_code == 2
    assert "URI scheme required (file:// or s3://). Did you mean file:///tmp/foo.tgm?" in (
        result.output
    )
    assert "Traceback" not in result.output
    assert "[dry-run] Would restore" not in result.output


@pytest.mark.contract
def test_restore_rejects_bare_zip_archive_path() -> None:
    result = _invoke_restore(
        ["--archive", "/tmp/foo.zip", "--target", "file:///tmp/x.zarr", "--dry-run"]
    )

    assert result.exit_code == 2
    assert "URI scheme required (file:// or s3://). Did you mean file:///tmp/foo.zip?" in (
        result.output
    )
    assert "Traceback" not in result.output
    assert "[dry-run] Would restore" not in result.output


def test_restore_rejects_remote_tgm_archive() -> None:
    result = _invoke_restore(
        [
            "--archive",
            "s3://bucket/a.tgm",
            "--target",
            "file:///tmp/x.zarr",
            "--storage-type",
            "local",
            "--storage-driver",
            "fsspec",
        ]
    )

    assert result.exit_code != 0
    assert "Remote .tgm artifacts not yet supported" in result.output
    assert "Traceback" not in result.output
