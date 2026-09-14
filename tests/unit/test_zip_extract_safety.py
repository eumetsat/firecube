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
from collections.abc import Mapping
from pathlib import Path

import pytest

from firecube.core.formats.zip import (
    extract_all_from_zips,
    extract_hdf5_from_zip,
    stream_hdf5_from_zip,
)

pytestmark = pytest.mark.unit


def _write_zip(path: Path, members: Mapping[str, bytes]) -> None:
    with zipfile.ZipFile(path, "w") as zf:
        for name, data in members.items():
            zf.writestr(name, data)


def test_rejects_dotdot_member(tmp_path: Path) -> None:
    archive = tmp_path / "unsafe.zip"
    _write_zip(archive, {"../evil.h5": b"evil"})

    with pytest.raises(ValueError, match="Unsafe ZIP member"):
        extract_hdf5_from_zip(archive, tmp_path / "out")


def test_rejects_absolute_path_member(tmp_path: Path) -> None:
    archive = tmp_path / "unsafe.zip"
    _write_zip(archive, {"/tmp/evil.h5": b"evil"})

    with pytest.raises(ValueError, match="Unsafe ZIP member"):
        extract_hdf5_from_zip(archive, tmp_path / "out")


@pytest.mark.parametrize("member", [r"..\evil.h5", r"\\evil\path.h5", "C:/evil.h5"])
def test_rejects_windows_traversal(tmp_path: Path, member: str) -> None:
    archive = tmp_path / "unsafe.zip"
    _write_zip(archive, {member: b"evil"})

    with pytest.raises(ValueError, match="Unsafe ZIP member"):
        extract_hdf5_from_zip(archive, tmp_path / "out")


def test_rejects_nested_traversal(tmp_path: Path) -> None:
    archive = tmp_path / "unsafe.zip"
    _write_zip(archive, {"safe/../evil.h5": b"evil"})

    with pytest.raises(ValueError, match="Unsafe ZIP member"):
        extract_hdf5_from_zip(archive, tmp_path / "out")


def test_rejects_zero_hdf5_candidates(tmp_path: Path) -> None:
    archive = tmp_path / "empty.zip"
    _write_zip(archive, {"readme.txt": b"not hdf5"})

    with pytest.raises(ValueError, match="No HDF5 member"):
        extract_hdf5_from_zip(archive, tmp_path / "out")


def test_rejects_multiple_hdf5_candidates_without_member(tmp_path: Path) -> None:
    archive = tmp_path / "multi.zip"
    _write_zip(archive, {"a.h5": b"a", "b.h5": b"b"})

    with pytest.raises(ValueError, match=r"a\.h5.*b\.h5"):
        extract_hdf5_from_zip(archive, tmp_path / "out")


def test_accepts_multiple_with_explicit_member(tmp_path: Path) -> None:
    archive = tmp_path / "multi.zip"
    _write_zip(archive, {"a.h5": b"a", "b.h5": b"b"})

    extracted = extract_hdf5_from_zip(archive, tmp_path / "out", member="a.h5")

    assert extracted is not None
    assert extracted == tmp_path / "out" / "a.h5"
    assert extracted.read_bytes() == b"a"


def test_accepts_safe_nested_path(tmp_path: Path) -> None:
    archive = tmp_path / "nested.zip"
    _write_zip(archive, {"data/product.h5": b"safe"})

    extracted = extract_hdf5_from_zip(archive, tmp_path / "out")

    streamed = stream_hdf5_from_zip(archive)

    assert extracted is not None
    assert extracted == tmp_path / "out" / "data" / "product.h5"
    assert extracted.read_bytes() == b"safe"
    assert streamed == b"safe"


def test_stream_path_and_extract_path_share_validation(tmp_path: Path) -> None:
    archive = tmp_path / "unsafe.zip"
    _write_zip(archive, {"safe/../evil.h5": b"evil"})

    with pytest.raises(ValueError, match="Unsafe ZIP member"):
        extract_hdf5_from_zip(archive, tmp_path / "out")
    with pytest.raises(ValueError, match="Unsafe ZIP member"):
        stream_hdf5_from_zip(archive)


def test_extract_all_from_zips_extracts_every_member(tmp_path: Path) -> None:
    archive = tmp_path / "nested.zip"
    _write_zip(archive, {"payload/a.txt": b"A", "payload/b.txt": b"B"})

    extracted, failures = extract_all_from_zips([archive], lambda p: tmp_path / "out" / p.stem)

    assert failures == {}
    out = extracted[archive]
    assert (out / "payload" / "a.txt").read_bytes() == b"A"
    assert (out / "payload" / "b.txt").read_bytes() == b"B"


def test_extract_all_from_zips_reports_unsafe_member_as_failure(tmp_path: Path) -> None:
    archive = tmp_path / "unsafe.zip"
    _write_zip(archive, {"../escape.txt": b"evil"})

    extracted, failures = extract_all_from_zips([archive], lambda p: tmp_path / "out" / p.stem)

    assert extracted == {}
    assert "Unsafe ZIP member" in failures[archive]
    assert not (tmp_path / "escape.txt").exists()
    # Partial output of a failed archive is removed.
    assert not (tmp_path / "out" / "unsafe").exists()


@pytest.mark.parametrize("workers", [1, 4])
def test_extract_all_from_zips_every_input_lands_in_one_mapping(
    tmp_path: Path, workers: int
) -> None:
    good = []
    for index in range(3):
        archive = tmp_path / f"good-{index}.zip"
        _write_zip(archive, {f"member-{index}.txt": bytes([index]) * 4})
        good.append(archive)
    corrupt = tmp_path / "corrupt.zip"
    corrupt.write_bytes(b"this is not a zip archive")
    evil = tmp_path / "evil.zip"
    _write_zip(evil, {"../escape.txt": b"evil"})

    extracted, failures = extract_all_from_zips(
        [*good, corrupt, evil],
        lambda p: tmp_path / "out" / p.stem,
        workers=workers,
    )

    assert sorted(extracted) == sorted(good)
    assert set(failures) == {corrupt, evil}
    dirs = list(extracted.values())
    assert len(set(dirs)) == len(dirs)
    for index, archive in enumerate(good):
        member = extracted[archive] / f"member-{index}.txt"
        assert member.read_bytes() == bytes([index]) * 4


def test_extract_all_from_zips_rejects_invalid_workers(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="workers must be >= 1"):
        extract_all_from_zips([], lambda p: tmp_path, workers=0)


@pytest.mark.parametrize("workers", [1, 4])
@pytest.mark.parametrize("unsafe", [False, True])
def test_failure_preserves_existing_destination(tmp_path, workers, unsafe):
    archive = tmp_path / "bad.zip"
    if unsafe:
        _write_zip(archive, {"../escape": b"bad"})
    else:
        archive.write_bytes(b"corrupt")
    dest = tmp_path / "existing"
    dest.mkdir()
    (dest / "sentinel").write_text("keep")
    good = tmp_path / "good.zip"
    _write_zip(good, {"data": b"good"})

    extracted, failures = extract_all_from_zips(
        [archive, good], lambda p: dest if p == archive else tmp_path / "good", workers=workers
    )

    assert set(failures) == {archive}
    assert extracted[good].is_dir()
    assert (dest / "sentinel").read_text() == "keep"


@pytest.mark.parametrize("workers", [1, 4])
@pytest.mark.parametrize("layout", ["same", "nested", "alias", "normalized"])
def test_failure_preserves_overlapping_success(tmp_path, workers, layout):
    good, bad = tmp_path / "good.zip", tmp_path / "bad.zip"
    _write_zip(good, {"payload": b"keep"})
    bad.write_bytes(b"corrupt")
    root = tmp_path / "output"
    good_dest = root / "child" if layout == "nested" else root
    bad_dest = root
    if layout == "alias":
        alias = tmp_path / "alias"
        alias.symlink_to(tmp_path, target_is_directory=True)
        bad_dest = alias / "output"
    elif layout == "normalized":
        bad_dest = root / ".." / "output"

    extracted, failures = extract_all_from_zips(
        [good, bad], lambda p: good_dest if p == good else bad_dest, workers=workers
    )

    assert set(failures) == {bad}
    assert (extracted[good] / "payload").read_bytes() == b"keep"


def _write_late_failure_zip(path):
    # Stored payload corruption fails its CRC only after the first member writes.
    _write_zip(path, {"first": b"partial", "second": b"UNIQUE_PAYLOAD"})
    path.write_bytes(path.read_bytes().replace(b"UNIQUE_PAYLOAD", b"BROKEN_PAYLOAD"))


@pytest.mark.parametrize("workers", [1, 4])
@pytest.mark.parametrize("existing", [False, True])
def test_late_failure_cleanup_boundary(tmp_path, workers, existing):
    bad, good = tmp_path / "bad.zip", tmp_path / "good.zip"
    _write_late_failure_zip(bad)
    _write_zip(good, {"data": b"good"})
    dest = tmp_path / "parent" / "bad"
    if existing:
        dest.mkdir(parents=True)
        (dest / "sentinel").write_bytes(b"keep")
    extracted, failures = extract_all_from_zips(
        [bad, good], lambda p: dest if p == bad else tmp_path / "good", workers=workers
    )
    assert "CRC" in failures[bad]
    assert extracted[good].is_dir()
    assert dest.parent.is_dir()
    if existing:
        assert (dest / "sentinel").read_bytes() == b"keep"
        assert (dest / "first").read_bytes() == b"partial"
    else:
        assert not dest.exists()


@pytest.mark.parametrize("state", ["new", "empty", "populated"])
def test_success_keeps_destination_and_overwrite_behavior(tmp_path, state):
    archive = tmp_path / "good.zip"
    _write_zip(archive, {"payload": b"new"})
    dest = tmp_path / "out"
    if state != "new":
        dest.mkdir()
    if state == "populated":
        (dest / "payload").write_bytes(b"old")
        (dest / "sentinel").write_bytes(b"keep")
    extracted, failures = extract_all_from_zips([archive], lambda p: dest)
    assert extracted == {archive: dest}
    assert failures == {}
    assert (dest / "payload").read_bytes() == b"new"
    if state == "populated":
        assert (dest / "sentinel").read_bytes() == b"keep"


@pytest.mark.parametrize("workers", [1, 4])
def test_repeated_archive_destination_is_nonexclusive(tmp_path, workers):
    archive = tmp_path / "bad.zip"
    _write_late_failure_zip(archive)
    dest = tmp_path / "out"
    calls = []

    def destination(path):
        # All destination callbacks run before any extraction starts.
        assert not dest.exists()
        calls.append(path)
        return dest

    extracted, failures = extract_all_from_zips([archive, archive], destination, workers=workers)
    assert calls == [archive, archive]
    assert extracted == {}
    assert set(failures) == {archive}
    assert (dest / "first").read_bytes() == b"partial"


def test_destination_created_by_another_caller_is_not_owned(tmp_path, monkeypatch):
    archive = tmp_path / "bad.zip"
    archive.write_bytes(b"corrupt")
    dest = tmp_path / "out"
    original_mkdir = Path.mkdir
    raced = False

    def racing_mkdir(path, *args, **kwargs):
        nonlocal raced
        if path == dest and not raced:
            raced = True
            original_mkdir(path)
            (path / "sentinel").write_text("other caller")
        return original_mkdir(path, *args, **kwargs)

    monkeypatch.setattr(Path, "mkdir", racing_mkdir)
    extracted, failures = extract_all_from_zips([archive], lambda p: dest)
    assert extracted == {}
    assert set(failures) == {archive}
    assert (dest / "sentinel").read_text() == "other caller"


def test_unusable_destination_does_not_abort_other_archives(tmp_path):
    good, bad = tmp_path / "good.zip", tmp_path / "bad.zip"
    _write_zip(good, {"data": b"good"})
    _write_zip(bad, {"data": b"bad"})
    parent_file = tmp_path / "file"
    parent_file.write_text("keep")
    extracted, failures = extract_all_from_zips(
        [bad, good],
        lambda p: parent_file / "child" if p == bad else tmp_path / "out",
    )
    assert set(failures) == {bad}
    assert (extracted[good] / "data").read_bytes() == b"good"
    assert parent_file.read_text() == "keep"
