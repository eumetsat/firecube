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

"""Resume-safety regression tests for the ``zarr_compression`` default flip.

The T1 change flipped the effective default of
:attr:`firecube.ingestor.templates.config.ZarrTemplateConfig.zarr_compression`
so that GenericZarr ingests produce compressed on-disk pipelines. These tests
verify that the flip does not break the two dominant real-world resume paths:

1. A store first written with the *new* default (compressed) can be resumed
   under the same default without raising codec drift.
2. A store first written with the *explicit* ``zarr_compression=false`` opt-out
   can be resumed under the same explicit opt-out.

Test 3 documents (deterministically) what happens when a user first writes with
``zarr_compression=false`` and then resumes without passing that option — i.e.
the resume request now sees ``zarr_compression=True`` from the flipped default.

The codec-drift guard in
:mod:`firecube.core.zarr.region_writer` only fires when the plugin's
:class:`~firecube.ingestor.api.ZarrArraySpec` declares explicit
``filters``/``serializer``/``compressors`` fields, so plugins that leave those
fields blank (the common case, including the ``cf_time_dim`` fixture used here)
should never observe drift on resume regardless of the ``zarr_compression``
value — see ``region_writer.py`` around the ``spec_compressors is not None``
gate. Test 3 pins that expectation.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from click.testing import CliRunner, Result

from firecube.cli.main import cli

pytestmark = pytest.mark.integration


_PLUGIN = "cf_time_dim"
_PRODUCT = "cf_time_dim"


def _make_dummy_input(tmp_path: Path) -> Path:
    source = tmp_path / "dummy_input"
    source.mkdir(exist_ok=True)
    (source / "dummy.nc").touch(exist_ok=True)
    return source


def _ingest_args(tmp_path: Path, target: Path, *extra: str) -> list[str]:
    """Baseline ``firecube ingest`` args for the ``cf_time_dim`` fixture.

    Matches the invocation style used by ``test_generic_zarr_codec_cli_defaults``
    so both tests exercise the same CLI-to-writer path.
    """
    source = _make_dummy_input(tmp_path)
    return [
        "ingest",
        _PLUGIN,
        "--input-data",
        str(source),
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
        "--output-format",
        "zarr",
        *extra,
    ]


def _run(args: list[str]) -> Result:
    return CliRunner().invoke(cli, args)


def test_resume_default_compressed_store_succeeds(tmp_path: Path) -> None:
    """Two back-to-back ingests with the new default (compressed) must both succeed."""
    target = tmp_path / "default_compressed.zarr"

    first = _run(_ingest_args(tmp_path, target))
    assert first.exit_code == 0, (
        f"first ingest (bare defaults) failed unexpectedly:\n{first.output}"
    )
    assert target.exists(), (
        f"target {target} was not created by first ingest; output:\n{first.output}"
    )

    second = _run(
        _ingest_args(tmp_path, target, "--option", "resume_existing=true"),
    )
    assert second.exit_code == 0, (
        "resuming a store created with the new default (compressed) failed; "
        f"the T1 flip must not break its own resume path:\n{second.output}"
    )


def test_resume_explicit_uncompressed_store_succeeds(tmp_path: Path) -> None:
    """Two back-to-back ingests with explicit ``zarr_compression=false`` must both succeed."""
    target = tmp_path / "explicit_uncompressed.zarr"

    first = _run(
        _ingest_args(tmp_path, target, "--option", "zarr_compression=false"),
    )
    assert first.exit_code == 0, f"first ingest with zarr_compression=false failed:\n{first.output}"
    assert target.exists(), (
        f"target {target} was not created by first ingest; output:\n{first.output}"
    )

    second = _run(
        _ingest_args(
            tmp_path,
            target,
            "--option",
            "zarr_compression=false",
            "--option",
            "resume_existing=true",
        ),
    )
    assert second.exit_code == 0, (
        "resuming a store created with explicit zarr_compression=false "
        "under the same explicit opt-out must succeed; the opt-out path is a "
        f"supported configuration:\n{second.output}"
    )


def test_resume_uncompressed_store_with_new_default_behavior(tmp_path: Path) -> None:
    """Document behavior: create uncompressed, resume WITHOUT the option (new default=True).

    The codec-drift check in ``region_writer.py`` only runs when the
    :class:`~firecube.ingestor.api.ZarrArraySpec` declares explicit
    ``filters``/``serializer``/``compressors``. The ``cf_time_dim`` fixture
    declares none of those, so the drift guard is a no-op on this store and the
    second run (now compressed-by-default) is expected to succeed cleanly.

    If this assertion ever flips (e.g. because a future change tightens the
    drift check to fire even on undeclared codec fields), that is a behavior
    change worth surfacing loudly — the test's job is to catch either outcome
    deterministically.
    """
    target = tmp_path / "mixed_default.zarr"

    first = _run(
        _ingest_args(tmp_path, target, "--option", "zarr_compression=false"),
    )
    assert first.exit_code == 0, f"first ingest with zarr_compression=false failed:\n{first.output}"
    assert target.exists(), (
        f"target {target} was not created by first ingest; output:\n{first.output}"
    )

    # Second run: no zarr_compression option -> ZarrTemplateConfig default (True).
    second = _run(
        _ingest_args(tmp_path, target, "--option", "resume_existing=true"),
    )
    assert second.exit_code == 0, (
        "Resuming a store originally written with zarr_compression=false while "
        "the new default (True) is in effect currently succeeds because the "
        "cf_time_dim fixture does not declare per-array codecs, so the "
        "region_writer codec-drift check is bypassed. If this assertion is "
        "failing after a legitimate change to the drift guard, update this test "
        "to assert the new expected behavior (and add a migration note in "
        f"CHANGELOG). Output:\n{second.output}"
    )
