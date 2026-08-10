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

"""End-to-end CLI regression guard for GenericZarr codec defaults (issue #25).

Drives the ``firecube ingest`` command through :class:`click.testing.CliRunner`
using the in-tree ``cf_time_dim`` fixture plugin (a
:class:`GenericZarrIngestor` subclass) and inspects on-disk Zarr metadata to
verify:

1. Default (no codec options) → a compressor (``BytesBytesCodec``) IS present.
   This is the PR #25 silent-regression guard: previously the default was
   uncompressed; after the T1 flip it must be compressed.
2. ``--option zarr_compression=false`` → no ``BytesBytesCodec`` in metadata.
3. ``--option zarr_codecs='[{"name":"zstd","configuration":{"level":3}}]'`` →
   the on-disk pipeline contains a Zstd codec with ``level=3``.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, cast

import pytest
import zarr
from click.testing import CliRunner
from zarr.abc.codec import BytesBytesCodec

from firecube.cli.main import cli

pytestmark = pytest.mark.integration


_PLUGIN = "cf_time_dim"
_PRODUCT = "cf_time_dim"
_ARRAY_PATH = "default/temperature"


def _make_dummy_input(tmp_path: Path) -> Path:
    source = tmp_path / "dummy_input"
    source.mkdir()
    (source / "dummy.nc").touch()
    return source


def _ingest_args(tmp_path: Path, target: Path, *extra: str) -> list[str]:
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


def _codecs_of(store_path: Path, array_path: str) -> list[Any]:
    arr = zarr.open_array(str(store_path), path=array_path, mode="r")
    return list(cast(Any, arr.metadata).codecs)


def _codec_names(codecs: list[Any]) -> list[str]:
    names: list[str] = []
    for codec in codecs:
        to_dict = getattr(codec, "to_dict", None)
        if callable(to_dict):
            dumped = cast(dict[str, Any], to_dict())
            name = dumped.get("name")
            if isinstance(name, str):
                names.append(name)
                continue
        names.append(type(codec).__name__)
    return names


def test_default_ingest_writes_compressed_array_pr25_regression_guard(
    tmp_path: Path,
) -> None:
    """Default GenericZarr ingest MUST produce a compressed on-disk pipeline.

    Before PR #25 the effective default was uncompressed; T1 restored the
    upstream zarr default (``ZstdCodec(level=0)``). If ``BytesBytesCodec`` is
    absent from on-disk metadata for a bare-defaults ingest, the PR #25 silent
    regression has returned and this assertion must fail loudly.
    """
    target = tmp_path / "default.zarr"

    result = CliRunner().invoke(cli, _ingest_args(tmp_path, target))
    assert result.exit_code == 0, result.output
    assert target.exists(), f"target {target} was not created; output:\n{result.output}"

    codecs = _codecs_of(target, _ARRAY_PATH)
    has_compressor = any(isinstance(c, BytesBytesCodec) for c in codecs)
    assert has_compressor, (
        "PR #25 regression: default GenericZarr ingest wrote an uncompressed "
        f"array. Expected a BytesBytesCodec (zstd default) in the pipeline, "
        f"got codecs={_codec_names(codecs)!r}."
    )


def test_zarr_compression_false_writes_uncompressed_array(tmp_path: Path) -> None:
    target = tmp_path / "no_compression.zarr"

    result = CliRunner().invoke(
        cli,
        _ingest_args(tmp_path, target, "--option", "zarr_compression=false"),
    )
    assert result.exit_code == 0, result.output
    assert target.exists(), f"target {target} was not created; output:\n{result.output}"

    codecs = _codecs_of(target, _ARRAY_PATH)
    has_compressor = any(isinstance(c, BytesBytesCodec) for c in codecs)
    assert not has_compressor, (
        "zarr_compression=false should produce an uncompressed array, but a "
        f"BytesBytesCodec was present: codecs={_codec_names(codecs)!r}."
    )


def test_zarr_codecs_json_reaches_on_disk_pipeline(tmp_path: Path) -> None:
    target = tmp_path / "custom_codecs.zarr"

    result = CliRunner().invoke(
        cli,
        _ingest_args(
            tmp_path,
            target,
            "--option",
            'zarr_codecs=[{"name":"zstd","configuration":{"level":3}}]',
        ),
    )
    assert result.exit_code == 0, result.output
    assert target.exists(), f"target {target} was not created; output:\n{result.output}"

    codecs = _codecs_of(target, _ARRAY_PATH)
    names = _codec_names(codecs)
    assert "zstd" in names, (
        f"expected 'zstd' in on-disk codec pipeline, got {names!r} "
        f"(the --option zarr_codecs JSON did not reach the writer)."
    )

    zstd_codecs = [
        codec
        for codec in codecs
        if isinstance(codec, BytesBytesCodec) and "zstd" in _codec_names([codec])
    ]
    assert zstd_codecs, f"expected at least one BytesBytesCodec-with-zstd, got codecs={names!r}."
    dumped = cast(Any, zstd_codecs[0]).to_dict()
    assert dumped.get("configuration", {}).get("level") == 3, (
        f"expected zstd level=3, got dumped codec={dumped!r}."
    )
