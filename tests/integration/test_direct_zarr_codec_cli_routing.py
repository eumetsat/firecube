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

"""CLI routing tests for DirectZarr codec options.

These tests exercise the ``firecube ingest`` CLI path against the
``direct_zarr_capable_test_plugin`` fixture and assert that the codec-related
``--option`` values reach the on-disk Zarr metadata unchanged. They protect
the ``DirectZarrIngestor.template_config_class = ZarrTemplateConfig`` wiring:
if that binding regresses, ``zarr_compression`` / ``zarr_codecs`` would be
rejected as unknown options and the CLI path would silently drop them,
leaving the store with zarr's implicit default codec pipeline.

Every test drives the full CLI (no direct ``ZarrTemplateConfig``
construction, no in-process ``run()`` calls) and inspects
``arr.metadata.codecs`` on the resulting store.
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


def _ingest_args(tmp_path: Path, extra: list[str] | None = None) -> list[str]:
    config = tmp_path / "config.toml"
    config.write_text("", encoding="utf-8")
    args = [
        "--config-file",
        str(config),
        "ingest",
        "direct_zarr_capable_test_plugin",
        "--input-data",
        str(tmp_path),
        "--target",
        (tmp_path / "out.zarr").as_uri(),
        "--product-name",
        "direct_zarr_capable_test_product",
        "--storage-type",
        "local",
        "--storage-driver",
        "fsspec",
        "--write-mode",
        "direct",
        "--output-format",
        "zarr",
    ]
    if extra:
        args.extend(extra)
    return args


def _data_array_codecs(tmp_path: Path) -> list[Any]:
    arr = zarr.open_array(str(tmp_path / "out.zarr"), path="data/data", mode="r")
    return list(cast(Any, arr.metadata).codecs)


def _zstd_compressor_entries(codecs: list[Any]) -> list[dict[str, Any]]:
    entries: list[dict[str, Any]] = []
    for codec in codecs:
        if not isinstance(codec, BytesBytesCodec):
            continue
        dumped = cast(dict[str, Any], codec.to_dict())
        if dumped.get("name") == "zstd":
            entries.append(dumped)
    return entries


def test_default_codec_routes_to_zstd_level_zero(tmp_path: Path) -> None:
    """No codec options → zarr default ``ZstdCodec(level=0)`` on the data array.

    Locks the DirectZarr CLI path to the zarr-python v3 default codec pipeline
    when ``zarr_compression`` / ``zarr_codecs`` are omitted. If the template
    config wiring regresses, either no compressor or a non-zstd default would
    appear here.
    """
    result = CliRunner().invoke(cli, _ingest_args(tmp_path))
    assert result.exit_code == 0, result.output

    codecs = _data_array_codecs(tmp_path)
    assert any(isinstance(c, BytesBytesCodec) for c in codecs), (
        f"expected at least one BytesBytesCodec (compressor), "
        f"got {[type(c).__name__ for c in codecs]}"
    )
    zstd_entries = _zstd_compressor_entries(codecs)
    assert zstd_entries, (
        f"expected a zstd BytesBytesCodec, got {[type(c).__name__ for c in codecs]}"
    )
    assert zstd_entries[0]["configuration"]["level"] == 0, zstd_entries


def test_zarr_compression_false_routes_to_no_compressor(tmp_path: Path) -> None:
    """``--option zarr_compression=false`` → no BytesBytesCodec on the data array.

    Confirms that the boolean opt-out reaches ``derive_effective_codecs_for_spec``
    and produces the ``compressors=[]`` shape (the canonical winner for the
    disabled-compression branch — see ``test_zarr_codec_api_assumptions``).
    """
    result = CliRunner().invoke(cli, _ingest_args(tmp_path, ["--option", "zarr_compression=false"]))
    assert result.exit_code == 0, result.output

    codecs = _data_array_codecs(tmp_path)
    assert not any(isinstance(c, BytesBytesCodec) for c in codecs), (
        f"expected no BytesBytesCodec (uncompressed), got {[type(c).__name__ for c in codecs]}"
    )


def test_custom_zarr_codecs_routes_to_zstd_level_three(tmp_path: Path) -> None:
    """Custom ``--option zarr_codecs=[...]`` overrides the zarr default level.

    A JSON pipeline of a single zstd compressor with ``level=3`` must be parsed
    by the CLI coercion layer, resolved via the codec registry, and land on
    disk with the requested level intact.
    """
    result = CliRunner().invoke(
        cli,
        _ingest_args(
            tmp_path,
            ["--option", 'zarr_codecs=[{"name":"zstd","configuration":{"level":3}}]'],
        ),
    )
    assert result.exit_code == 0, result.output

    codecs = _data_array_codecs(tmp_path)
    zstd_entries = _zstd_compressor_entries(codecs)
    assert zstd_entries, (
        f"expected a zstd BytesBytesCodec, got {[type(c).__name__ for c in codecs]}"
    )
    assert zstd_entries[0]["configuration"]["level"] == 3, zstd_entries


def test_malformed_zarr_codecs_fails_clean(tmp_path: Path) -> None:
    """Non-JSON ``--option zarr_codecs`` exits non-zero with a clean, named error.

    The CLI's coercion layer must reject the malformed value at the click
    parameter boundary — surfacing the option name (``zarr_codecs``) and
    without leaking a Python traceback into the user-facing output.
    """
    result = CliRunner().invoke(cli, _ingest_args(tmp_path, ["--option", "zarr_codecs=not-json"]))
    assert result.exit_code != 0, result.output
    assert "zarr_codecs" in result.output, result.output
    assert "Traceback" not in result.output, result.output
