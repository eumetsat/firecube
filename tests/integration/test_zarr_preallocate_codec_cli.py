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

"""CLI routing tests for ``firecube zarr preallocate`` codec options.

These tests exercise the ``firecube zarr preallocate`` CLI path against the
``direct_zarr_capable_test_plugin`` fixture and assert that codec-related
``--option`` values (``zarr_compression``, ``zarr_codecs``) reach the on-disk
Zarr array metadata unchanged.

They protect the codec-routing parity between ``firecube ingest`` (via
``DirectZarrIngestor.ensure_effective_zarr_stores``) and the standalone
``firecube zarr preallocate`` command. Both paths must:

1. Coerce ``--option`` pairs through ``TierConfigurator`` into the tier-typed
   ``ZarrTemplateConfig`` (``ingestor.template_config``).
2. Translate that config into per-array pipeline kwargs via
   ``derive_effective_codecs_for_spec`` before calling
   ``RegionZarrWriter.ensure_group(..., filters=, serializer=, compressors=)``.

If either half regresses, preallocated arrays silently fall back to zarr's
default codec pipeline (``ZstdCodec(level=0)``) — indistinguishable from a
successful custom-level or uncompressed request. These tests inspect
``arr.metadata.codecs`` on the on-disk store to catch that regression
class.
"""

from __future__ import annotations

import importlib
from collections.abc import Iterator
from pathlib import Path
from typing import Any, cast

import pytest
import zarr
from click.testing import CliRunner
from zarr.abc.codec import BytesBytesCodec

from firecube.cli.main import cli

pytestmark = pytest.mark.integration


@pytest.fixture(autouse=True)
def _fresh_direct_zarr_fixture_plugin() -> Iterator[None]:
    """Force a fresh registry class for this file's CLI entry-point tests.

    Other integration tests reload the fixture plugin module and restore the
    global registry afterward. Re-discovering from a clean registry here keeps
    the class object patched by each test aligned with the class consumed by
    the CLI command under order-dependent full-suite runs.
    """
    from firecube.ingestor.registry import loader as _loader

    original_loaded = _loader._LOADED
    original_registry = dict(_loader.AVAILABLE_INGESTORS)
    _loader.reset_plugin_discovery_cache()
    importlib.reload(importlib.import_module("direct_zarr_capable_test_plugin"))
    _loader.discover_ingestors()
    try:
        yield
    finally:
        _loader._LOADED = original_loaded
        _loader.AVAILABLE_INGESTORS.clear()
        _loader.AVAILABLE_INGESTORS.update(original_registry)


def _preallocate_args(tmp_path: Path, extra: list[str] | None = None) -> list[str]:
    args = [
        "zarr",
        "preallocate",
        "direct_zarr_capable_test_plugin",
        "--target",
        (tmp_path / "out.zarr").as_uri(),
        "--product-name",
        "direct_zarr_capable_test_product",
        "--storage-type",
        "local",
        "--storage-driver",
        "fsspec",
        "--write-mode",
        "staged",
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


def test_preallocate_default_codec_routes_to_zstd_level_zero(tmp_path: Path) -> None:
    """No codec options → zarr default ``ZstdCodec(level=0)`` on preallocated arrays.

    Locks the ``firecube zarr preallocate`` path to zarr-python v3's default
    codec pipeline when neither ``zarr_compression`` nor ``zarr_codecs`` is
    supplied. Regression here would indicate either the ``TierConfigurator``
    path or ``ensure_group`` codec kwargs plumbing has broken in the
    preallocate command.
    """
    result = CliRunner().invoke(cli, _preallocate_args(tmp_path))
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


def test_preallocate_zarr_compression_false_routes_to_no_compressor(tmp_path: Path) -> None:
    """``--option zarr_compression=false`` → no BytesBytesCodec on preallocated arrays.

    Confirms the boolean opt-out reaches ``derive_effective_codecs_for_spec``
    inside the preallocate command and produces the ``compressors=[]`` shape
    on disk — matching the ``firecube ingest`` DirectZarr path's behavior and
    protecting against silent regression to the implicit zstd default.
    """
    result = CliRunner().invoke(
        cli, _preallocate_args(tmp_path, ["--option", "zarr_compression=false"])
    )
    assert result.exit_code == 0, result.output

    codecs = _data_array_codecs(tmp_path)
    assert not any(isinstance(c, BytesBytesCodec) for c in codecs), (
        f"expected no BytesBytesCodec (uncompressed), got {[type(c).__name__ for c in codecs]}"
    )


def test_preallocate_custom_zarr_codecs_routes_to_zstd_level_three(tmp_path: Path) -> None:
    """Custom ``--option zarr_codecs=[...]`` overrides the zarr default level in preallocate.

    A JSON pipeline of a single zstd compressor with ``level=3`` must be
    parsed by the CLI coercion layer, resolved via the codec registry through
    ``derive_effective_codecs_for_spec``, and land on disk with the requested
    level intact. Without the preallocate wiring, the request would silently
    fall back to ``ZstdCodec(level=0)``.
    """
    result = CliRunner().invoke(
        cli,
        _preallocate_args(
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


def test_preallocate_rejects_per_array_codecs_when_compression_false(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Preallocate must call ``validate_zarr_specs_against_template`` before mutating.

    Behavioral parity with ``DirectZarrIngestor._setup_global_zarr_schema``
    and ``_process_batch``: when a plugin declares per-array codec fields on
    a ``ZarrArraySpec`` while the tier-coerced template sets
    ``zarr_compression=False``, the CLI must fail fast with a ``ValueError``
    naming the offending array. Without the preallocate-side validation,
    incompatible arrays would be partially created on disk before the
    mismatch was surfaced — a silent-partial-mutation regression class this
    test guards against.

    The fixture plugin does not declare per-array codecs by default, so we
    monkeypatch ``zarr_schema`` to return a spec with ``compressors`` set and
    invoke preallocate with ``--option zarr_compression=false`` to construct
    the incompatible combination through the real CLI entry point.

    The monkeypatch target is resolved through
    ``discover_ingestors()`` (the same registry the CLI consumes) rather
    than through a direct module import, because a co-located autouse
    fixture in ``test_cli_zarr_preallocate_typed_config.py`` reloads the
    plugin module between tests, which desynchronizes the module-level
    class object from the registry entry. Patching the registry class
    ensures the CLI and the test target the same object under full-suite
    ordering.
    """
    from firecube.ingestor.api import PluginContext, ZarrArraySpec, ZarrGroupSpec
    from firecube.ingestor.registry.loader import discover_ingestors

    plugins = discover_ingestors()
    plugin_cls = plugins["direct_zarr_capable_test_plugin"]

    def _schema_with_per_array_compressors(self: Any, ctx: PluginContext) -> list[ZarrGroupSpec]:
        data_spec = ZarrArraySpec(
            name="data",
            chunks=(100, 10),
            shape=(1000, 10),
            dtype="float32",
            dimension_names=("timestamp", "x"),
            attrs={"long_name": "test data", "units": "K"},
            compressors=({"name": "zstd", "configuration": {"level": 3}},),
        )
        lat_spec = ZarrArraySpec(
            name="lat",
            chunks=(10,),
            shape=(10,),
            dtype="float64",
            time_indexed=False,
            dimension_names=("lat",),
            attrs={"units": "degrees_north", "standard_name": "latitude"},
        )
        return [ZarrGroupSpec(group="data", arrays=[data_spec, lat_spec])]

    monkeypatch.setattr(plugin_cls, "zarr_schema", _schema_with_per_array_compressors)

    result = CliRunner().invoke(
        cli, _preallocate_args(tmp_path, ["--option", "zarr_compression=false"])
    )

    assert result.exit_code != 0, (
        "preallocate must reject per-array codecs + zarr_compression=false, "
        f"but exited 0 with output:\n{result.output}"
    )
    assert isinstance(result.exception, ValueError), (
        f"expected ValueError from validate_zarr_specs_against_template, "
        f"got {type(result.exception).__name__}: {result.exception!r}"
    )
    message = str(result.exception)
    assert "'data'" in message, f"error must name offending array 'data': {message}"
    assert "compressors" in message, (
        f"error must mention the offending codec field 'compressors': {message}"
    )
    assert "zarr_compression=False" in message, (
        f"error must mention 'zarr_compression=False': {message}"
    )

    data_array_json = tmp_path / "out.zarr" / "data" / "data" / "zarr.json"
    assert not data_array_json.exists(), (
        f"validation must run BEFORE array creation, but {data_array_json} exists"
    )
