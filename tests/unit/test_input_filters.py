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

from pathlib import Path

import h5py
import pytest

from firecube.core.api import discover_input_files
from firecube.core.storage.uri import StorageUri
from firecube.ingestor.api import EngineConfig, IngestContext
from firecube.ingestor.runtime.configure import TierConfigurator


@pytest.fixture
def source(tmp_path):
    for name in [
        "mynetcdf.nc",
        "myothernetcdf.nc",
        "another.nc",
        "sample.hdf",
        "sample.h5",
        "measurement.csv",
        "draft_01.csv",
        "uppercase.NC",
        ".hidden.csv",
        "!measurement.nc",
        "incoming/deeper/record.nc",
    ]:
        path = tmp_path / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.touch()
    return tmp_path


def selected(source, filters, **kwargs):
    return {
        str(path).removeprefix(str(source) + "/")
        for path in discover_input_files(
            source, preferred_globs=filters, sniff_hdf5=False, **kwargs
        )
    }


@pytest.mark.parametrize(
    "filters,expected",
    [
        (
            ["*.csv"],
            {
                "mynetcdf.nc",
                "myothernetcdf.nc",
                "another.nc",
                "sample.hdf",
                "sample.h5",
                "measurement.csv",
                "draft_01.csv",
                "uppercase.NC",
                ".hidden.csv",
                "!measurement.nc",
                "incoming/deeper/record.nc",
            },
        ),
        (["!*", "*.csv"], set()),
        (["*.csv", "!*"], set()),
    ],
)
def test_additive_selection_and_exclusions(source, filters, expected):
    assert selected(source, filters) == expected


def test_negative_filters_remove_defaults_and_nested_files(source):
    actual = selected(source, ["mynetcdf.nc", "!myothernetcdf.nc", "!*.hdf", "!incoming/*"])
    assert actual == {"mynetcdf.nc", "another.nc", "sample.h5", "uppercase.NC", "!measurement.nc"}


@pytest.mark.parametrize("filters", [["*.csv", "!draft_*.csv"], ["!draft_*.csv", "*.csv"]])
def test_exclusions_win_regardless_of_order(source, filters):
    assert selected(source, filters, include_suffixes=()) == {"measurement.csv", ".hidden.csv"}


def test_filters_case_sensitive_but_defaults_ignore_case(source):
    assert selected(source, ["!*.nc", "!*.hdf", "!*.h5"]) == {"uppercase.NC"}
    assert selected(source, ["*.nc"], include_suffixes=()) == {
        "mynetcdf.nc",
        "myothernetcdf.nc",
        "another.nc",
        "!measurement.nc",
        "incoming/deeper/record.nc",
    }


def test_literal_bang_and_character_classes(source):
    assert selected(source, [r"\!measurement.nc"], include_suffixes=()) == {"!measurement.nc"}
    assert "!measurement.nc" not in selected(source, ["!!measurement.nc"])
    assert selected(source, ["[ma]*.nc"], include_suffixes=()) == {
        "mynetcdf.nc",
        "myothernetcdf.nc",
        "another.nc",
    }


def test_empty_filters_and_duplicates_keep_deterministic_results(source):
    assert selected(source, []) == selected(source, None)
    found = discover_input_files(source, preferred_globs=["*.csv", "*.csv"], sniff_hdf5=False)
    assert len(found) == len(set(found))
    assert found == sorted(found, key=lambda name: Path(name).name)


def test_full_path_exclusion_and_explicit_helper_exclusions(source):
    assert "another.nc" not in selected(source, [f"!{source / 'another.nc'}"])
    assert "another.nc" not in selected(source, ["another.nc"], exclude=["another.nc"])


def test_filters_apply_to_explicit_file(source):
    path = source / "measurement.csv"
    assert discover_input_files(path, preferred_globs=["*.csv"], sniff_hdf5=False) == [str(path)]
    assert (
        discover_input_files(path, preferred_globs=["*.csv", "!measurement.csv"], sniff_hdf5=False)
        == []
    )


def test_exclusions_override_content_detection_without_sniffing(tmp_path, monkeypatch):
    path = tmp_path / "measurement"
    with h5py.File(path, "w"):
        pass
    assert discover_input_files(tmp_path) == [str(path)]

    def must_not_sniff(_):
        pytest.fail("excluded file was inspected")

    monkeypatch.setattr("firecube.core.formats.discovery.looks_like_hdf5", must_not_sniff)
    assert discover_input_files(tmp_path, preferred_globs=["!measurement"]) == []


def test_remote_paths_use_same_matching_without_sniffing(monkeypatch):
    paths = [
        "s3://bucket/root/a.csv",
        "s3://bucket/root/draft_b.csv",
        "s3://bucket/root/incoming/deeper/a.nc",
        "s3://bucket/root/file.hdf",
        "s3://bucket/root/file.NC",
    ]

    class RemoteListing:
        def find(self, root: StorageUri) -> list[StorageUri]:
            _ = root
            return [StorageUri.parse(path) for path in paths]

    def open_source_filesystem(
        source_uri: str, storage_config: object | None
    ) -> tuple[RemoteListing, StorageUri]:
        _ = storage_config
        return RemoteListing(), StorageUri.parse(source_uri)

    monkeypatch.setattr(
        "firecube.core.formats.discovery.open_source_filesystem",
        open_source_filesystem,
    )

    def must_not_sniff(_):
        pytest.fail("remote source was inspected as a local file")

    monkeypatch.setattr("firecube.core.formats.discovery.looks_like_hdf5", must_not_sniff)
    assert discover_input_files(
        "s3://bucket/root",
        preferred_globs=["*.csv", "!draft_*.csv", "!*/incoming/*", "!s3://bucket/root/file.hdf"],
    ) == ["s3://bucket/root/a.csv", "s3://bucket/root/file.NC"]


@pytest.mark.parametrize("value", ["*.csv", {}, [1], [None], [False], [""], ["!"]])
def test_invalid_sdk_filters_fail(value):
    with pytest.raises(ValueError, match="input_filters"):
        EngineConfig.from_options({"input_filters": value})
    with pytest.raises(ValueError, match="input_filters"):
        EngineConfig(input_filters=value)


@pytest.mark.parametrize(
    "options", [{"include_patterns": []}, {"include_patterns": [], "input_filters": []}]
)
def test_legacy_sdk_options_fail_with_migration(options):
    with pytest.raises(ValueError, match="include_patterns has been removed; use --input-filters"):
        EngineConfig.from_options(options)
    with pytest.raises(ValueError, match="include_patterns has been removed; use --input-filters"):
        TierConfigurator(None, None, plugin_name="test").configure(
            IngestContext(source=".", options=options)
        )


def test_filters_preserve_spaces(tmp_path):
    (tmp_path / "my measurement.csv").touch()
    assert selected(tmp_path, ["my measurement.csv"], include_suffixes=()) == {"my measurement.csv"}
