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

import ast
import inspect
from pathlib import Path

import pytest

from firecube.core.product import ensure_product_uri
from firecube.core.storage.uri import StorageUri
from firecube.core.uris import is_remote_target, parse_uri
from tests.helpers.rows import table_row


class TestParseUriStrictness:
    """T1: parse_uri raises ValueError on malformed URIs, accepts valid ones."""

    def test_raises_on_malformed(self, subtests, tmp_path):
        for row_id, uri in [
            ("test_raises_on_malformed_single_slash", "s3:/bucket"),
            ("test_raises_on_no_protocol", "://noprotocol"),
            ("test_raises_on_double_slash_no_colon", "s3//bucket"),
        ]:
            with (
                table_row(subtests, row_id, tmp_path),
                pytest.raises(ValueError, match="Malformed URI"),
            ):
                parse_uri(uri)

    def test_accepts_valid_s3(self):
        r = parse_uri("s3://bucket/prefix")
        assert r["protocol"] == "s3"
        assert "bucket" in r["path"]

    def test_accepts(self, subtests, tmp_path):
        for row_id, uri, protocol in [
            ("test_accepts_local_path", "/tmp/store.zarr", "file"),
            ("test_accepts_relative_path", "data/output", "file"),
            ("test_accepts_empty_string", "", "file"),
            ("test_accepts_gcs", "gcs://bucket/key", "gcs"),
        ]:
            with table_row(subtests, row_id, tmp_path):
                assert parse_uri(uri)["protocol"] == protocol


class TestEnsureProductUri:
    """T2: ensure_product_uri uses parsed path segments, not substring matching."""

    def test_ensure_product_uri(self, subtests, tmp_path):
        for row_id, base, product, expected in [
            (
                "test_no_substring_match",
                "s3://bucket/TEST_PRODUCT_data",
                "FRM",
                "s3://bucket/TEST_PRODUCT_data/FRM",
            ),
            ("test_exact_match_preserved", "s3://b/FRM", "FRM", "s3://b/FRM"),
            ("test_appends_to_bucket", "s3://bucket", "product.zarr", "s3://bucket/product.zarr"),
        ]:
            with table_row(subtests, row_id, tmp_path):
                assert ensure_product_uri(base, product) == expected


class TestIsRemoteTarget:
    """T1: is_remote_target returns True for cloud protocols, False for local."""

    def test_is_remote_target(self, subtests, tmp_path):
        for row_id, uri, remote in [
            ("test_s3", "s3://bucket/data", True),
            ("test_local", "/tmp/data", False),
            ("test_file_uri", "file:///tmp/data", False),
            ("test_gcs", "gcs://bucket/data", True),
        ]:
            with table_row(subtests, row_id, tmp_path):
                assert is_remote_target(uri) is remote


class TestStorageUriAuthorityRequirement:
    """T6: StorageUri fails fast when S3 authority is missing."""

    def test_parse_requires_s3_authority(self):
        with pytest.raises(ValueError, match="authority"):
            StorageUri.parse("s3:///key")


class TestNoStorageTypeRouting:
    """T7: engine.py must not contain storage_type == 's3'/'local' routing."""

    def test_no_storage_type_routing_in_engine(self):
        engine_path = (
            Path(__file__).resolve().parents[2] / "src/firecube/ingestor/runtime/engine.py"
        )
        tree = ast.parse(engine_path.read_text(encoding="utf-8"), filename=str(engine_path))
        offenders: list[int] = []

        for node in ast.walk(tree):
            if not isinstance(node, ast.Compare):
                continue
            left_names = {name.id for name in ast.walk(node.left) if isinstance(name, ast.Name)}
            left_attrs = {
                attr.attr for attr in ast.walk(node.left) if isinstance(attr, ast.Attribute)
            }
            comparator_values = {
                comparator.value
                for comparator in node.comparators
                if isinstance(comparator, ast.Constant) and isinstance(comparator.value, str)
            }
            if ({"storage_type"} & (left_names | left_attrs)) and comparator_values & {
                "s3",
                "local",
            }:
                offenders.append(node.lineno)

        assert offenders == []

    def test_no_hardcoded_s3_protocol_checks(self):
        root = Path(__file__).resolve().parents[2]
        src_root = root / "src/firecube"
        allowed = {(src_root / "core/uris.py").resolve()}
        violations: list[str] = []

        for path in sorted(src_root.rglob("*.py")):
            parsed = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
            if path.resolve() in allowed:
                continue
            for node in ast.walk(parsed):
                if not isinstance(node, ast.Call):
                    continue
                if not (isinstance(node.func, ast.Attribute) and node.func.attr == "startswith"):
                    continue
                if any(
                    isinstance(arg, ast.Constant)
                    and isinstance(arg.value, str)
                    and arg.value == "s3://"
                    for arg in node.args
                ):
                    violations.append(f"{path.relative_to(root).as_posix()}:{node.lineno}")

        assert violations == []


class TestIngestContextSourceType:
    """T9/T10: IngestContext.source must be str, not Path."""

    def test_ingest_context_source_is_str(self):
        from firecube.ingestor.types.context import IngestContext

        hints = inspect.get_annotations(IngestContext)
        assert hints.get("source") is str or "str" in str(hints.get("source", ""))


class TestDiscoverInputFiles:
    """T11: discover_input_files accepts str and Path, returns list[str]."""

    @pytest.mark.parametrize("as_path", [False, True], ids=["str", "path"])
    def test_accepts_str_and_path_with_exact_suffix_filtering(self, tmp_path, as_path: bool):
        from firecube.core.discovery import discover_input_files

        keep = tmp_path / "file.nc"
        drop = tmp_path / "file.txt"
        keep.write_text("data")
        drop.write_text("ignore")

        source = tmp_path if as_path else str(tmp_path)
        result = discover_input_files(source)

        assert result == [str(keep)]
        assert all(isinstance(r, str) for r in result)
