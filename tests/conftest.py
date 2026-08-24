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

"""Global pytest fixtures and configuration."""

import os
import sys
import tempfile
import time
import zipfile
from pathlib import Path

import pytest

# Add src to path for imports
src_path = Path(__file__).parent.parent / "src"
sys.path.insert(0, str(src_path))


def pytest_addoption(parser):
    """Register --strict-deps CLI option for CI fail-fast on missing optional extras."""
    parser.addoption(
        "--strict-deps",
        action="store_true",
        default=False,
        help=(
            "Fail collection if any test-required optional dependency is missing. "
            "Install with: uv sync --extra test"
        ),
    )


def pytest_configure(config):
    """Fail fast if --strict-deps is set and any required extra is missing.

    This ensures CI catches missing optional dependencies at collection time
    rather than producing silent skips. Local dev without --strict-deps
    preserves existing behavior (tests will ERROR at runtime if imports fail).
    """
    if not config.getoption("--strict-deps", default=False):
        return
    required = {
        "tensogram": "tensogram",
        "obstore": "obstore",
        "moto": "moto",
        "healpix_geo": "healpix-geo",
        "griffe": "griffelib",
    }
    for module_name, package_name in required.items():
        try:
            __import__(module_name)
        except ImportError as exc:
            raise pytest.UsageError(
                f"--strict-deps requires '{package_name}'; install with: uv sync --extra test"
            ) from exc
    # moto[server] provides ThreadedMotoServer for tests that need a real
    # HTTP S3 endpoint (e.g. obstore which bypasses Python-level mocks).
    try:
        __import__("moto.server")
    except ImportError as exc:
        raise pytest.UsageError(
            "--strict-deps requires 'moto[server]'; install with: uv sync --extra test"
        ) from exc


def pytest_sessionstart(session):
    """Fail fast if any in-tree fixture plugin is not installed."""
    required_plugins = (
        ("cli_test_plugin", "cli_test_plugin"),
        ("direct_zarr_capable_test_plugin", "direct_zarr_capable_test_plugin"),
        ("direct_zarr_non_capable_test_plugin", "direct_zarr_non_capable_test_plugin"),
        ("multi_group_capable_test_plugin", "multi_group_capable_test_plugin"),
        ("cf_time_dim_test_plugin", "cf_time_dim_test_plugin"),
        ("slot_shape_test_plugin", "slot_shape_test_plugin"),
        ("index_spec_test_plugin", "index_spec_test_plugin"),
        ("index_spec_integer_test_plugin", "index_spec_integer_test_plugin"),
        ("callable_payload_test_plugin", "callable_payload_test_plugin"),
        ("irregular_axis_test_plugin", "irregular_axis_test_plugin"),
        ("indexed_write_test_plugin", "indexed_write_test_plugin"),
    )
    missing_plugins = []

    for module_name, fixture_dir in required_plugins:
        try:
            __import__(module_name)
        except ImportError:
            missing_plugins.append((module_name, fixture_dir))

    if missing_plugins:
        details = ["Missing required fixture plugins:"]
        for module_name, fixture_dir in missing_plugins:
            details.append(f"- {module_name}: uv pip install -e tests/fixtures/{fixture_dir}")
        raise pytest.UsageError("\n".join(details))


_AWS_ENV_VARS = (
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
    "AWS_DEFAULT_REGION",
    "AWS_ENDPOINT_URL",
    "AWS_S3_ADDRESSING_STYLE",
)

_FIRECUBE_ENV_VARS = (
    "FIRECUBE_STORAGE_TYPE",
    "FIRECUBE_TARGET_PATH",
    "FIRECUBE_BUCKET",
    "FIRECUBE_ENDPOINT_URL",
    "FIRECUBE_ACCESS_KEY",
    "FIRECUBE_SECRET_KEY",
    "FIRECUBE_REGION",
    "FIRECUBE_PATH_STYLE",
    "FIRECUBE_STORAGE_DRIVER",
    "FIRECUBE_TARGET_URI",
)


@pytest.fixture(autouse=True)
def reset_temp_globals():
    """Reset tempfile.tempdir, TMPDIR, and AWS/FIRECUBE env vars after each test.

    Prevents pollution from tests that modify global temp settings or that
    invoke the CLI (which exports AWS_* env vars via export_env_from_config).
    """
    original_tempdir = tempfile.tempdir
    original_env_tmpdir = os.environ.get("TMPDIR")
    saved_aws = {k: os.environ.get(k) for k in _AWS_ENV_VARS}
    saved_firecube = {k: os.environ.get(k) for k in _FIRECUBE_ENV_VARS}

    yield

    tempfile.tempdir = original_tempdir
    if original_env_tmpdir is None:
        os.environ.pop("TMPDIR", None)
    else:
        os.environ["TMPDIR"] = original_env_tmpdir

    for k, v in saved_aws.items():
        if v is None:
            os.environ.pop(k, None)
        else:
            os.environ[k] = v

    for k, v in saved_firecube.items():
        if v is None:
            os.environ.pop(k, None)
        else:
            os.environ[k] = v


from firecube.core.controlplane import ChunkManager  # noqa: E402
from firecube.core.product.identity import ProductIdentity  # noqa: E402
from firecube.core.storage.binding import StorageBinding  # noqa: E402
from firecube.core.storage.driver_config import StorageDriverConfig  # noqa: E402
from firecube.core.storage.uri import StorageUri  # noqa: E402
from firecube.ingestor.api import IngestContext  # noqa: E402
from tests.helpers.storage import (  # noqa: E402
    make_test_binding,
    make_test_context,
    make_test_session,
)


@pytest.fixture()
def temp_workspace():
    """Create a temporary workspace for tests."""
    with tempfile.TemporaryDirectory(prefix="firecube_test_") as tmp_dir:
        yield Path(tmp_dir)


@pytest.fixture
def chunk_manager(temp_workspace):
    """Create a ChunkManager instance with test workspace."""
    product_uri = StorageUri.from_local_path(temp_workspace / "__firecube_controlplane__")
    binding = StorageBinding(
        identity=ProductIdentity.from_uri(product_uri, "zarr", product_name="control_product"),
        driver=StorageDriverConfig(),
    )
    return ChunkManager(binding=binding, workspace=temp_workspace)


@pytest.fixture
def test_binding(tmp_path):
    return make_test_binding(tmp_path)


@pytest.fixture
def test_session(tmp_path):
    return make_test_session(tmp_path)


@pytest.fixture
def test_context(tmp_path):
    return make_test_context(tmp_path)


@pytest.fixture
def sample_chunk_entries():
    """Sample chunk entries for testing."""
    return [
        {"type": "chunk", "key": "F024/FWI/c/0/0/0", "size": 12345, "timestamp": time.time()},
        {"type": "chunk", "key": "F048/FWI/c/0/0/1", "size": 23456, "timestamp": time.time()},
        {"type": "meta", "key": "F024/zarr.json", "timestamp": time.time()},
        {"type": "chunk", "key": "F120/FWI/c/1/1/0", "size": 34567, "timestamp": time.time()},
    ]


@pytest.fixture
def sample_manifest_content():
    """Sample manifest file content for testing."""
    return [
        '{"type": "chunk", "key": "F024/FWI/c/0/0/0", "size": 12345, "timestamp": 1703332800.0}',
        '{"type": "chunk", "key": "F048/FWI/c/0/0/1", "size": 23456, "timestamp": 1703419200.0}',
        '{"type": "meta", "key": "F024/zarr.json", "timestamp": 1703505600.0}',
        '{"type": "chunk", "key": "F120/FWI/c/1/1/0", "size": 34567, "timestamp": 1703592000.0}',
    ]


@pytest.fixture
def mock_hdf5_data():
    """Mock HDF5 data for testing."""
    return b"Mock HDF5 content for testing" * 100  # ~2.7KB


@pytest.fixture
def sample_zip_files(temp_workspace, mock_hdf5_data):
    """Create sample ZIP files for testing."""
    zip_files = []
    for i in range(3):
        zip_path = temp_workspace / f"test_file_{i}.zip"
        hdf5_name = f"S-LSA_-HDF5_LSASAF_TEST_PRODUCT-F024_Euro_2023101412{i:02d}"

        with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
            zf.writestr(hdf5_name, mock_hdf5_data)
            zf.writestr("manifest.xml", "<manifest>test</manifest>")

        zip_files.append(zip_path)

    return zip_files


@pytest.fixture
def ingest_context(temp_workspace):
    """Create a basic IngestContext for testing."""
    return IngestContext(
        source=temp_workspace / "input",
        target=str(temp_workspace / "output"),
        in_memory=True,
        output_format="zarr",
        options={"test": True},
    )


@pytest.fixture
def mock_s3_config():
    """Mock S3 configuration for testing."""
    return {
        "endpoint_url": "https://test-s3.example.com",
        "access_key": "test_access_key",
        "secret_key": "test_secret_key",
        "region": "us-east-1",
        "bucket": "test-bucket",
        "path_style": True,
    }
