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

"""RED test: proves obstore upload loads full file into memory (Path.read_bytes bug).

A 256 MiB synthetic file upload through ``ObstoreFilesystem.multipart_upload``
must satisfy four properties:

1. Peak Python allocation during the upload is < 64 MiB (bounded memory).
2. No ``io.BytesIO`` is created with > 1 MiB of initial bytes (no whole-file
   buffering wrapped in BytesIO).
3. The underlying obstore ``store.put`` receives a file-like / iterator
   (NOT a ``bytes`` blob), and is called with ``use_multipart=True`` so the
   Rust side performs a real streaming multipart upload.
4. The uploaded object can be read back from S3 and SHA-256 matches the
   source bytes (integrity preserved).

On the current buggy implementation
(``ObstoreFilesystem.put`` does ``Path(src_uri.path).read_bytes()``):

  * Assertion 1 fails — ``read_bytes()`` allocates the full 256 MiB.
  * Assertion 3 fails — ``store.put`` is called with ``bytes`` and no
    ``use_multipart`` kwarg.

After T19 (streaming fix) all four assertions pass.

The upload + tracemalloc measurement runs inside a fresh ``python -c``
subprocess so the peak-allocation baseline is not polluted by allocations
left behind by other tests sharing the same pytest worker. The parent test
starts a moto S3 server, prepares the source file, launches the subprocess
with the moto endpoint and other parameters passed via environment
variables, and finally reads a single ``PEAK_BYTES=<n>`` line from stdout
to enforce the memory budget. The BytesIO spy, call-shape spy, and S3
integrity check all execute inside the subprocess as well — they would not
survive process boundaries any other way.
"""

from __future__ import annotations

import hashlib
import os
import re
import subprocess
import sys
import textwrap
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import boto3
import pytest
from moto.server import ThreadedMotoServer

pytestmark = [pytest.mark.slow, pytest.mark.s3]

_BUCKET = "stream-bucket"
_ACCESS_KEY = "testing"
_SECRET_KEY = "testing"
_REGION = "us-east-1"
_PRODUCT_PATH = "/streaming/product.bin"
_OBJECT_KEY = "streaming/product.bin"

_MIB = 1024 * 1024
_CHUNK_SIZE_BYTES = 4 * _MIB
_NUM_CHUNKS = 64
_FILE_SIZE_BYTES = _CHUNK_SIZE_BYTES * _NUM_CHUNKS
# Calibrated 2026-05-22: subprocess-isolated peak for a 256 MiB upload is
# ~2.6 KiB across 5 deterministic runs — obstore streams the file in Rust, so
# Python tracemalloc only sees small wrapper allocations. Budget kept at 64 MiB
# (review ceiling: 96 MiB) as a generous regression guard well below the
# Path.read_bytes()-style leak it must catch. tracemalloc.reset_peak() was
# removed from production in fix(filesystem): bf03eeb; subprocess isolation
# (see ``_SUBPROCESS_SCRIPT`` below) now provides a clean per-run baseline.
_MEMORY_BUDGET_BYTES = 64 * _MIB
_LARGE_BYTESIO_THRESHOLD_BYTES = _MIB


@pytest.fixture(scope="module")
def moto_s3_endpoint() -> Iterator[str]:
    server = ThreadedMotoServer(port=0)
    server.start()
    host, port = server.get_host_and_port()
    endpoint = f"http://{host}:{port}"
    try:
        yield endpoint
    finally:
        server.stop()


@pytest.fixture
def s3_bucket(moto_s3_endpoint: str) -> Iterator[Any]:
    client = boto3.client(
        "s3",
        endpoint_url=moto_s3_endpoint,
        aws_access_key_id=_ACCESS_KEY,
        aws_secret_access_key=_SECRET_KEY,
        region_name=_REGION,
    )
    client.create_bucket(Bucket=_BUCKET)
    try:
        yield client
    finally:
        objects = client.list_objects_v2(Bucket=_BUCKET).get("Contents", [])
        if objects:
            client.delete_objects(
                Bucket=_BUCKET,
                Delete={"Objects": [{"Key": o["Key"]} for o in objects]},
            )
        client.delete_bucket(Bucket=_BUCKET)


def _write_large_file_in_chunks(path: Path, *, chunk_size: int, num_chunks: int) -> str:
    """Write a deterministic large file in fixed-size chunks; return SHA-256 hex digest.

    The chunk buffer is allocated ONCE and reused so we never hold the full
    file in memory during setup — this matches the property we're asserting
    against the production code.
    """
    hasher = hashlib.sha256()
    chunk = b"\xab" * chunk_size
    with path.open("wb") as fh:
        for _ in range(num_chunks):
            fh.write(chunk)
            hasher.update(chunk)
    return hasher.hexdigest()


_SUBPROCESS_SCRIPT = textwrap.dedent("""\
    import hashlib
    import io
    import os
    import sys
    import tracemalloc

    import boto3

    from firecube.core.config import StorageConfig
    from firecube.core.filesystem.obstore_backend import ObstoreFilesystem
    from firecube.core.product.identity import ProductIdentity
    from firecube.core.storage.binding import StorageBinding
    from firecube.core.storage.driver_config import StorageDriverConfig
    from firecube.core.storage.uri import StorageUri

    BUCKET = os.environ["FC_TEST_BUCKET"]
    ACCESS_KEY = os.environ["FC_TEST_ACCESS_KEY"]
    SECRET_KEY = os.environ["FC_TEST_SECRET_KEY"]
    REGION = os.environ["FC_TEST_REGION"]
    ENDPOINT = os.environ["FC_TEST_ENDPOINT"]
    PRODUCT_PATH = os.environ["FC_TEST_PRODUCT_PATH"]
    OBJECT_KEY = os.environ["FC_TEST_OBJECT_KEY"]
    LOCAL_FILE = os.environ["FC_TEST_LOCAL_FILE"]
    EXPECTED_DIGEST = os.environ["FC_TEST_EXPECTED_DIGEST"]
    FILE_SIZE = int(os.environ["FC_TEST_FILE_SIZE"])
    CHUNK_SIZE = int(os.environ["FC_TEST_CHUNK_SIZE"])
    LARGE_BYTESIO_THRESHOLD = int(os.environ["FC_TEST_BYTESIO_THRESHOLD"])
    MIB = 1024 * 1024

    storage_config = StorageConfig(
        storage_type='s3',
        storage_driver='obstore',
        endpoint_url=ENDPOINT,
        access_key=ACCESS_KEY,
        secret_key=SECRET_KEY,
        region=REGION,
        path_style=True,
    )
    uri = StorageUri(protocol='s3', authority=BUCKET, path=PRODUCT_PATH)
    binding = StorageBinding(
        identity=ProductIdentity.from_uri(uri, 'zarr', product_name='streaming_product'),
        driver=StorageDriverConfig.from_storage_config(storage_config),
    )
    fs = ObstoreFilesystem.from_binding(binding)

    class _StorePutSpy:
        '''Wraps an obstore store and records the shape of every ``put`` call.

        Composition (not subclassing) because obstore's S3Store is a Rust
        extension type and isn't safely subclassable from Python.
        '''

        def __init__(self, real_store):
            self._real = real_store
            self.put_calls = []

        def put(self, path, file, **kwargs):
            is_bytes_arg = isinstance(file, (bytes, bytearray, memoryview))
            size_if_bytes = len(file) if is_bytes_arg else None
            self.put_calls.append({
                'path': path,
                'is_bytes': is_bytes_arg,
                'size_if_bytes': size_if_bytes,
                'data_type': type(file).__name__,
                'use_multipart': kwargs.get('use_multipart'),
                'chunk_size': kwargs.get('chunk_size'),
            })
            return self._real.put(path, file, **kwargs)

        def __getattr__(self, name):
            return getattr(self._real, name)

    spy = _StorePutSpy(fs._store)
    fs._store = spy

    large_bytesio_sizes = []
    original_bytesio = io.BytesIO

    class _TrackingBytesIO(original_bytesio):
        def __init__(self, *args, **kwargs):
            if args and isinstance(args[0], (bytes, bytearray, memoryview)):
                size = len(args[0])
                if size > LARGE_BYTESIO_THRESHOLD:
                    large_bytesio_sizes.append(size)
            super().__init__(*args, **kwargs)

    io.BytesIO = _TrackingBytesIO

    remote_uri = f's3://{BUCKET}{PRODUCT_PATH.rstrip(chr(47))}'.rstrip('/')
    tracemalloc.start()
    try:
        fs.multipart_upload(LOCAL_FILE, remote_uri)
        _, peak_bytes = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()

    # Restore real BytesIO before the integrity download so boto3's own
    # internal BytesIO usage doesn't trip the spy and so any allocations
    # after this point are unambiguous.
    io.BytesIO = original_bytesio

    assert not large_bytesio_sizes, (
        'Implementation created io.BytesIO instance(s) with sizes '
        f'{[s / MIB for s in large_bytesio_sizes]} MiB — the upload '
        'must stream from disk, not buffer the file in a BytesIO.'
    )

    assert len(spy.put_calls) >= 1, 'Expected at least one obstore store.put() call.'
    final_call = spy.put_calls[-1]
    assert not final_call['is_bytes'], (
        f'obstore store.put() was called with a {final_call["data_type"]} bytes blob '
        f'of {final_call["size_if_bytes"]} bytes — multipart_upload must pass a '
        'file-like object (IO[bytes]) or iterator, not the full bytes payload.'
    )
    assert final_call['use_multipart'] is True, (
        f'obstore store.put() was called with use_multipart='
        f'{final_call["use_multipart"]!r}; expected True so the Rust side '
        'performs a streaming multipart upload.'
    )

    s3 = boto3.client(
        's3',
        endpoint_url=ENDPOINT,
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
        region_name=REGION,
    )
    obj = s3.get_object(Bucket=BUCKET, Key=OBJECT_KEY)
    assert obj['ContentLength'] == FILE_SIZE, (
        f'Uploaded ContentLength {obj["ContentLength"]} != expected {FILE_SIZE}'
    )
    downloaded_hasher = hashlib.sha256()
    for chunk in obj['Body'].iter_chunks(chunk_size=CHUNK_SIZE):
        downloaded_hasher.update(chunk)
    assert downloaded_hasher.hexdigest() == EXPECTED_DIGEST, (
        'Round-trip integrity check failed: uploaded bytes do not match source.'
    )

    print(f'PEAK_BYTES={peak_bytes}')
""")


@pytest.mark.integration
@pytest.mark.slow
def test_obstore_multipart_upload_streams_without_loading_full_file(
    moto_s3_endpoint: str,
    s3_bucket: Any,
    tmp_path: Path,
) -> None:
    """Streaming-upload contract: bounded memory, file-like obstore call, integrity.

    The upload itself runs in a clean subprocess to keep tracemalloc's peak
    measurement free of cross-test allocator noise; the parent here only
    prepares fixtures, spawns the subprocess, and enforces the budget.
    """
    local_file = tmp_path / "large.bin"
    expected_digest = _write_large_file_in_chunks(
        local_file, chunk_size=_CHUNK_SIZE_BYTES, num_chunks=_NUM_CHUNKS
    )
    assert local_file.stat().st_size == _FILE_SIZE_BYTES

    env = os.environ.copy()
    env.update(
        {
            "FC_TEST_BUCKET": _BUCKET,
            "FC_TEST_ACCESS_KEY": _ACCESS_KEY,
            "FC_TEST_SECRET_KEY": _SECRET_KEY,
            "FC_TEST_REGION": _REGION,
            "FC_TEST_ENDPOINT": moto_s3_endpoint,
            "FC_TEST_PRODUCT_PATH": _PRODUCT_PATH,
            "FC_TEST_OBJECT_KEY": _OBJECT_KEY,
            "FC_TEST_LOCAL_FILE": str(local_file),
            "FC_TEST_EXPECTED_DIGEST": expected_digest,
            "FC_TEST_FILE_SIZE": str(_FILE_SIZE_BYTES),
            "FC_TEST_CHUNK_SIZE": str(_CHUNK_SIZE_BYTES),
            "FC_TEST_BYTESIO_THRESHOLD": str(_LARGE_BYTESIO_THRESHOLD_BYTES),
        }
    )

    completed = subprocess.run(
        [sys.executable, "-c", _SUBPROCESS_SCRIPT],
        env=env,
        capture_output=True,
        text=True,
        timeout=300,
        check=False,
    )

    assert completed.returncode == 0, (
        f"Subprocess upload script failed (rc={completed.returncode}).\n"
        f"STDOUT:\n{completed.stdout}\n"
        f"STDERR:\n{completed.stderr}"
    )

    match = re.search(r"^PEAK_BYTES=(\d+)\s*$", completed.stdout, re.MULTILINE)
    assert match is not None, (
        f"Could not parse PEAK_BYTES=<n> from subprocess stdout:\n{completed.stdout}"
    )
    peak_bytes = int(match.group(1))

    assert peak_bytes < _MEMORY_BUDGET_BYTES, (
        f"Streaming upload exceeded memory budget: peak={peak_bytes / _MIB:.1f} MiB "
        f"> budget={_MEMORY_BUDGET_BYTES / _MIB:.0f} MiB. "
        "Implementation is loading the full file into RAM (e.g. Path.read_bytes())."
    )
