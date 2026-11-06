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

"""RED test: ``FilesystemClaimService.acquire()`` against obstore S3 has a TOCTOU race.

The buggy path (current code, ``src/firecube/core/filesystem/obstore_backend.py:105-111``)
implements ``open(uri, "x")`` as:

    if "x" in mode and self.exists(uri):
        raise FileExistsError(...)
    # otherwise return a deferred write buffer; the actual PUT happens at close().

Between the ``exists()`` head request and the deferred ``put()``, **any other writer
can win the race** — multiple concurrent ``acquire()`` calls each see "claim absent"
and each writes its payload. The last writer wins on disk, but every caller returns
a ClaimHandle as if it owns the claim.

After T18 (``ObstoreAtomicWriter`` using ``PutMode.Create``) + T24 (wire it into
``FilesystemClaimService.acquire()``), exactly one writer succeeds and the others
raise ``ClaimConflictError``.

This integration test uses a real ``moto[server]`` HTTP endpoint because ``obstore``
is a Rust extension and bypasses ``moto.mock_aws()`` (which only patches boto3-level
calls). The HTTP S3 surface is the only reliable way to exercise the actual obstore
write path.
"""
# pyright: reportMissingImports=false, reportGeneralTypeIssues=false

from __future__ import annotations

import concurrent.futures
import json
import threading
import time
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import boto3
import pytest
from moto.server import ThreadedMotoServer

from firecube.core.controlplane import ChunkManager
from firecube.core.controlplane.claims import ClaimHandle
from firecube.core.controlplane.types import CONTROL_DIRNAME, WriteDomain
from firecube.core.credentials import Credentials
from firecube.core.errors import ClaimConflictError
from firecube.core.filesystem.protocol import PreconditionFailed
from firecube.core.product.identity import ProductIdentity
from firecube.core.storage.binding import StorageBinding
from firecube.core.storage.driver_config import StorageDriverConfig
from firecube.core.storage.uri import StorageUri
from tests.integration.test_claim_transitions import (
    ClaimEnv,
    owner_of,
    release_all,
    scenario_heartbeat_after_removal,
    scenario_release_with_inflight_heartbeat,
    scenario_stale_handle_release,
    scenario_successor_between_read_and_delete,
)

from tests.unit.test_filesystem_capabilities import (  # noqa: F401  isort: skip
    test_compare_delete_is_reported_on_local_bindings_only,
    test_create_if_absent_has_one_winner_among_threads,
    test_read_snapshot_never_pairs_body_with_another_generation,
    test_compare_replace_never_creates_a_missing_object,
    test_stale_validator_is_refused_by_replace_and_delete,
)

pytestmark = [pytest.mark.s3, pytest.mark.concurrency]

_BUCKET = "test-bucket"
_ACCESS_KEY = "testing"
_SECRET_KEY = "testing"
_REGION = "us-east-1"
_PRODUCT = "product.zarr"
_THREADS = 10
_IF_MATCH: list[str | None] = []


@pytest.fixture(scope="module")
def moto_s3_endpoint() -> Iterator[str]:
    server = ThreadedMotoServer(port=0)
    server.start()
    try:
        wsgi = server._server.app  # type: ignore[union-attr]

        def record(env: Any, start: Any) -> Any:
            _IF_MATCH.append(env.get("HTTP_IF_MATCH"))
            return wsgi(env, start)

        server._server.app = record  # type: ignore[union-attr]
        host, port = server.get_host_and_port()
        endpoint = f"http://{host}:{port}"
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


def _binding_for_s3(endpoint: str, driver: Any = "obstore") -> StorageBinding:
    uri = StorageUri(protocol="s3", authority=_BUCKET, path=f"/{_PRODUCT}")
    return StorageBinding(
        identity=ProductIdentity.from_uri(uri, "zarr", product_name=_PRODUCT),
        driver=StorageDriverConfig(
            driver=driver,
            endpoint_url=endpoint,
            credentials=Credentials(access_key=_ACCESS_KEY, secret_key=_SECRET_KEY),
            region=_REGION,
            path_style=True,
        ),
    )


@pytest.mark.integration
def test_concurrent_claim_acquisition_against_obstore_s3(
    moto_s3_endpoint: str,
    s3_bucket: Any,
    tmp_path: Path,
) -> None:
    """Exactly one of N concurrent acquire() calls must win; the rest must raise ClaimConflictError."""
    binding = _binding_for_s3(moto_s3_endpoint)
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    manager = ChunkManager(binding=binding, workspace=workspace)
    domain = WriteDomain(product=_PRODUCT, category="zarr_append", name="F024")

    # Barrier sync: without it the threads serialize at ThreadPoolExecutor
    # launch overhead and the TOCTOU window is never exercised. Removing
    # this would make the test pass against buggy code.
    barrier = threading.Barrier(_THREADS)
    successes: list[ClaimHandle] = []
    failures: list[BaseException] = []
    other_errors: list[BaseException] = []
    lock = threading.Lock()

    def _try_acquire(idx: int) -> None:
        owner_id = f"thread-{idx}"
        barrier.wait()
        try:
            handle = manager.acquire_claim(
                product=_PRODUCT,
                domain=domain,
                owner_id=owner_id,
            )
        except ClaimConflictError as exc:
            with lock:
                failures.append(exc)
            return
        except Exception as exc:
            with lock:
                other_errors.append(exc)
            return
        with lock:
            successes.append(handle)

    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=_THREADS) as pool:
            futures = [pool.submit(_try_acquire, i) for i in range(_THREADS)]
            for fut in concurrent.futures.as_completed(futures):
                fut.result()

        assert not other_errors, f"Unexpected exception type(s) during acquire(): {other_errors!r}"

        assert len(successes) == 1, (
            f"TOCTOU race detected: expected exactly 1 winning acquire(), "
            f"got {len(successes)} winners and {len(failures)} ClaimConflictErrors. "
            "Current obstore_backend.open('x') uses exists()+deferred put() "
            "instead of a backend-native atomic create (PutMode.Create). "
            "Fix: T18 (ObstoreAtomicWriter) + T24 (wire into "
            "FilesystemClaimService.acquire())."
        )
        assert len(failures) == _THREADS - 1, (
            f"Expected exactly {_THREADS - 1} ClaimConflictError, got {len(failures)}"
        )
        for exc in failures:
            assert isinstance(exc, ClaimConflictError), (
                f"Expected ClaimConflictError, got {type(exc).__name__}: {exc!r}"
            )

        winner = successes[0]
        listing = s3_bucket.list_objects_v2(Bucket=_BUCKET).get("Contents", [])
        claim_keys = sorted(
            obj["Key"]
            for obj in listing
            if "/.firecube/claims/" in obj["Key"] and obj["Key"].endswith(".json")
        )
        assert len(claim_keys) == 1, (
            f"Expected exactly 1 claim file on S3, got {len(claim_keys)}: {claim_keys}"
        )
        body = s3_bucket.get_object(Bucket=_BUCKET, Key=claim_keys[0])["Body"].read()
        payload = json.loads(body)
        assert payload["owner_id"] == winner.info.owner_id, (
            f"On-disk owner_id {payload['owner_id']!r} does not match the "
            f"sole winner {winner.info.owner_id!r}. Indicates a non-winning "
            "thread's PUT clobbered the winner's claim payload."
        )
    finally:
        for handle in successes:
            handle.release()
        manager.close()


@pytest.mark.integration
def test_abandon_releases_claim_missing_from_a_cached_listing(
    moto_s3_endpoint: str, s3_bucket: Any, tmp_path: Path
) -> None:
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    manager = ChunkManager(binding=_binding_for_s3(moto_s3_endpoint, "fsspec"), workspace=workspace)

    def put_claim(name: str, owner: str) -> str:
        domain = WriteDomain(product=_PRODUCT, category="zarr_region", name=name)
        key = f"{_PRODUCT}/.firecube/claims/{domain.claim_name}"
        now = time.time()
        payload = {"product": _PRODUCT, "domain": domain.identifier, "owner_id": owner}
        payload |= {"claim_path": f"s3://{_BUCKET}/{key}", "acquired_at": now}
        payload |= {"last_heartbeat_at": now, "heartbeat_interval_s": 30, "stale_threshold_s": 120}
        s3_bucket.put_object(Bucket=_BUCKET, Key=key, Body=json.dumps(payload).encode())
        return key

    try:
        put_claim("slot=0", "other:grp")
        assert len(manager.list_claims(product=_PRODUCT)) == 1
        manager.record_run_started(
            product=_PRODUCT, run_id="run-r", output_path="", output_format="zarr", size=0, meta={}
        )
        claim_key = put_claim("slot=1", "run-r:grp")
        manager.abandon_run(product=_PRODUCT, run_id="run-r", reason="killed writer")
        record = json.loads(s3_bucket.get_object(Bucket=_BUCKET, Key=claim_key)["Body"].read())
        assert record["state"] == "released"
    finally:
        manager.close()


@pytest.fixture(params=["fsspec", "obstore"])
def s3_env(request: pytest.FixtureRequest, moto_s3_endpoint: str, s3_bucket: Any) -> ClaimEnv:
    control = lambda p: StorageUri(protocol="s3", authority=_BUCKET, path=f"/{p}/{CONTROL_DIRNAME}")  # noqa: E731
    return ClaimEnv(_binding_for_s3(moto_s3_endpoint, request.param), control)


@pytest.mark.integration
def test_s3_heartbeat_in_flight_at_release_does_not_recreate_claim(s3_env: ClaimEnv) -> None:
    scenario_release_with_inflight_heartbeat(s3_env)


@pytest.mark.integration
@pytest.mark.parametrize(
    "successor",
    [
        pytest.param(False, id="cleared_claim_is_not_recreated"),
        pytest.param(True, id="late_heartbeat_keeps_successor_claim"),
    ],
)
def test_s3_late_heartbeat_after_force_clear_leaves_claim_state(
    s3_env: ClaimEnv, successor: bool
) -> None:
    scenario_heartbeat_after_removal(s3_env, successor=successor)


@pytest.mark.integration
def test_s3_stale_handle_release_keeps_successor_claim(s3_env: ClaimEnv) -> None:
    scenario_stale_handle_release(s3_env)


@pytest.mark.integration
@pytest.mark.parametrize("operation", ["clear_claim", "release_claim_if_unchanged"])
def test_s3_sweep_does_not_delete_successor_acquired_after_its_read(
    s3_env: ClaimEnv, operation: str
) -> None:
    scenario_successor_between_read_and_delete(s3_env, operation)


@pytest.mark.integration
@pytest.mark.parametrize("s3_env", ["fsspec"], indirect=True)
def test_fsspec_s3_claim_read_survives_object_replaced_after_its_first_request(
    s3_env: ClaimEnv, s3_bucket: Any
) -> None:
    """s3fs pins the ETag it saw on HEAD; a heartbeat between HEAD and GET raised FileExpired."""
    service = s3_env.service(hb=3600, stale=120)
    domain = s3_env.domain("replaced-during-read")
    handle = service.acquire(product=_PRODUCT, domain=domain, owner_id="A")
    key = f"{_PRODUCT}/.firecube/claims/{domain.claim_name}"

    def heartbeat_lands() -> None:
        record = json.loads(s3_bucket.get_object(Bucket=_BUCKET, Key=key)["Body"].read())
        s3_bucket.put_object(Bucket=_BUCKET, Key=key, Body=json.dumps(record).encode() + b" ")

    try:
        fired = s3_env.after_next_read(heartbeat_lands)
        owner = owner_of(service, domain)
    finally:
        release_all([handle])
    assert fired, "no claim read happened for the hook to follow"
    assert owner == "A"


@pytest.fixture
def cond(s3_env: ClaimEnv) -> tuple[Any, StorageUri]:
    return s3_env.fs, StorageUri(
        protocol="s3", authority=_BUCKET, path=f"/{_PRODUCT}/{CONTROL_DIRNAME}/claims"
    )


@pytest.mark.integration
def test_s3_if_match_is_sent_unquoted_while_validators_stay_quoted(cond: Any) -> None:
    fs, base = cond
    uri = base.join("wire.json")
    first = fs.create_if_absent(uri, b"a")
    _IF_MATCH.clear()
    second = fs.compare_replace(uri, first, b"b")
    with pytest.raises(PreconditionFailed):
        fs.compare_replace(uri, first, b"c")
    assert [h for h in _IF_MATCH if h] == [first.strip('"')] * 2
    assert all(v[0] == v[-1] == '"' for v in (first, second))
