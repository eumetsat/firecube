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

"""S3 exclusive-create contention must converge, not crash, at the claim layer.

On a fresh store, slot-range parallelism starts many ``firecube ingest`` pods at
once. Each races for the ``slot_index_model:current`` claim, written via
exclusive-create. On S3 the losing pod's conditional ``PutObject`` returns HTTP
412 ``PreconditionFailed``, which s3fs surfaces as ``OSError(EINVAL)``. Before the
fsspec writer normalized that to ``FileExistsError``, the raw ``OSError`` escaped
``ChunkManager.ensure_slot_index_model`` and killed the pod at startup instead of
flowing into the ``ClaimConflictError`` retry/convergence loop.

This test injects exactly that s3fs surface into the writer for the first claim
attempt against a local control plane, and asserts the manager retries and
converges. It is the end-to-end proof of the ``fsspec_backend`` fix: with the
translation removed, the injected ``OSError`` propagates and this test fails.
"""

from __future__ import annotations

import errno
from pathlib import Path
from typing import Any

import pytest

from firecube.core.controlplane import ChunkManager
from firecube.core.product.identity import ProductIdentity
from firecube.core.slot_index import SlotAxis, SlotIndexModel
from firecube.core.storage.binding import StorageBinding
from firecube.core.storage.driver_config import StorageDriverConfig
from firecube.core.storage.uri import StorageUri

_PRODUCT = "prod1"


class _FakeClientError(Exception):
    """Duck-typed botocore ClientError carrying an S3 412 response."""

    def __init__(self) -> None:
        super().__init__("PreconditionFailed (412)")
        self.response = {
            "Error": {"Code": "PreconditionFailed", "Message": "At least one of the "},
            "ResponseMetadata": {"HTTPStatusCode": 412},
        }


def _s3fs_precondition_oserror() -> OSError:
    exc = OSError(errno.EINVAL, "None")
    exc.__cause__ = _FakeClientError()
    return exc


def _make_manager(tmp_path: Path) -> ChunkManager:
    product_uri = StorageUri.from_local_path(tmp_path / "__firecube_controlplane__")
    binding = StorageBinding(
        identity=ProductIdentity.from_uri(product_uri, "zarr", product_name="control_product"),
        driver=StorageDriverConfig(),
    )
    return ChunkManager(binding=binding, workspace=tmp_path)


def _model() -> SlotIndexModel:
    return SlotIndexModel(
        name="opera_v1",
        epoch="2026-01-01T00:00:00Z",
        groups={"g1": SlotAxis(cadence_s=300, mode="exact")},
    )


@pytest.mark.integration
def test_ensure_slot_index_model_converges_after_s3_precondition(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    cm = _make_manager(tmp_path)
    cm.record_run_started(
        product=_PRODUCT,
        run_id="run-1",
        output_path=str(tmp_path / _PRODUCT),
        output_format="zarr",
        size=0,
        meta={"plugin": "test"},
    )

    # Bind the repo so the real fsspec writer/raw-fs exist, then wrap the raw
    # `open` to emit the s3fs 412 surface on the FIRST exclusive-create of a
    # claim file. The exception flows through the real FsspecAtomicWriter, whose
    # normalization is the code under test.
    cm.repo._ensure_bound()
    fs_facade = cm.repo._fs
    assert fs_facade is not None
    raw_fs = fs_facade._fs  # FsspecFilesystem -> raw fsspec fs (shared with writer)
    original_open = raw_fs.open
    state = {"fired": False}

    # Force the writer down its remote-object-store branch. On a local fs
    # ``write_atomic`` publishes via temp-file + ``os.link`` (content-atomic, no
    # ``open("xb")``), so the injected s3fs ``open`` surface below would never be
    # reached. Pretending the backend is remote routes the claim write through the
    # exclusive-``open("xb")`` path whose 412->FileExistsError normalization is the
    # code under test here.
    monkeypatch.setattr(fs_facade.atomic_writer, "_is_local", lambda: False)

    def flaky_open(path: str, mode: str = "rb", *args: Any, **kwargs: Any) -> Any:
        if not state["fired"] and mode == "xb" and "claims" in str(path):
            state["fired"] = True
            raise _s3fs_precondition_oserror()
        return original_open(path, mode, *args, **kwargs)

    monkeypatch.setattr(raw_fs, "open", flaky_open)
    # Keep the retry backoff from slowing the suite.
    import firecube.core.controlplane.manager as manager_module

    monkeypatch.setattr(manager_module.time, "sleep", lambda _s: None)

    record = cm.ensure_slot_index_model(product=_PRODUCT, model=_model(), run_id="run-1")

    assert state["fired"], "the injected s3fs precondition error never triggered"
    assert record.identity_hash == _model().identity_hash
    assert cm.list_claims(product=_PRODUCT) == [], "claim must be released after convergence"
