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

"""A lost exclusive-create race on a claim must converge, not crash, at the claim layer.

When parallel pods race for ``slot_index_model:current``, the loser's ``create_if_absent``
raises ``FileExistsError`` (on S3 a 412, translated by the driver). The manager must retry
through the ``ClaimConflictError`` loop and converge.
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

    # Bind the repo so the real driver exists, then make the FIRST claim create lose
    # the race. The first claim create takes the s3 branch of the real `create_if_absent`,
    # and the put below it fails as s3fs fails a 412; the translation is the code under test.
    cm.repo._ensure_bound()
    fs_facade = cm.repo._fs
    assert fs_facade is not None
    original_create = fs_facade.create_if_absent
    state = {"fired": False}

    def put_412(*_args: Any, **_kwargs: Any) -> str:
        raise _s3fs_precondition_oserror()

    def flaky_create(uri: StorageUri, data: bytes) -> str:
        if state["fired"] or "claims" not in uri.to_str():
            return original_create(uri, data)
        state["fired"] = True
        with monkeypatch.context() as s3_branch:
            s3_branch.setattr(fs_facade, "_conditional_protocol", lambda _uri: "s3")
            s3_branch.setattr(fs_facade, "_conditional_put", put_412)
            return original_create(uri, data)

    monkeypatch.setattr(fs_facade, "create_if_absent", flaky_create)
    # Keep the retry backoff from slowing the suite.
    import firecube.core.controlplane.manager as manager_module

    monkeypatch.setattr(manager_module.time, "sleep", lambda _s: None)

    record = cm.ensure_slot_index_model(product=_PRODUCT, model=_model(), run_id="run-1")

    assert state["fired"], "the injected create-race error never triggered"
    assert record.identity_hash == _model().identity_hash
    assert cm.list_claims(product=_PRODUCT) == [], "claim must be released after convergence"
