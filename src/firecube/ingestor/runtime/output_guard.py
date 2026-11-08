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

"""Runtime ownership for templates that cannot safely reuse a product."""

from collections.abc import Iterator
from contextlib import contextmanager

from firecube.core.controlplane import ChunkManager
from firecube.core.controlplane.types import WriteDomain
from firecube.core.storage.session import StorageSession
from firecube.ingestor.errors import ConfigurationError, ResumeConflictError
from firecube.ingestor.types.context import RuntimeIngestContext


def require_empty_data_target(session: StorageSession, *, format_label: str) -> None:
    """Refuse any existing payload, including output without WAL records."""
    target = session.product.product_uri
    control = session.product.control_root_uri.path.rstrip("/") + "/"
    for uri in session.find(target):
        if not uri.path.startswith(control):
            raise ResumeConflictError(
                f"{format_label} ingestion requires a fresh empty target; data already "
                f"exists at {target.to_str()}. Resume and force-reingest are not supported "
                "for this template. Write to a new target."
            )


def _output_session(ctx: RuntimeIngestContext) -> StorageSession:
    if ctx.storage is None or ctx.storage.output is None:
        raise ConfigurationError("Fresh-target validation requires output storage.")
    return ctx.storage.output


def _refuse_reused_output(
    session: StorageSession, manager: ChunkManager, *, format_label: str
) -> None:
    """Refuse a product with a recorded run or with payload outside the control plane."""
    if manager.list_runs(product=session.product.product_name):
        raise ResumeConflictError(
            f"{format_label} ingestion requires a fresh target with no previous runs. "
            "Resume and force-reingest are not supported for this template. "
            "Write to a new target."
        )
    require_empty_data_target(session, format_label=format_label)


@contextmanager
def fresh_output_run(
    ctx: RuntimeIngestContext,
    manager: ChunkManager,
    *,
    required: bool,
    format_label: str,
) -> Iterator[None]:
    """Hold product ownership through staged promotion and terminal recording."""
    if not required:
        yield
        return
    session = _output_session(ctx)
    product = session.product.product_name
    with manager.acquire_claim(
        product=product,
        domain=WriteDomain(product=product, category="fresh_output", name="product"),
        owner_id=str(ctx.run_id),
    ):
        _refuse_reused_output(session, manager, format_label=format_label)
        yield


def fresh_output_findings(
    ctx: RuntimeIngestContext,
    manager: ChunkManager,
    *,
    required: bool,
    format_label: str,
) -> dict[str, object]:
    """Report what `fresh_output_run` would refuse, reading only and taking no claim.

    Returns:
        ``status`` (``"pass"`` or ``"blocked"``), ``required`` and ``reason``, the refusal or ``None``.
    """
    if not required:
        return {"status": "pass", "required": False, "reason": None}
    try:
        _refuse_reused_output(_output_session(ctx), manager, format_label=format_label)
    except ResumeConflictError as exc:
        return {"status": "blocked", "required": True, "reason": str(exc)}
    return {"status": "pass", "required": True, "reason": None}
