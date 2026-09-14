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
    if ctx.storage is None or ctx.storage.output is None:
        raise ConfigurationError("Fresh-target validation requires output storage.")
    session = ctx.storage.output
    product = session.product.product_name
    with manager.acquire_claim(
        product=product,
        domain=WriteDomain(product=product, category="fresh_output", name="product"),
        owner_id=str(ctx.run_id),
    ):
        if manager.list_runs(product=product):
            raise ResumeConflictError(
                f"{format_label} ingestion requires a fresh target with no previous runs. "
                "Resume and force-reingest are not supported for this template. "
                "Write to a new target."
            )
        require_empty_data_target(session, format_label=format_label)
        yield
