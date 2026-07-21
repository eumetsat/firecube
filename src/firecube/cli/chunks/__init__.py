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

"""Chunk management commands (firecube chunks)."""

from __future__ import annotations

from pathlib import Path

import click

from firecube.core import observability

from ._claims import claims_group
from ._delete import delete_cmd, delete_span_cmd
from ._list import list_cmd
from ._runs import runs_group
from ._snapshots import snapshots_group

CONTEXT_SETTINGS = {"help_option_names": ["-h", "--help"]}


@click.group(context_settings=CONTEXT_SETTINGS)
@click.option(
    "--workspace", type=click.Path(path_type=Path), help="local workspace directory to use"
)
@click.option(
    "--quiet",
    "quiet",
    is_flag=True,
    default=False,
    help="Suppress storage configuration banner.",
)
@click.pass_context
def chunks(
    ctx: click.Context,
    workspace: Path | None,
    quiet: bool,
) -> None:
    """inspect and manage chunk records

    Inspect and manage tracked chunk records across all products.

    \b
    firecube tracks chunk state in a product-local control plane
    (event log, snapshots, claims). use chunks list to inspect, chunks delete
    to remove storage and records, and chunks runs to review ingestion run
    history"""
    observability.init_observability("firecube-chunks")
    ctx.ensure_object(dict)
    if workspace is not None:
        ctx.obj["workspace"] = workspace
    ctx.obj["chunks_quiet"] = quiet


chunks.add_command(list_cmd)
chunks.add_command(delete_cmd)
chunks.add_command(delete_span_cmd)
chunks.add_command(claims_group)
chunks.add_command(runs_group)
chunks.add_command(snapshots_group)

__all__ = ["chunks"]
