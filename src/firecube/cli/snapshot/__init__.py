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

"""Snapshot CLI commands for the metadata history of a product.

Every command takes a product URI as ``--target``. The commands register with
``@click.command`` and the group imports them; the other order is circular.
"""

from __future__ import annotations

import click

from firecube.cli.snapshot._inspect import diff, list_cmd, show
from firecube.cli.snapshot._take import take
from firecube.core import observability


@click.group(context_settings={"help_option_names": ["-h", "--help"]})
@click.pass_context
def snapshot(ctx: click.Context) -> None:
    """capture and compare the metadata snapshots of a product

    A snapshot stores the exact bytes of every Zarr metadata document of a
    product, never a chunk. Use snapshot take to capture one, snapshot list
    and snapshot show to read them, and snapshot diff to compare two.
    """
    observability.init_observability("firecube-snapshot")
    ctx.ensure_object(dict)


snapshot.add_command(take)
snapshot.add_command(list_cmd)
snapshot.add_command(show)
snapshot.add_command(diff)

__all__ = ["snapshot"]
