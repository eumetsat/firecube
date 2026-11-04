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

"""Migrate CLI commands for metadata-only changes to a product.

The commands register with ``@click.command`` and the group imports them; the
other order is circular.
"""

from __future__ import annotations

import click

from firecube.cli.migrate._inspect import list_cmd, status
from firecube.cli.migrate._run import adopt, run
from firecube.core import observability


@click.group(context_settings={"help_option_names": ["-h", "--help"]})
@click.pass_context
def migrate(ctx: click.Context) -> None:
    """run metadata-only migration steps on a product

    A migration step changes Zarr metadata documents and never a chunk. Use
    migrate list to see the installed steps, migrate run to apply one,
    migrate status to read the recorded runs, and migrate adopt to give a
    store that Firecube did not write a control plane.
    """
    observability.init_observability("firecube-migrate")
    ctx.ensure_object(dict)


migrate.add_command(run)
migrate.add_command(list_cmd)
migrate.add_command(status)
migrate.add_command(adopt)

__all__ = ["migrate"]
