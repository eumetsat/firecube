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

"""Release CLI commands for the named states of a product.

The commands register with ``@click.command`` and the group imports them; the
other order is circular.
"""

from __future__ import annotations

import click

from firecube.cli.release._create import create
from firecube.cli.release._inspect import list_cmd, show, verify
from firecube.core import observability


@click.group(context_settings={"help_option_names": ["-h", "--help"]})
@click.pass_context
def release(ctx: click.Context) -> None:
    """name, list and verify the releases of a product

    A release names one coherent state of the metadata of a product and binds
    it to a snapshot. Use release create to name the current state, release
    list and release show to read them, and release verify to check that the
    metadata still equals a release.
    """
    observability.init_observability("firecube-release")
    ctx.ensure_object(dict)


release.add_command(create)
release.add_command(list_cmd)
release.add_command(show)
release.add_command(verify)

__all__ = ["release"]
