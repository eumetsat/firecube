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

"""``firecube zarr``: Zarr product maintenance command group.

Commands: validate, compare, multires, slots, preallocate,
consolidate-time-coord, and the ``zarr index`` subgroup.
"""

from __future__ import annotations

import click

from firecube.cli.index import index as index_group
from firecube.cli.zarr._consolidate import consolidate_time_coord
from firecube.cli.zarr._multires import multires
from firecube.cli.zarr._preallocate import preallocate
from firecube.cli.zarr._slots import slots
from firecube.cli.zarr._validate import compare, validate
from firecube.core import observability


@click.group(context_settings={"help_option_names": ["-h", "--help"]})
@click.pass_context
def zarr(ctx: click.Context) -> None:
    """Manage Zarr products

    Validate, build multi-resolution pyramids, pre-allocate arrays for parallel
    ingestion, and plan chunk-aligned slot ranges. Use zarr validate to check
    structural consistency, zarr multires to build downsampled layers, and
    zarr slots to emit parallel ingestion plans.
    """
    observability.init_observability("firecube-zarr")
    ctx.ensure_object(dict)


zarr.add_command(slots)
zarr.add_command(validate)
zarr.add_command(compare)
zarr.add_command(multires)
zarr.add_command(preallocate)
zarr.add_command(consolidate_time_coord)
zarr.add_command(index_group, name="index")

__all__ = ["zarr"]
