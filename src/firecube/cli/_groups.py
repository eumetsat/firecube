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

from __future__ import annotations

import click

from firecube.cli._formatter import OPTION_GROUPS
from firecube.cli.advise import advise as advise_group
from firecube.cli.archive import archive as archive_group
from firecube.cli.catalog import catalog as catalog_group
from firecube.cli.chunks import chunks as chunks_group
from firecube.cli.completion import completion_cmd
from firecube.cli.migrate import migrate as migrate_group
from firecube.cli.parquet import parquet as parquet_group
from firecube.cli.plugins import plugins as plugins_group
from firecube.cli.release import release as release_group
from firecube.cli.snapshot import snapshot as snapshot_group
from firecube.cli.zarr import zarr as zarr_group

_REQUIRED_OPTIONS: dict[str, list[str]] = {
    "snapshot take": ["--target"],
    "snapshot list": ["--target"],
    "snapshot show": ["--target"],
    "snapshot diff": ["--target"],
    "migrate run": ["--target"],
    "migrate adopt": ["--target", "--product-name"],
    "migrate status": ["--target"],
    "release create": ["--target"],
    "release list": ["--target"],
    "release show": ["--target"],
    "release verify": ["--target"],
    "zarr index show": ["--target"],
    "zarr index verify": ["--target"],
    "zarr index rebuild": ["--target", "--plugin", "--product-name"],
    "zarr consolidate-time-coord": ["--target"],
}
OPTION_GROUPS.update(
    {
        f"firecube {path}": [{"name": "Required", "options": names}]
        for path, names in _REQUIRED_OPTIONS.items()
    }
)


def register_groups(cli: click.Group) -> None:
    """Attach every command group to the root command, in help order."""
    cli.add_command(archive_group, name="archive")
    cli.add_command(advise_group, name="advise")
    cli.add_command(chunks_group, name="chunks")
    cli.add_command(snapshot_group, name="snapshot")
    cli.add_command(migrate_group, name="migrate")
    cli.add_command(release_group, name="release")
    cli.add_command(plugins_group, name="plugins")
    cli.add_command(zarr_group, name="zarr")
    cli.add_command(catalog_group, name="catalog")
    cli.add_command(parquet_group, name="parquet")
    cli.add_command(completion_cmd, name="completion")
