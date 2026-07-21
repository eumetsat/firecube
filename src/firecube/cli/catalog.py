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

"""CLI helpers for catalog-related operations (e.g. Intake)."""

from __future__ import annotations

import json
import logging
from pathlib import Path

import click

from firecube.cli._ctx import get_storage_config
from firecube.cli._product import resolve_product_identity
from firecube.cli._shared_options import (
    product_uri_option,
    storage_driver_option,
    storage_type_option,
)
from firecube.cli._uri_policy import apply_smart_default, parse_product_uri
from firecube.core import observability
from firecube.core.intake import (
    build_catalog_source_specs,
    build_intake_catalog,
    discover_catalog_groups,
)
from firecube.core.storage.binding import StorageBinding
from firecube.core.storage.driver_config import StorageDriverConfig
from firecube.core.storage.session import StorageSession
from firecube.ingestor.registry.loader import discover_ingestors

log = logging.getLogger("firecube.cli")


@click.group(context_settings={"help_option_names": ["-h", "--help"]})
def catalog() -> None:
    """generate Intake YAML catalogs

    Generate Intake YAML catalogs for Firecube products. Use catalog intake to
    produce a machine-readable catalog that describes a product's storage
    layout, groups, and storage options for use with the Intake data discovery
    library.
    """


@catalog.command(
    epilog="""\b
Examples:
  # generate an Intake catalog for a product
  firecube catalog intake <plugin> -p <product> -o <path>/catalog.yaml --collection-id <id>

\b
  # generate without storage_options (for local or public stores)
  firecube catalog intake <plugin> -p <product> -o <path>/catalog.yaml --collection-id <id> --no-storage-options

See also: firecube plugins list, firecube zarr validate
""",
)
@click.argument("plugin")
@product_uri_option(tier="inspect")
@click.option(
    "-o",
    "--output",
    type=str,
    required=True,
    help="Output artifact URI (file:///abs/path); s3:// accepted but runtime support coming later",
)
@click.option(
    "--collection-id",
    required=True,
    help="explicit collection id for the generated catalog",
)
@click.option(
    "--include-storage-options/--no-storage-options",
    default=True,
    show_default=True,
    help="include generic storage_options using FIRECUBE_* placeholders",
)
@storage_driver_option(required=False)
@storage_type_option(required=False)
@click.pass_context
def intake(
    ctx: click.Context,
    plugin: str,
    product: str,
    output: str,
    collection_id: str,
    include_storage_options: bool,
    storage_driver: str | None,
    storage_type: str | None,
) -> None:
    """generate an Intake product catalog

    Generates an Intake YAML catalog for a supported product store. Discovers
    dataset groups using the specified plugin and writes a catalog file with
    source specs and optional storage_options placeholders. Requires
    --product, --output, and --collection-id.
    """
    observability.init_observability("firecube-catalog-intake")

    # Ensure plugins are discovered before we try to instantiate the ingestor.
    plugins = discover_ingestors()
    if plugin not in plugins:
        raise click.ClickException(f"Unknown plugin: {plugin}")

    ingestor_cls = plugins[plugin]

    parsed_uri = parse_product_uri(product)
    storage_type = apply_smart_default(parsed_uri, storage_type)
    parsed_output = parse_product_uri(output)
    if parsed_output.scheme == "s3":
        raise click.ClickException("Remote artifact output not yet supported")
    local_output = Path(parsed_output.normalized.removeprefix("file://"))
    storage_config = get_storage_config(
        ctx,
        overrides={"storage_type": storage_type, "storage_driver": storage_driver},
        cache=False,
    )

    driver_config = StorageDriverConfig.from_storage_config(storage_config)
    identity = resolve_product_identity(
        parsed_uri.normalized,
        format="zarr",
        product_name=parsed_uri.normalized,
    )
    session = StorageSession(
        StorageBinding(
            identity=identity,
            driver=driver_config,
        )
    )

    store_uri = identity.product_uri.to_str()

    # Instantiate ingestor and let Firecube discover/annotate catalog groups.
    ingestor = ingestor_cls()
    try:
        groups = discover_catalog_groups(store_uri, storage_session=session)
    except Exception as exc:
        raise click.ClickException(str(exc)) from exc
    source_specs = build_catalog_source_specs(
        plugin_name=plugin,
        product=parsed_uri.normalized,
        store_uri=store_uri,
        groups=groups,
        group_info_resolver=getattr(ingestor, "catalog_group_info", None),
        storage_session=session,
    )
    if not source_specs:
        raise click.ClickException(
            f"No catalogable dataset groups found for plugin '{plugin}' under product '{parsed_uri.normalized}'."
        )

    catalog_name = plugin
    catalog_description = f"Firecube product '{parsed_uri.normalized}' for plugin '{plugin}'."
    catalog_dict = build_intake_catalog(
        catalog_name=catalog_name,
        catalog_description=catalog_description,
        collection_id=collection_id,
        store_uri=store_uri,
        sources=source_specs,
        include_storage_options=include_storage_options,
    )

    # Dump YAML safely; fall back to JSON if PyYAML is unavailable.
    try:
        import yaml  # type: ignore[import-not-found]

        local_output.parent.mkdir(parents=True, exist_ok=True)
        with local_output.open("w", encoding="utf-8") as fh:
            yaml.safe_dump(catalog_dict, fh, sort_keys=False)
    except Exception as exc:
        # As a fallback, write JSON so the user still gets a usable artifact.
        log.warning("Failed to write YAML catalog (%s); falling back to JSON.", exc)
        output_json = local_output.with_suffix(".json")
        output_json.parent.mkdir(parents=True, exist_ok=True)
        with output_json.open("w", encoding="utf-8") as fh:
            json.dump(catalog_dict, fh, indent=2)
        click.echo(
            f"Intake catalog written as JSON fallback to: {output_json}",
        )
        return

    click.echo(f"Intake catalog written to: {local_output}")
