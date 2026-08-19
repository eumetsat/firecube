# Hooks & Lifecycle

This reference covers the complete `BaseIngestor` hook surface. Template
plugins inherit all of these hooks; plugins that own the whole batch pipeline
subclass `BaseIngestor` directly and implement `_process_batch`.

DirectZarr plugins add the index contract on the template page. See
[Plugin Templates](templates.md#directzarringestor) and
[Parallel Zarr Writes](parallelism.md).

Firecube does not yet expose a public storage-writer protocol for custom
output code. A new external custom pipeline cannot be implemented using only
stable, typed public storage APIs. Do not import the concrete storage session
from an internal module as a workaround.

{{ render_api_summary("firecube.ingestor.api", [
    "BaseIngestor",
    "BaseIngestor.discover_source_files",
    "BaseIngestor._process_batch",
    "BaseIngestor.resolve_output_uri",
    "BaseIngestor.filter_item",
    "BaseIngestor.item_size_bytes",
    "BaseIngestor.get_batch_groups",
    "BaseIngestor.prepare_batch_data",
    "BaseIngestor.cleanup_batch_data",
    "BaseIngestor.batch_setup",
    "BaseIngestor.batch_teardown",
    "BaseIngestor.on_pipeline_start",
    "BaseIngestor.on_batch_success",
    "BaseIngestor.on_batch_failure",
    "BaseIngestor.slice_meta_keys",
    "BaseIngestor.slice_meta",
    "BaseIngestor.validation_group",
    "BaseIngestor.catalog_group_info",
    "BaseIngestor.default_aggregate_metrics",
]) }}

## Base Class

::: firecube.ingestor.api.BaseIngestor
    options:
        members: false

### Class Declarations

- `PRODUCT_NAME: ClassVar[str]`: required, non-empty product name; checked
  when the class is defined.
- `time_dim_name: ClassVar[str]`: time dimension name, default
  `"timestamp"`; a class declaration, not a `--option` field.
- `template_config_class`: the template config dataclass whose fields become
  validated typed options; set by each template.
- `plugin_config_class`: the plugin's own
  [`PluginConfig`](config.md#pluginconfig) subclass declaring
  product-specific options.

## Core Hooks

::: firecube.ingestor.api.BaseIngestor.discover_source_files
    options:
        inherited_members: true

::: firecube.ingestor.api.BaseIngestor._process_batch
    options:
        inherited_members: true

::: firecube.ingestor.api.BaseIngestor.resolve_output_uri
    options:
        inherited_members: true

## Batch Shaping

::: firecube.ingestor.api.BaseIngestor.filter_item
    options:
        inherited_members: true

::: firecube.ingestor.api.BaseIngestor.item_size_bytes
    options:
        inherited_members: true

::: firecube.ingestor.api.BaseIngestor.get_batch_groups
    options:
        inherited_members: true

::: firecube.ingestor.api.BaseIngestor.prepare_batch_data
    options:
        inherited_members: true

::: firecube.ingestor.api.BaseIngestor.cleanup_batch_data
    options:
        inherited_members: true

## Lifecycle Hooks

Cooperative hooks: overrides of `batch_setup` and `batch_teardown` must call
`super()` so mixins in the class hierarchy run their own setup and teardown.

::: firecube.ingestor.api.BaseIngestor.batch_setup
    options:
        inherited_members: true

::: firecube.ingestor.api.BaseIngestor.batch_teardown
    options:
        inherited_members: true

::: firecube.ingestor.api.BaseIngestor.on_pipeline_start
    options:
        inherited_members: true

::: firecube.ingestor.api.BaseIngestor.on_batch_success
    options:
        inherited_members: true

::: firecube.ingestor.api.BaseIngestor.on_batch_failure
    options:
        inherited_members: true

## Validation And Metadata

::: firecube.ingestor.api.BaseIngestor.slice_meta_keys
    options:
        inherited_members: true

::: firecube.ingestor.api.BaseIngestor.slice_meta
    options:
        inherited_members: true

::: firecube.ingestor.api.BaseIngestor.validation_group
    options:
        inherited_members: true

::: firecube.ingestor.api.BaseIngestor.catalog_group_info
    options:
        inherited_members: true

::: firecube.ingestor.api.BaseIngestor.default_aggregate_metrics
    options:
        inherited_members: true

## Runtime Context

`RuntimeIngestContext` is the engine-owned context used to run the pipeline.
It is not the context type passed to plugin hooks and must not be reused
across runs.

::: firecube.ingestor.api.RuntimeIngestContext

## See Also

- [Custom pipeline plugins](../guides/plugins/base-ingestor.md)
- [Plugin extensions](../guides/plugins/extensions.md)
- [Context & Results](context.md)
- [Parallel Zarr Writes](parallelism.md)
- [Package and Register a Plugin](../guides/plugins/contract.md)
