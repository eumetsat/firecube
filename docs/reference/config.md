# Configuration Reference

Firecube has two configuration surfaces:

- **Command flags** such as `--target`, `--storage-type`, `--storage-driver`,
  `--product-name`, `--write-mode`, and `--input-data`. These are documented
  from the live Click command tree in [CLI Reference](cli.md).
- **Config dataclasses** used for storage settings and `--option key=value`
  ingestion settings. The schemas below are generated from the actual Python
  dataclasses.

Pass the required ingest command flags explicitly for each run. For ingest,
`--storage-type` and `--storage-driver` are inferred from the URI scheme by
default unless overridden.

## Configuration File

By default, Firecube looks for `~/.config/firecube/config.toml`. Use the root
`--config-file` option to select another file:

```bash
firecube --config-file /path/to/config.toml ingest <plugin> --show-options
```

Example:

```toml
[storage]
endpoint_url = "https://your-s3-endpoint"
path_style = true
# driver = "fsspec"  # or "obstore" (requires firecube[obstore])

[metrics]
pushgateway_url = "http://localhost:9091"
label_allowlist = ["frp_variant"]

[plugins.my_plugin]
zarr_chunk_shape = '{"timestamp":1,"ny":550,"nx":475}'
zarr_compression = false
zarr_consolidate = false
pipeline_parallel = true
pipeline_workers = 2
pipeline_batch_size = 40
resume_existing = true
```

`[storage].type` and `FIRECUBE_STORAGE_TYPE` apply only to commands that do not
take a product URI (e.g. `chunks/*`); for URI-bearing commands the storage type
comes from the URI scheme or an explicit `--storage-type`.

`[plugins.<name>]` only applies when that plugin is installed in the current
environment. Use these commands to inspect the effective keys for a plugin:

```bash
firecube ingest <plugin_name> --show-options
firecube plugins describe <plugin_name>
```

Observability environment variables and `[metrics]` keys are listed in
[Observability Reference](observability.md).

## Precedence

For storage settings loaded through `StorageConfig`, precedence is:

1. CLI overrides
2. Environment variables
3. `config.toml`
4. Built-in defaults

For `firecube ingest`, `--target` and `--write-mode` remain explicit. Pass
`--input-data` when the plugin reads command-supplied input, and pass
`--product-name` when the plugin has no `PRODUCT_NAME` or the run needs an
override.

## Generated Schemas

### StorageConfig

Public import: `from firecube.core.api import StorageConfig`

::: firecube.core.api.StorageConfig
    options:
        show_root_heading: false
        show_source: false
        docstring_section_style: table
        members: []

### EngineConfig

Public import: `from firecube.ingestor.api import EngineConfig`

These fields are accepted as common `--option key=value` settings and plugin
defaults under `[plugins.<name>]`.

::: firecube.ingestor.api.EngineConfig
    options:
        show_root_heading: false
        show_source: false
        docstring_section_style: table
        members: []

### TemplateConfig

Public import: `from firecube.ingestor.api import TemplateConfig`

Base class of the template config tier. Each template declares its config
dataclass through `template_config_class`; the dataclass fields become
validated `--option key=value` settings.

::: firecube.ingestor.api.TemplateConfig
    options:
        show_root_heading: false
        docstring_section_style: table
        members: []

### ZarrTemplateConfig

Public import: `from firecube.ingestor.api import ZarrTemplateConfig`

::: firecube.ingestor.api.ZarrTemplateConfig
    options:
        show_root_heading: false
        show_source: false
        docstring_section_style: table
        members: []

### ParquetTemplateConfig

Public import: `from firecube.ingestor.api import ParquetTemplateConfig`

The current default Parquet writer does not apply `parquet_partition_by` or
`parquet_row_group_size`. Do not configure these fields until the corresponding
writer support is implemented.

::: firecube.ingestor.api.ParquetTemplateConfig
    options:
        show_root_heading: false
        show_source: false
        docstring_section_style: table
        members: []

### TensogramTemplateConfig

Public import: `from firecube.ingestor.api import TensogramTemplateConfig`

::: firecube.ingestor.api.TensogramTemplateConfig
    options:
        show_root_heading: false
        show_source: false
        docstring_section_style: table
        members: []

### PluginConfig

Public import: `from firecube.ingestor.api import PluginConfig`

Plugin authors subclass this dataclass to declare product-specific options.

::: firecube.ingestor.api.PluginConfig
    options:
        show_root_heading: false
        show_source: false
        docstring_section_style: table
        members: []

## Next Steps

- **[CLI Reference](cli.md)** — complete command flags
- **[Configuration Model](../concepts/configuration.md)** — precedence and runtime model
- **[Observability Reference](observability.md)** — metrics and environment variables
