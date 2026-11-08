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
pipeline_workers = 2
pipeline_batch_size = 40
resume_existing = true
```

`[storage].type` and `FIRECUBE_STORAGE_TYPE` apply only to commands that do not
take a product URI, for example `chunks --all-products`. For a command that takes a URI,
the storage type comes from the URI scheme. An explicit `--storage-type` overrides it.

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
`--input-data` when the plugin reads command-supplied input. Pass
`--product-name` only to record the label of a new store when the plugin has no
`PRODUCT_NAME`. The store records the label once in `.firecube/product.json`.

## Command-Line Conventions

These rules apply to every command. The [CLI Reference](cli.md) lists the flags of each command.

### Target And Label

Every command that takes one product URI uses `-t` or `--target`. `firecube zarr compare` is the exception. It takes two stores as positional arguments.

The store records its product label once, in `.firecube/product.json`. A command that needs the recorded label refuses a store without one and exits with code 2. The message names `firecube migrate adopt`. Pure storage readers and first writes do not refuse such a store. Examples are `zarr validate`, `parquet validate`, `catalog intake`, `advise` and `archive create`.

`--product-name` is optional on write commands. It records the label of a new store, and the store refuses a different name later. `firecube migrate adopt` and `firecube zarr index rebuild` require it. `firecube archive restore` needs it only when the archive records no label.

### Output Format

A command with an output format takes `-f` or `--format`. The values are `table`, `json` and `csv`. A command lists only the values it supports. For example, `firecube plugins list` supports all three, and `firecube zarr index show` supports `table` and `json`. The default is `table`, except for `firecube zarr slots`, which defaults to `json`.

### Dry Run And Confirmation

These commands take `--dry-run`: `ingest`, `snapshot take`, `migrate run`, `migrate adopt`, `release create`, `zarr multires`, `zarr index rebuild` and `zarr preallocate`. A dry run writes nothing. `migrate run`, `migrate adopt` and `release create` ask for confirmation on a terminal. Off a terminal, they need `--yes-i-really-mean-it` and otherwise exit with code 2. `--dry-run` skips the question.

### Root Options

| Option | Effect |
|---|---|
| `--config-file PATH` | Reads the configuration from the file at `PATH`. |
| `--debug` | Prints the traceback of an error. It sets `FIRECUBE_DEBUG=1`. |
| `--version` | Prints the version and exits. |

### Exit Codes

| Code | Meaning |
|---|---|
| 0 | The command did what it was asked. |
| 1 | A check found a problem, or the library refused the operation. |
| 2 | The invocation or the configuration is wrong. |
| 3 | A `--strict` check found warnings and no errors. |
| 4 | Storage, a path or an optional dependency failed the command. |
| 70 | An unexpected error ended the command. It is a bug. |
| 130 | The operator interrupted or declined the command. |

An unexpected error exits with code 70. The command prints one error line and one hint line. The hint names `--debug` and `FIRECUBE_DEBUG=1`. With `--debug`, the command prints the full traceback.

## Generated Schemas

### StorageConfig

`StorageConfig` is not in `firecube.sdk`. Its import path
`from firecube.core.api import StorageConfig` is deprecated until v0.2.0.

::: firecube.core.api.StorageConfig
    options:
        show_root_heading: false
        show_source: false
        docstring_section_style: table
        members: []

### EngineConfig

Public import: `from firecube.sdk import EngineConfig`

See [Discover Source Data](../guides/plugins/source-discovery.md)
for input-filter commands and
[Configuration Model](../concepts/configuration.md) for configuration precedence.

::: firecube.sdk.EngineConfig
    options:
        show_root_heading: false
        show_source: false
        docstring_section_style: table
        members: []

### TemplateConfig

Public import: `from firecube.sdk import TemplateConfig`

Base class of the template config tier. Each template declares its config
dataclass through `template_config_class`; the dataclass fields become
validated `--option key=value` settings.

::: firecube.sdk.TemplateConfig
    options:
        show_root_heading: false
        docstring_section_style: table
        members: []

### ZarrTemplateConfig

Public import: `from firecube.sdk import ZarrTemplateConfig`

::: firecube.sdk.ZarrTemplateConfig
    options:
        show_root_heading: false
        show_source: false
        docstring_section_style: table
        members: []

### ParquetTemplateConfig

Public import: `from firecube.sdk import ParquetTemplateConfig`

The current default Parquet writer does not apply `parquet_partition_by` or
`parquet_row_group_size`. Do not configure these fields until the corresponding
writer support is implemented.

::: firecube.sdk.ParquetTemplateConfig
    options:
        show_root_heading: false
        show_source: false
        docstring_section_style: table
        members: []

### TensogramTemplateConfig

Public import: `from firecube.sdk import TensogramTemplateConfig`

::: firecube.sdk.TensogramTemplateConfig
    options:
        show_root_heading: false
        show_source: false
        docstring_section_style: table
        members: []

### PluginConfig

Public import: `from firecube.sdk import PluginConfig`

Plugin authors subclass this dataclass to declare product-specific options.

::: firecube.sdk.PluginConfig
    options:
        show_root_heading: false
        show_source: false
        docstring_section_style: table
        members: []

## Next Steps

- **[CLI Reference](cli.md)** — complete command flags
- **[Configuration Model](../concepts/configuration.md)** — precedence and runtime model
- **[Observability Reference](observability.md)** — metrics and environment variables
