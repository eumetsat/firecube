# Package and Register a Plugin

## Goal

Prepare an external plugin package so Firecube can discover its registered
class through package metadata.

The generated ingestor already contains the decorator and product name. The
[`register_ingestor`](../../reference/templates.md#registration) decorator
sets `cls.name`; do not repeat the same value in a `name` class attribute.
Import SDK types from `firecube.ingestor.api` and helpers from
`firecube.core.api`.

Every concrete plugin class must declare a non-empty
`PRODUCT_NAME: ClassVar[str]`. Use `PluginConfig` only when the product needs
typed options. See the [Configuration Reference](../../reference/config.md) for these
public types.

## Register The Package Entry Point

Declare the package entry point in `pyproject.toml`:

```toml
[project.entry-points."firecube.plugins"]
my_plugin = "firecube_my_plugin"
```

Use the same entry-point and decorator name. A mismatch can still import the
class, but distribution and compatibility metadata will not be associated with
the registered plugin correctly. The target module must import the registered
class so the decorator runs.

## Verify Registration

```bash
uv run firecube plugins list
uv run firecube plugins describe my_plugin
uv run firecube ingest my_plugin --show-options
```

Before publishing, run the plugin's tests, lint and format checks, and one local
ingestion using representative input data for the selected plugin path.

## Common Mistakes

| Mistake | Fix |
|---|---|
| Adding `name = "my_plugin"` beside the decorator | Keep only `@register_ingestor("my_plugin")`. |
| Importing from a deep Firecube module | Import from `firecube.ingestor.api` or `firecube.core.api`. |
| Using a different entry-point key | Use the registered plugin name as the entry-point key. |

## Next Steps

- **[Plugin Development Overview](index.md)** — review the public authoring classes
- **[Create a Plugin](create-a-plugin.md)** — generate a package with matching registration metadata
- **[Install Your Plugin](install-a-plugin.md)** — verify discovery before publishing
- **[Configure a Plugin](cli-and-config.md)** — declare typed plugin options
