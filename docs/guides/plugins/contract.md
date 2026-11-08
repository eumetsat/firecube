# Package and Register a Plugin

## Goal

Prepare an external plugin package so Firecube can discover its registered
class through package metadata.

The generated ingestor already contains the decorator and product name. The
[`register_ingestor`](../../reference/templates.md#registration) decorator
sets `cls.name`; do not repeat the same value in a `name` class attribute.
Import every Firecube name from `firecube.sdk`.

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
firecube plugins list
firecube plugins describe my_plugin
firecube ingest my_plugin --show-options
```

Before publishing, run the plugin's tests, lint and format checks, and one local
ingestion using representative input data for the selected plugin path.

## Common Mistakes

| Mistake | Fix |
|---|---|
| Adding `name = "my_plugin"` beside the decorator | Keep only `@register_ingestor("my_plugin")`. |
| Importing from a deep Firecube module | Import from `firecube.sdk`. |
| Importing from `firecube.ingestor.api` or `firecube.core.api` | Import from `firecube.sdk`. See [Move To firecube.sdk](#move-to-firecubesdk). |
| Using a different entry-point key | Use the registered plugin name as the entry-point key. |

## Move To firecube.sdk

The modules `firecube.ingestor.api` and `firecube.core.api` are deprecated. They keep every name until v0.2.0. The first use of any name of either module raises a `DeprecationWarning`. The module warns once per process. The warning names v0.2.0 as the removal release.

1. Replace the import path in the package and in its tests:

    ```bash
    sed -i.bak 's/firecube\.\(ingestor\|core\)\.api/firecube.sdk/g' $(git ls-files '*.py')
    ```

    The option `-i.bak` works on GNU and BSD `sed`. It leaves a `.bak` copy of each file. Delete the copies when the tests pass.

2. Run the tests of the plugin.
3. If an import fails, the name is not in `firecube.sdk`. For example, `StorageConfig` is not in it. Put that one import back on the old module. The name stays there until v0.2.0.
4. [Open an issue](https://github.com/eumetsat/firecube/issues) that asks for the name in `firecube.sdk`. Give the name and say what your plugin needs it for.

A use of a name that is not in `firecube.sdk` raises a `DeprecationWarning` that names it. Use that warning to find the imports that must stay.

## Next Steps

- **[Plugin Development Overview](index.md)** — review the public authoring classes
- **[Create a Plugin](create-a-plugin.md)** — generate a package with matching registration metadata
- **[Install Your Plugin](install-a-plugin.md)** — verify discovery before publishing
- **[Add Plugin Configuration Options](add-config-options.md)** — declare typed plugin options
