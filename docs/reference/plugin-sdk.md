# Plugin SDK

The module `firecube.sdk` is the module that a plugin imports to build a Firecube plugin. It provides templates, hooks, context and result types, array specs, errors, and storage and time helpers. The pages of this group document it.

## Import Rule

Import every Firecube name that your plugin uses from `firecube.sdk`. The optional module `firecube.ingestor.extensions` is the one exception. It has its own page in this group.

## Deprecated Modules

Earlier plugins import from `firecube.ingestor.api` and `firecube.core.api`. Both modules are deprecated and Firecube will remove them in v0.2.0. Each module warns once per process. Import the same names from `firecube.sdk` instead. If `firecube.sdk` does not provide a name, you can still import that name from the old module until v0.2.0. [Open an issue](https://github.com/eumetsat/firecube/issues) with the name and what your plugin needs it for. Then the maintainers can add it to `firecube.sdk`.

## Pages Of The Group

- [Templates](templates.md): the template classes that a plugin subclasses, and the registration decorator.
- [Hooks & Lifecycle](hooks.md): the `BaseIngestor` hooks for batch shaping, lifecycle, validation and metrics.
- [Context & Results](context.md): the context that hooks receive, and the result and metrics types they return.
- [Exceptions](exceptions.md): the error types that Firecube raises and a plugin can raise or catch.
- [Index Specification](parallelism.md): the types that declare and resolve the index of a direct-Zarr plugin.
- [Extensions](extensions.md): the optional regridding and DuckDB helpers in `firecube.ingestor.extensions`.
- [Metadata Migration](migration.md): the types that a plugin uses to declare a metadata-only migration step.

## Names Without A Reference Entry

`firecube.sdk` provides these names. You can import and use them. Their docstrings are the only reference text.

- `CatalogGroupInfo`
- `IngestContext`
- `Ingestor`
- `ItemManifestEntry`
- `SourceFile`
- `decode_time_array`
- `discover_ingestors`
- `read_chunk_grid_with_shards`

## See Also

- [Configuration](config.md): the config dataclasses of a plugin.
- [Core Utilities](core-utilities.md): the URI, filesystem, discovery and time helpers in `firecube.sdk`.
- [Operations](operations.md): to run the same operations as the CLI from Python code, for example to archive or release a product.
