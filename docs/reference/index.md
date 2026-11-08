# API Reference

Firecube has two public Python modules.

- `firecube.sdk` is for plugin authors. Import from it to build a plugin. It provides templates, hooks, context and result types, array specs, errors, and storage and time helpers.
- `firecube.operations` is for scripts and tools. It holds the Python functions that do what the `firecube` commands do on a finished product.

Plugins can also import the optional module `firecube.ingestor.extensions`. It holds regridding and DuckDB helpers.

Import only from these modules. Other Firecube modules can change without notice.

!!! warning "Deprecated modules"

    The modules `firecube.ingestor.api` and `firecube.core.api` are deprecated. Import from `firecube.sdk` instead. Firecube will remove both modules in v0.2.0.

The Plugin SDK pages document `firecube.sdk` and `firecube.ingestor.extensions`. The Operations page documents `firecube.operations`.

<div class="grid cards" markdown>

-   :material-package-variant-closed:{ .lg .middle } **Plugin SDK**

    ---

    The import rule for `firecube.sdk`, the deprecated `api` modules, and a
    map of the Plugin SDK pages.

    [:octicons-arrow-right-24: Plugin SDK](plugin-sdk.md)

-   :material-puzzle:{ .lg .middle } **Templates**

    ---

    The template classes plugin authors subclass, and the hooks each
    template requires.

    [:octicons-arrow-right-24: Templates](templates.md)

-   :material-hook:{ .lg .middle } **Hooks & Lifecycle**

    ---

    The full `BaseIngestor` hook surface: batch shaping, lifecycle,
    validation, and metrics hooks.

    [:octicons-arrow-right-24: Hooks & Lifecycle](hooks.md)

-   :material-package-variant:{ .lg .middle } **Context & Results**

    ---

    `PluginContext`, storage sessions, batch inputs, and the result and
    metrics types hooks receive and return.

    [:octicons-arrow-right-24: Context & Results](context.md)

-   :material-alert-circle:{ .lg .middle } **Exceptions**

    ---

    The error types Firecube raises and plugins may raise or catch.

    [:octicons-arrow-right-24: Exceptions](exceptions.md)

-   :material-arrow-split-vertical:{ .lg .middle } **Slot-Range Parallelism**

    ---

    Types and methods for parallel `DirectZarrIngestor` writes across
    disjoint slot ranges.

    [:octicons-arrow-right-24: Slot-Range Parallelism](parallelism.md)

-   :material-toy-brick:{ .lg .middle } **Extensions**

    ---

    Optional HEALPix and lat/lon regridding helpers and DuckDB batch
    support.

    [:octicons-arrow-right-24: Extensions](extensions.md)

-   :material-database-edit:{ .lg .middle } **Metadata Migration**

    ---

    The types a plugin uses to declare a metadata-only migration step.

    [:octicons-arrow-right-24: Metadata Migration](migration.md)

-   :material-cog-play:{ .lg .middle } **Operations**

    ---

    The Python functions that do what the `firecube` commands do on a
    finished product: archive, snapshot, migrate, release, and maintain
    its Zarr arrays.

    [:octicons-arrow-right-24: Operations](operations.md)

-   :material-tools:{ .lg .middle } **Core Utilities**

    ---

    URI parsing, filesystem access, time conversion, and netCDF/HDF5
    preparation helpers from `firecube.sdk`.

    [:octicons-arrow-right-24: Core Utilities](core-utilities.md)

-   :material-cog:{ .lg .middle } **Configuration**

    ---

    Config dataclasses: storage, engine, template, and plugin option
    schemas.

    [:octicons-arrow-right-24: Configuration](config.md)

-   :material-console:{ .lg .middle } **CLI**

    ---

    The complete `firecube` command tree, generated from the live CLI.

    [:octicons-arrow-right-24: CLI Reference](cli.md)

-   :material-chart-line:{ .lg .middle } **Observability**

    ---

    Metrics, environment variables, and telemetry surfaces.

    [:octicons-arrow-right-24: Observability](observability.md)

-   :material-database:{ .lg .middle } **Storage Drivers**

    ---

    Driver capabilities and selection rules.

    [:octicons-arrow-right-24: Storage Drivers](storage-drivers.md)

-   :material-file-tree:{ .lg .middle } **Control-Plane Spec**

    ---

    What ChunkManager writes into `.firecube/` to make resume, recovery,
    and parallel writes safe.

    [:octicons-arrow-right-24: Control-Plane Spec](control-plane-spec.md)

</div>
