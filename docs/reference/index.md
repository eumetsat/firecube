# API Reference

Plugins are written against three public import surfaces:

- `firecube.ingestor.api` — templates, hooks, context, and result types
- `firecube.core.api` — storage, URI, time, and dataset-preparation utilities
- `firecube.ingestor.extensions` — optional regridding and DuckDB helpers

Import only from these modules. Deep imports into other Firecube modules are
not part of the public contract and are rejected by the plugin contract test.

<div class="grid cards" markdown>

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

-   :material-tools:{ .lg .middle } **Core Utilities**

    ---

    URI parsing, filesystem access, time conversion, and netCDF/HDF5
    preparation helpers from `firecube.core.api`.

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
