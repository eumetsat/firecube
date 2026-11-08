# Exceptions

This reference covers the error types exported from `firecube.sdk`.
Ingestor-layer errors inherit from `IngestorError`. `ManifestError`,
`SchemaDriftError`, and `StorageError` are shared error types that
`firecube.sdk` re-exports.

`ExtentUnknownError` is the core resolver error raised when a regular axis has
no fixed extent.

`UnboundedAxisError` is the `ConfigurationError` subclass the engine raises for
the same unbounded-axis case.

`MissingIrregularCoordinateError`, `DuplicateIrregularCoordinateError`, and
`NoDiscoveredItemsError` are raised during `IrregularTimeAxis(values=AUTO)`
discovery.

{{ render_api_summary("firecube.sdk", [
    "IngestorError",
    "ConfigurationError",
    "ExtentUnknownError",
    "UnboundedAxisError",
    "MissingIrregularCoordinateError",
    "DuplicateIrregularCoordinateError",
    "NoDiscoveredItemsError",
    "IndexedWriteCompilationError",
    "SchemaDriftError",
    "SchemaSizeMismatchError",
    "ManifestError",
    "StorageError",
    "ResumeConflictError",
    "RangeOverlapError",
    "WriteIntentRangeError",
]) }}

## Ingestor Errors

::: firecube.sdk.IngestorError

::: firecube.sdk.ConfigurationError

::: firecube.sdk.ExtentUnknownError

::: firecube.sdk.UnboundedAxisError

::: firecube.sdk.SchemaSizeMismatchError

::: firecube.sdk.ResumeConflictError

::: firecube.sdk.RangeOverlapError

::: firecube.sdk.WriteIntentRangeError

## Irregular Axis Discovery Errors

These errors are raised when `IrregularTimeAxis(values=AUTO)` discovery fails.
All three inherit from `ConfigurationError`.

::: firecube.sdk.MissingIrregularCoordinateError

::: firecube.sdk.DuplicateIrregularCoordinateError

::: firecube.sdk.NoDiscoveredItemsError

## Shared Errors

::: firecube.sdk.ManifestError

::: firecube.sdk.SchemaDriftError

::: firecube.sdk.StorageError

## IndexedWrite Compilation Errors

`IndexedWriteCompilationError` is raised when the engine cannot map an
`IndexedWrite.coordinate` to a slot index at compile time.

::: firecube.sdk.IndexedWriteCompilationError

## See Also

- [Discovery failure modes](parallelism.md#firecube.sdk.TimeAxis.discovered)
- [`build_write_intents`](templates.md#firecube.sdk.DirectZarrIngestor.build_write_intents)
- [Declare The Schema And Index](../guides/plugins/direct-zarr.md)
