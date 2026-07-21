---
hide:
  - navigation
  - toc
title: Firecube
---

# Firecube { style="display: none;" }

<p align="center" style="margin-top: 0; margin-bottom: 0.5rem;">
  <img src="assets/firecube-logo-title-2x.png" alt="Firecube" width="165">
</p>

<p align="center">
  <strong>A plugin-based batch ingestion CLI for turning Earth Observation products into analysis-ready datacubes.</strong>
</p>

<p align="center">
  Firecube handles the operational work of building and maintaining cubes, so you can focus on the data.
</p>

<div class="grid cards" markdown>

-   :material-puzzle:{ .lg .middle } **[Easy plugin system](concepts/plugins/index.md)**

    ---

    Plugins teach Firecube how to read a specific dataset. The engine
    owns the rest of the complexity.

-   :material-shield-check:{ .lg .middle } **[Ingestion safety](concepts/chunkmanager.md)**

    ---

    Failed runs resume where they left off. ChunkManager tracks exactly
    what was written, so no storage inspection is required.

-   :material-source-fork:{ .lg .middle } **[Built-in parallelism](concepts/parallelism.md)**

    ---

    Multiple workers per product, including disjoint Zarr slot ranges.
    ChunkManager coordinates parallel writes to prevent conflicts.

-   :material-cloud-sync:{ .lg .middle } **[Storage abstraction](concepts/storage.md)**

    ---

    Plugins write through a unified storage layer, so you can switch between local, remote, `fsspec`, and `obstore` writes with a simple flag.

-   :material-chart-line:{ .lg .middle } **[Observability](concepts/observability/index.md)**

    ---

    Push-based Prometheus metrics, JSON structured logs, OpenTelemetry
    traces. No sidecar required.

-   :material-wrench:{ .lg .middle } **[Operational safety first](operations/index.md)**

    ---

    Inspect runs, recover failed
    work, and clean up product state with built-in `firecube chunks` utilities.

</div>
