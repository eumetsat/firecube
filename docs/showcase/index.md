---
hide:
  - toc
  - navigation
---

<div class="showcase-intro" markdown>

# Showcase

Explore what you can build with Firecube.

<div class="showcase-intro-links" markdown>

**New to Firecube?** [Quickstart](../quickstart/index.md) runs an installed plugin.
**Building your own?** [Plugin Development](../guides/plugins/index.md) covers individual authoring tasks.

</div>

</div>

<div class="showcase-features" markdown>

<div class="showcase-feature" markdown>
<div class="showcase-feature-copy" markdown>

<span class="showcase-label">Beginner notebook · Generated data</span>

## Firecube 101: NetCDF To Zarr

Create a cube from three days of data, append the next three, and check that
retrying an arrival adds no duplicates. Small files, no credentials.

<div class="showcase-actions" markdown>

[Open the notebook](netcdf-to-zarr.ipynb){ .md-button .md-button--primary }
<a class="md-button notebook-download" href="netcdf-to-zarr/netcdf-to-zarr.ipynb" download><svg viewBox="0 0 24 24" aria-hidden="true"><path d="M19 9h-4V3H9v6H5l7 7 7-7zM5 18v3h14v-3H5z"/></svg>Download notebook</a>

</div>

</div>
<div class="showcase-feature-image" markdown>

[![A Zarr cube grows from three days to six: original dates in blue and appended dates in orange.](../assets/images/netcdf-to-zarr-preview.png)](netcdf-to-zarr.ipynb)

<span class="showcase-caption">Two arrivals · One cube · Six unique days</span>

</div>
</div>

<div class="showcase-feature" markdown>
<div class="showcase-feature-copy" markdown>

<span class="showcase-label">Featured notebook · Real satellite data</span>

## Sentinel-3 SLSTR Level 2 Fire Radiative Power

Explore thermal detections around Spain in July 2026. Download the standard
MWIR tables, write Parquet with Firecube, and compare days on an interactive map.

<div class="showcase-actions" markdown>

[Open the notebook](sentinel3-fire-detections.ipynb){ .md-button .md-button--primary }
<a class="md-button notebook-download" href="sentinel3-fire-detections/sentinel3-fire-detections.ipynb" download><svg viewBox="0 0 24 24" aria-hidden="true"><path d="M19 9h-4V3H9v6H5l7 7 7-7zM5 18v3h14v-3H5z"/></svg>Download notebook</a>

</div>

</div>
<div class="showcase-feature-image" markdown>

[![Sentinel-3 SLSTR FRP detections around Spain on 25 July 2026, shown on an OpenStreetMap basemap.](../assets/images/sentinel3-frp-preview.png)](sentinel3-fire-detections.ipynb)

<span class="showcase-caption">25 July 2026 · Colour shows fire radiative power</span>

</div>
</div>

<div class="showcase-feature" markdown>
<div class="showcase-feature-copy" markdown>

<span class="showcase-label">Benchmark notebook · Recorded runs</span>

## Slot-Based Parallelism: MTG FCI L1C

Ingest two hours of FCI observations with the public plugin. Reproduce the
tuned parallel setup and compare your timings with recorded runs of about
39 seconds for twelve acquisitions.

<div class="showcase-actions" markdown>

[Open the notebook](mtg-fci-l1c-benchmarks.ipynb){ .md-button .md-button--primary }
<a class="md-button notebook-download" href="mtg-fci-l1c-benchmarks/mtg-fci-l1c-benchmarks.ipynb" download><svg viewBox="0 0 24 24" aria-hidden="true"><path d="M19 9h-4V3H9v6H5l7 7 7-7zM5 18v3h14v-3H5z"/></svg>Download notebook</a>

</div>

</div>
<div class="showcase-feature-image" markdown>

[![Recorded ingestion wall time falls from a median of 250.87 seconds at one process to 39.01 seconds at twelve processes for the same twelve FCI acquisitions.](../assets/images/mtg-fci-benchmark-preview.png)](mtg-fci-l1c-benchmarks.ipynb)

<span class="showcase-caption">12 acquisitions · One ARM host · Local Zarr writes</span>

</div>
</div>

</div>
