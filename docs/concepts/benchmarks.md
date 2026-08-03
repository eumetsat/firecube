# Benchmarks

Use these workload profiles to understand what Firecube measurements look like.
Treat the numbers as examples, not promises. Storage hardware, network
bandwidth, source format, compression, and plugin code all change runtime.

Use [Performance Tuning](performance.md) to choose tuning knobs and
[Parallelism](parallelism.md) to choose a safe concurrency model.

## How To Read Benchmarks

Compare the timing split before comparing total runtime:

- pipeline time shows source work plus writes;
- upload time matters only for staged mode;
- per-file or per-timestamp time helps compare products of different sizes;
- throughput is only meaningful when the source work is similar.

Change one setting at a time. If batch size, worker count, compression, storage,
and sharding all change together, the benchmark will not tell you which change
helped.

## Large Spatial Grid, Append-Heavy

Workload: 288 source files per day, 20 variables on a 2200 x 1900 grid, one
timestamp per file, about 1.4 GB/day uncompressed. The plugin appends along the
time axis.

| Configuration | Pipeline | Upload | Total | Per timestamp |
|---|---:|---:|---:|---:|
| Staged, sharding, synchronous dask, network-attached block storage | ~47 min | ~10 min | **57 min** | 11.9 s |
| Same config, local NVMe pipeline workspace | **18 min** | 71 min | 89 min | 3.76 s |
| Pipeline only, no upload, local NVMe | **18 min** | - | 18 min | 3.76 s |
| Direct writes, no sharding, baseline | - | - | ~90 min | 18.8 s |

What this shows:

- staged writes plus sharding reduced the direct-write baseline on the same
  hardware;
- local SSD improved pipeline time, but upload still depended on network
  bandwidth;
- upload can dominate even when the ingest pipeline itself is fast.

## Small Files, CPU-Bound

Workload: hundreds of compressed source archives, about 3 GB total. The plugin
decompresses, parses, and transforms source data before writing.

| Configuration | Files | Data volume | Wall time | Throughput |
|---|---:|---:|---:|---:|
| Single process | 726 | ~2.9 GB | 44.0 min | ~1.0 MB/s |
| Parallel, 2 workers | 209 | ~0.8 GB | 12.6 min | ~1.9 MB/s |
| Resume mode | 725 | ~2.9 GB | 40.3 min | ~1.5 MB/s |
| Parallel and aligned batches | 745 | ~3.06 GB | **5.5 min** | **~9.2 MB/s** |

What this shows:

- CPU-bound source work can benefit strongly from pipeline workers;
- aligned batches reduce append-Zarr read-modify-write overhead;
- resume checks add some overhead, but keep retries inspectable.

## Same-Group Slot Scaling

This workload measured the optional parallel contract of a
`DirectZarrIngestor` plugin. An MTG FCI L1C FDHSI product used a preallocated
12-slot Zarr extent on S3, the `obstore` driver, `--write-mode direct`, shared
geolocation grids, and one disjoint slot per local ingest process.

For the same 12-slot window, a same-day run took 4,341 seconds with one process
and 371 seconds with 12 concurrent processes. That is an 11.70x speedup and
97.5% parallel efficiency for this workload.

Across the self-normalized, balanced strong-scaling points at 1, 2, 3, 4, 6,
and 12 processes, a least-squares Amdahl fit estimated a parallel fraction of
0.9976. In a separate self-normalized weak-scaling series with one slot per
process, measured efficiency remained between 96.6% and 100% through 12
processes; the Gustafson fit estimated a serial fraction of 0.0261.

These results demonstrate that preallocated, disjoint slot ranges can remove
the serialized same-group append bottleneck for a suitable product. They do not
show that `DirectZarrIngestor` is inherently faster than
`GenericZarrIngestor`: no comparison between those classes was made. Most
points had one repetition, the processes shared one host, and S3 endpoint and
background load varied during the campaign. Treat the result as workload
evidence, not a performance guarantee or an extrapolation beyond 12 processes.

## Benchmark Your Own Product

Start with one representative day or one bounded source window. Record:

- source file count and total input size;
- output format and write mode;
- product URI storage type and driver;
- `pipeline_batch_size`, `pipeline_workers`, and `upload_workers`;
- Zarr sharding, compression, and staged-write settings;
- pipeline duration, upload duration, and total duration;
- failed batches and storage errors.

Then change one setting and run again. Firecube metrics expose the timing split;
see [Metrics](observability/metrics.md) for the metric model.

## Next Steps

- **[Performance Tuning](performance.md)** — choose tuning knobs by bottleneck
- **[Parallelism](parallelism.md)** — choose a safe concurrency model
- **[Metrics](observability/metrics.md)** — inspect timing and throughput signals
