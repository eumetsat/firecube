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

Workload: a `DirectZarrIngestor` plugin uses the optional parallel contract. An
MTG FCI L1C FDHSI product goes to a preallocated 12-slot Zarr extent on S3 with
the `obstore` driver and `--write-mode direct`. The product shares its
geolocation grids. Each local ingest process writes one disjoint slot.

| Configuration | Processes | Wall time | Speedup | Efficiency |
|---|---:|---:|---:|---:|
| Single process | 1 | 4,341 s | - | - |
| Concurrent processes | 12 | **371 s** | **11.70x** | **97.5%** |

What this shows:

- Preallocated, disjoint slot ranges can remove the serialized same-group
  append bottleneck for a suitable product.
- The strong-scaling points are self-normalized and balanced, at 1, 2, 3, 4, 6,
  and 12 processes. A least-squares Amdahl fit of them estimated a parallel
  fraction of 0.9976.
- A separate weak-scaling series is self-normalized and has one slot per
  process. Its measured efficiency stayed between 96.6% and 100% through 12
  processes.
- The Gustafson fit of the weak-scaling series estimated a serial fraction of
  0.0261.

Treat this as workload evidence. Most points had one repetition. The processes
shared one host. S3 endpoint and background load varied during the campaign.
The campaign did not compare `DirectZarrIngestor` with `GenericZarrIngestor`.
The result does not show that one is inherently faster. It is not a
performance guarantee, and it does not extend beyond 12 processes.

## Unit Write Buffer, Sharded S3

This workload measured the option `zarr_unit_write_buffer`, explained in
[How The Unit Write Buffer Works](output-formats/zarr/unit-write-buffer.md) and
described in [Performance Tuning](performance.md#if-zarr-writes-are-slow). An MTG FCI L1C
product of two hours of FCI observations from 1 July 2025 used the default
sharded layout. The
run used 12 processes with one slot each, to a remote S3 object store.
The processes ran on an x86_64 VM in the same cloud as the bucket. The VM had
64 logical CPUs and 126 GiB of RAM.

The test had 6 paired rounds. Each round ran the buffer off and on back to back,
in alternating order. In every
pair, the two stores were identical. The wall time ran from the launch of the 12
processes to the last exit.

| Configuration | Wall time | Median | Payload written | Payload read back |
|---|---:|---:|---:|---:|
| Buffer off | 358 to 395 s | 364 s | 94.79 GB | 87.6 GB |
| Buffer on | 90 to 92 s | 91 s | 7.19 GB | ~0 |

The payload written was 13.19 times the final payload with the buffer off and
1.00 times with it on. The wall-time ratio was about 4.0, with a range of 3.93
to 4.41. This ratio is for this workload only.

The buffer costs memory. The peak per process rose from 1.1 to 2.55 GiB. The
process that also writes the static grids rose from 2.5 to 3.6 GiB. At the same
moment, the 12 processes held about 14.2 GiB with the buffer off and about
25.7 GiB with it on. These figures are medians of 3 rounds. The highest total seen
at one moment in the 6 rounds was 26.0 GiB, so they are not a capacity bound.

This result comes from one workload, one VM, one provider and 6 pairs. Other
setups give other results. An unsharded layout showed the same writes with and
without the buffer. To repeat the measurement on your own bucket, see the
notebook [Unit Write Buffer](../showcase/unit-write-buffer.ipynb).

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
