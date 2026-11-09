# Cloud measurement, 2026-11-09

These are illustrative measurements on a shared cloud machine, not a throughput guarantee.
The retained JSON includes all five repetitions plus aggregates. No other task tests or
compilers were running during this recorded run; host contention is uncontrolled.

- Source revision: `7c827d43dc2c1560bc3f103dd668ea7c34eb77f0`
- Compiler: GCC 14.2.0; CMake Release (`-O3 -DNDEBUG`), C++20.
- Google Benchmark: v1.9.1 (`c58e6d0710581e3a08d65c349664128a8d9a2461`).
- CPU/cache details and timestamp: see the JSON context.
- No CPU pinning or governor control.

```sh
./build-bench/queue-benchmark --benchmark_min_warmup_time=0.2 \
  --benchmark_min_time=0.2s --benchmark_repetitions=5 \
  --benchmark_display_aggregates_only=true \
  --benchmark_out=cloud-gcc14.json --benchmark_out_format=json
```

| Benchmark | Median records or roundtrips / second |
|---|---:|
| `roundtrip<log_ingest::LockedQueue>/32` | 33,577,293 |
| `roundtrip<log_ingest::LockedQueue>/256` | 34,328,127 |
| `roundtrip<log_ingest::SpscQueue>/32` | 88,325,998 |
| `roundtrip<log_ingest::SpscQueue>/256` | 88,324,941 |
| `ingestion<log_ingest::LockedQueue>/32/real_time` | 1,717,898 |
| `ingestion<log_ingest::LockedQueue>/256/real_time` | 1,809,919 |
| `ingestion<log_ingest::SpscQueue>/32/real_time` | 11,016,244 |
| `ingestion<log_ingest::SpscQueue>/256/real_time` | 9,587,148 |

Roundtrip results are same-thread queue operations; ingestion results include a writer thread and a byte-count sink. Neither measures disk logging. Read the full [methodology](../benchmarking.md) before comparing results.
