# Measuring queue costs

Use a Release build and record CPU model, OS, compiler, optimization flags, Google Benchmark version and the exact command. Keep raw JSON. Run at least five repetitions after warmup. Avoid running compilation, sanitizers, or other benchmarks at the same time.

`roundtrip` reuses a string and executes push/pop on one thread. It measures queue mechanics without inter-core cache transfer; it cannot establish concurrent ingestion throughput. `ingestion` uses batches of 65,536 records and a background sink that counts observed bytes. Wall time includes constructing strings, backpressure, thread startup, drainage and joining. It checks the total bytes to detect loss. Neither test models filesystem latency, flush behavior or durability.

Google Benchmark's `DoNotOptimize` and `ClobberMemory` protect observable operations. These barriers do not replace realistic data or remove scheduler noise. The byte-count sink observes payload length; it does not traverse every character.

SPSC is not guaranteed to win on every payload or CPU. Allocation, adaptive waits and cache-line layout can dominate. Report medians and variation, distinguish operations from records (one roundtrip includes both a push and a pop), and avoid extrapolating a shared VM result to a production system. A successful test or sanitizer run is evidence, not a proof of race freedom.
