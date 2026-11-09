# C++ Log Ingestion Engine

[![CI](https://github.com/rishansujesh/cpp-log-ingestion/actions/workflows/ci.yml/badge.svg)](https://github.com/rishansujesh/cpp-log-ingestion/actions/workflows/ci.yml)

A C++20 experiment in bounded asynchronous log ingestion. Compare a mutex/condition-variable baseline against a cache-line-aligned single-producer/single-consumer ring, using the same writer and explicit overflow policies.

## Build and run

Requires CMake 3.20+, a C++20 compiler, and threads. Core builds have no downloaded dependencies.

```sh
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j2
ctest --test-dir build --output-on-failure
printf 'hello\nworld\n' | ./build/log-ingest spsc output.log
# The baseline supports multiple producers through the library API.
printf 'baseline\n' | ./build/log-ingest locked output.log
```

The CLI appends newline-delimited input to the output file, applies blocking backpressure, and reports accepted/written counts on stderr. It returns 2 for invalid arguments and 1 for an I/O or record-size error. A record is limited to 64 KiB by default; the CLI reads a complete line before checking its length. Successful flush is not an `fsync` durability guarantee.

## Library example

```cpp
#include <log_ingest/engine.hpp>
#include <fstream>

std::ofstream output("app.log", std::ios::app);
log_ingest::Engine<log_ingest::SpscQueue> engine(
    4096, log_ingest::line_sink(output), log_ingest::OverflowPolicy::block);
auto result = engine.submit("service started");
// Handle every non-accepted result in application code.
engine.close();             // drain, join, and rethrow writer errors
output.flush();             // sink/stream lifetime belongs to the caller
```

Use the `log_ingest` CMake interface target via `add_subdirectory`, or include the headers and link your platform's thread library.

| Backend | Producers / consumers | Capacity | Waiting |
|---|---|---|---|
| `LockedQueue` | Multiple / multiple; engine uses one consumer | Any positive size | Consumer condition variable |
| `SpscQueue` | Exactly one / one | Power of two, at least 2 | Adaptive yield / 50 μs sleep |

`reject` returns `full` immediately. `block` retries until accepted, closed, or the sink fails. Both preserve accepted records during **normal** shutdown. The queue never silently overwrites old records. Capacity is fixed: resize is deliberately excluded to keep ownership and reclamation simple.

## Concurrency and error contract

- `SpscQueue::try_push` and `close` belong to the single producer. Stop/join the producer before another thread closes the engine. Do not use the SPSC backend from multiple producer threads.
- The locked backend permits `close` racing with producers. Serialize all engine `close` calls, and join producers before destroying the engine.
- Successful `close` drains accepted records and joins the writer. Calling it again is safe. The destructor also drains but cannot report errors; explicitly call `close` to observe sink failures.
- On a sink exception, the writer stops, blocking producers observe `sink_failed`, and `close` rethrows. Already accepted but unwritten records are **not** guaranteed delivery. `accepted - written` quantifies that gap after producers stop. There is no retry or durable spool.
- Sinks must finish and must not reenter the engine. A hung sink can prevent shutdown. Stream flushing and durability are caller responsibilities.
- Stats are atomic but form an approximate snapshot during concurrent activity; they are exact after producers stop and `close` returns.

The SPSC queue's synchronization is lock-free only where 64-bit atomics are lock-free (`SpscQueue::atomics_always_lock_free`). String allocation, the sink, backoff, and the complete ingestion engine are **not** claimed to be lock-free. See the [memory-ordering design](docs/design.md).

## Benchmarks and sanitizers

Install Google Benchmark (for example `libbenchmark-dev` on Debian/Ubuntu), then:

```sh
cmake -S . -B build-bench -DCMAKE_BUILD_TYPE=Release -DLOG_INGEST_BENCHMARKS=ON
cmake --build build-bench -j2
./build-bench/queue-benchmark --benchmark_min_warmup_time=0.2 \
  --benchmark_min_time=0.2s --benchmark_repetitions=5 \
  --benchmark_out=benchmark.json --benchmark_out_format=json

cmake -S . -B build-asan -DCMAKE_BUILD_TYPE=Debug -DLOG_INGEST_SANITIZER=address
cmake --build build-asan -j2
ctest --test-dir build-asan --output-on-failure
# Configure a separate build with -DLOG_INGEST_SANITIZER=thread for TSan.
```

Benchmarks separate same-thread queue mechanics from producer/writer ingestion batches. The latter includes allocation, backpressure, thread startup, drainage, and join; its sink counts bytes rather than writing to disk. Neither represents disk throughput. See [benchmark methodology](docs/benchmarking.md) for interpretation and reproduction.

## Project status

Version 0.1.0: bounded baseline and SPSC queue, asynchronous writer, CLI, explicit errors, concurrency tests, Google Benchmark workloads, and sanitizer CI. This initial release has no production reliability history. A lock-free MPSC queue, batching, durable spooling, rotation, and dynamic resizing are potential future work, not implemented features.

Contributions follow [CONTRIBUTING.md](CONTRIBUTING.md). MIT [license](LICENSE).
