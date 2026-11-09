#include <benchmark/benchmark.h>
#include <log_ingest/engine.hpp>

// Same-thread queue mechanics: isolates synchronization overhead, not throughput
// between cores. Moving a preallocated payload avoids measuring string creation.
template <class Queue> void roundtrip(benchmark::State &state) {
    Queue queue(1024);
    std::string payload(static_cast<std::size_t>(state.range(0)), 'x'), out;
    for (auto _ : state) {
        benchmark::DoNotOptimize(queue.try_push(payload));
        benchmark::DoNotOptimize(queue.try_pop(out));
        payload.swap(out);
        benchmark::ClobberMemory();
    }
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK_TEMPLATE(roundtrip, log_ingest::LockedQueue)->Arg(32)->Arg(256);
BENCHMARK_TEMPLATE(roundtrip, log_ingest::SpscQueue)->Arg(32)->Arg(256);

// Includes allocation, producer retries, thread startup, drain and join. A batch
// amortizes startup. Sink observes every byte count but performs no file I/O.
template <class Queue> void ingestion(benchmark::State &state) {
    constexpr std::size_t batch = 65536;
    for (auto _ : state) {
        std::size_t bytes = 0;
        log_ingest::Engine<Queue> engine(
            4096, [&](std::string_view value) { bytes += value.size(); },
            log_ingest::OverflowPolicy::block);
        for (std::size_t i = 0; i < batch; ++i) {
            benchmark::DoNotOptimize(
                engine.submit(std::string(static_cast<std::size_t>(state.range(0)), 'x')));
        }
        engine.close();
        benchmark::DoNotOptimize(bytes);
        if (bytes != batch * static_cast<std::size_t>(state.range(0)))
            state.SkipWithError("record loss");
    }
    state.SetItemsProcessed(state.iterations() * batch);
    state.SetBytesProcessed(state.iterations() * batch * state.range(0));
}
BENCHMARK_TEMPLATE(ingestion, log_ingest::LockedQueue)->Arg(32)->Arg(256)->UseRealTime();
BENCHMARK_TEMPLATE(ingestion, log_ingest::SpscQueue)->Arg(32)->Arg(256)->UseRealTime();
