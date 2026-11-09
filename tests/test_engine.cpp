#include <log_ingest/engine.hpp>
#include <future>
#include <iostream>
#include <sstream>
#include <vector>

#define CHECK(expr) do { if (!(expr)) throw std::runtime_error("check failed: " #expr); } while (false)
using namespace log_ingest;

template<class Queue> void queue_contract() {
    Queue queue(2);
    std::string a = "a", b = "b", c = "c", out;
    CHECK(queue.try_push(a) == PushResult::accepted);
    CHECK(queue.try_push(b) == PushResult::accepted);
    CHECK(queue.try_push(c) == PushResult::full);
    CHECK(c == "c");
    CHECK(queue.try_pop(out) && out == "a");
    CHECK(queue.try_push(c) == PushResult::accepted);
    queue.close();
    CHECK(queue.try_push(a) == PushResult::closed);
    CHECK(queue.wait_pop(out) && out == "b");
    CHECK(queue.wait_pop(out) && out == "c");
    CHECK(!queue.wait_pop(out));
    queue.close();
}

template<class Queue> void ordered_stress() {
    constexpr int count = 200000;
    int next = 0;
    Engine<Queue> engine(8, [&](std::string_view record) {
        CHECK(record == std::to_string(next++));
    }, OverflowPolicy::block);
    for (int i = 0; i < count; ++i) CHECK(engine.submit(std::to_string(i)) == SubmitResult::accepted);
    engine.close();
    CHECK(next == count);
    CHECK(engine.stats().accepted == count);
    CHECK(engine.stats().written == count);
    CHECK(engine.submit("late") == SubmitResult::closed);
    engine.close();
}

template<class Queue> void rejection_and_failure() {
    std::promise<void> entered, release;
    auto gate = release.get_future().share();
    Engine<Queue> engine(2, [&](std::string_view) { entered.set_value(); gate.wait(); throw std::runtime_error("disk full"); });
    CHECK(engine.submit("first") == SubmitResult::accepted);
    entered.get_future().wait();
    CHECK(engine.submit("second") == SubmitResult::accepted);
    CHECK(engine.submit("third") == SubmitResult::accepted);
    CHECK(engine.submit("overflow") == SubmitResult::full);
    CHECK(engine.submit(std::string(65537, 'x')) == SubmitResult::too_large);
    release.set_value();
    bool failed = false;
    try { engine.close(); } catch (const std::runtime_error&) { failed = true; }
    CHECK(failed);
    CHECK(engine.stats().written == 0);
    CHECK(engine.stats().accepted == 3);
    CHECK(engine.stats().rejected == 2);
    CHECK(engine.submit("late") == SubmitResult::sink_failed);
}

template<class Queue> void blocked_producer_observes_failure() {
    std::promise<void> entered, release;
    auto gate = release.get_future().share();
    Engine<Queue> engine(2, [&](std::string_view) {
        entered.set_value();
        gate.wait();
        throw std::runtime_error("sink failure");
    }, OverflowPolicy::block);
    CHECK(engine.submit("first") == SubmitResult::accepted);
    entered.get_future().wait();
    CHECK(engine.submit("second") == SubmitResult::accepted);
    CHECK(engine.submit("third") == SubmitResult::accepted);
    // The sole producer stays on this thread; the helper only releases the sink.
    std::thread releaser([&] {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
        release.set_value();
    });
    const auto result = engine.submit("blocked");
    releaser.join();
    CHECK(result == SubmitResult::sink_failed);
    bool failed = false;
    try { engine.close(); } catch (const std::runtime_error&) { failed = true; }
    CHECK(failed);
}

void multiple_producers() {
    constexpr int producers = 4, records = 10000;
    std::vector<int> next(producers);
    Engine<LockedQueue> engine(16, [&](std::string_view record) {
        const int id = record[0] - '0';
        CHECK(std::stoi(std::string(record.substr(2))) == next.at(id)++);
    }, OverflowPolicy::block);
    std::vector<std::thread> threads;
    std::atomic<bool> ok{true};
    for (int id = 0; id < producers; ++id) threads.emplace_back([&, id] {
        for (int i = 0; i < records; ++i)
            if (engine.submit(std::to_string(id) + ":" + std::to_string(i)) != SubmitResult::accepted) ok = false;
    });
    for (auto& thread : threads) thread.join();
    engine.close();
    CHECK(ok);
    for (auto n : next) CHECK(n == records);
}

void concurrent_locked_close() {
    Engine<LockedQueue> engine(2, [](std::string_view) {}, OverflowPolicy::block);
    std::promise<void> started;
    auto producer = std::thread([&] {
        started.set_value();
        while (engine.submit("x") == SubmitResult::accepted) {}
    });
    started.get_future().wait();
    engine.close();
    producer.join();
    CHECK(engine.stats().accepted == engine.stats().written);
}

int main() {
    try {
        queue_contract<LockedQueue>(); queue_contract<SpscQueue>();
        ordered_stress<LockedQueue>(); ordered_stress<SpscQueue>();
        rejection_and_failure<LockedQueue>(); rejection_and_failure<SpscQueue>();
        blocked_producer_observes_failure<LockedQueue>();
        blocked_producer_observes_failure<SpscQueue>();
        multiple_producers(); concurrent_locked_close();
        for (auto capacity : {0, 1, 3}) {
            bool invalid = false;
            try { SpscQueue queue(capacity); } catch (const std::invalid_argument&) { invalid = true; }
            CHECK(invalid);
        }
        std::ostringstream output;
        { Engine<SpscQueue> engine(2, line_sink(output), OverflowPolicy::block);
          CHECK(engine.submit("hello") == SubmitResult::accepted); }
        CHECK(output.str() == "hello\n");
        { Engine<SpscQueue> empty(2, [](std::string_view) {}); empty.close(); }
        std::cout << "All queue and engine tests passed\n";
    } catch (const std::exception& error) {
        std::cerr << error.what() << '\n'; return 1;
    }
}
