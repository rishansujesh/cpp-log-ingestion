#pragma once
#include "queue.hpp"
#include <exception>
#include <functional>
#include <ostream>
#include <string_view>

namespace log_ingest {
enum class OverflowPolicy { reject, block };
enum class SubmitResult { accepted, full, closed, too_large, sink_failed };
struct Stats { std::uint64_t accepted, written, rejected; };

// Sink runs on the writer thread. All close() calls must be serialized;
// destruction requires producer threads to be joined. SPSC close additionally
// requires its sole producer to be quiescent. Sink callbacks must not reenter.
template<class Queue>
class Engine {
public:
    using Sink = std::function<void(std::string_view)>;
    explicit Engine(std::size_t capacity, Sink sink,
                    OverflowPolicy policy = OverflowPolicy::reject,
                    std::size_t max_record_bytes = 64 * 1024)
        : queue_(capacity), sink_(std::move(sink)), policy_(policy), max_bytes_(max_record_bytes) {
        if (!sink_) throw std::invalid_argument("sink must be callable");
        worker_ = std::thread([this] { run(); });
    }
    Engine(const Engine&) = delete;
    Engine& operator=(const Engine&) = delete;
    ~Engine() { try { close(); } catch (...) {} }

    SubmitResult submit(std::string record) {
        if (record.size() > max_bytes_) return reject(SubmitResult::too_large);
        unsigned attempts = 0;
        for (;;) {
            if (failed_.load(std::memory_order_acquire)) return reject(SubmitResult::sink_failed);
            const auto result = queue_.try_push(record);
            if (result == PushResult::accepted) {
                accepted_.fetch_add(1, std::memory_order_relaxed);
                return SubmitResult::accepted;
            }
            if (result == PushResult::closed) return reject(SubmitResult::closed);
            if (policy_ == OverflowPolicy::reject) return reject(SubmitResult::full);
            backoff(attempts);
        }
    }
    // Drains accepted records, joins the writer, then reports sink failure.
    // A permanently blocked user sink can prevent completion; no forced cancel.
    void close() {
        queue_.close();
        if (worker_.joinable()) worker_.join();
        if (error_) std::rethrow_exception(error_);
    }
    Stats stats() const {
        return {accepted_.load(std::memory_order_relaxed), written_.load(std::memory_order_relaxed),
                rejected_.load(std::memory_order_relaxed)};
    }
private:
    SubmitResult reject(SubmitResult result) {
        rejected_.fetch_add(1, std::memory_order_relaxed);
        return result;
    }
    void run() noexcept {
        try {
            std::string record;
            while (queue_.wait_pop(record)) {
                sink_(record);
                written_.fetch_add(1, std::memory_order_relaxed);
            }
        } catch (...) {
            error_ = std::current_exception();
            failed_.store(true, std::memory_order_release);
        }
    }
    Queue queue_;
    Sink sink_;
    OverflowPolicy policy_;
    std::size_t max_bytes_;
    std::thread worker_;
    std::exception_ptr error_;
    std::atomic<bool> failed_{false};
    alignas(64) std::atomic<std::uint64_t> accepted_{0}, rejected_{0};
    alignas(64) std::atomic<std::uint64_t> written_{0};
};

inline auto line_sink(std::ostream& output) {
    return [&output](std::string_view record) {
        output.write(record.data(), static_cast<std::streamsize>(record.size()));
        output.put('\n');
        if (!output) throw std::runtime_error("log sink write failed");
    };
}
} // namespace log_ingest
