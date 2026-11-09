#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>

namespace log_ingest {
enum class PushResult { accepted, full, closed };

inline void backoff(unsigned &attempts) {
    if (++attempts < 64)
        std::this_thread::yield();
    else
        std::this_thread::sleep_for(std::chrono::microseconds(50));
}

// Bounded MPSC/MPMC reference implementation. close() linearizes under mutex.
class LockedQueue {
  public:
    explicit LockedQueue(std::size_t capacity) : capacity_(capacity) {
        if (capacity == 0)
            throw std::invalid_argument("capacity must be positive");
    }
    PushResult try_push(std::string &value) {
        std::lock_guard lock(mutex_);
        if (closed_)
            return PushResult::closed;
        if (queue_.size() == capacity_)
            return PushResult::full;
        queue_.push_back(std::move(value));
        ready_.notify_one();
        return PushResult::accepted;
    }
    bool try_pop(std::string &value) {
        std::lock_guard lock(mutex_);
        if (queue_.empty())
            return false;
        value = std::move(queue_.front());
        queue_.pop_front();
        return true;
    }
    bool wait_pop(std::string &value) {
        std::unique_lock lock(mutex_);
        ready_.wait(lock, [this] { return closed_ || !queue_.empty(); });
        if (queue_.empty())
            return false;
        value = std::move(queue_.front());
        queue_.pop_front();
        return true;
    }
    void close() {
        std::lock_guard lock(mutex_);
        closed_ = true;
        ready_.notify_all();
    }

  private:
    const std::size_t capacity_;
    std::mutex mutex_;
    std::condition_variable ready_;
    std::deque<std::string> queue_;
    bool closed_ = false;
};

// Exactly one producer and one consumer. close() belongs to the producer.
// A full/closed push leaves its argument untouched. Capacity is fixed.
class SpscQueue {
    struct alignas(64) Slot {
        std::atomic<std::uint64_t> sequence{0};
        std::string value;
    };

  public:
    explicit SpscQueue(std::size_t capacity) : capacity_(capacity), mask_(capacity - 1) {
        if (capacity < 2 || (capacity & (capacity - 1)) != 0)
            throw std::invalid_argument("SPSC capacity must be a power of two >= 2");
        slots_ = std::make_unique<Slot[]>(capacity);
        for (std::size_t i = 0; i < capacity; ++i)
            slots_[i].sequence.store(i, std::memory_order_relaxed);
    }
    PushResult try_push(std::string &value) {
        if (closed_.load(std::memory_order_relaxed))
            return PushResult::closed;
        auto &slot = slots_[write_ & mask_];
        // Acquire pairs with consumer reclamation: its move must finish first.
        if (slot.sequence.load(std::memory_order_acquire) != write_)
            return PushResult::full;
        slot.value = std::move(value);
        slot.sequence.store(write_ + 1, std::memory_order_release);
        ++write_;
        return PushResult::accepted;
    }
    bool try_pop(std::string &value) {
        auto &slot = slots_[read_ & mask_];
        // Acquire makes the producer's non-atomic string assignment visible.
        if (slot.sequence.load(std::memory_order_acquire) != read_ + 1)
            return false;
        value = std::move(slot.value);
        slot.sequence.store(read_ + capacity_, std::memory_order_release);
        ++read_;
        return true;
    }
    bool wait_pop(std::string &value) {
        unsigned attempts = 0;
        for (;;) {
            if (try_pop(value))
                return true;
            // Recheck after acquiring close: the final publication may have raced
            // with the first pop, but it happens-before this second pop.
            if (closed_.load(std::memory_order_acquire))
                return try_pop(value);
            backoff(attempts);
        }
    }
    void close() { closed_.store(true, std::memory_order_release); }
    static constexpr bool atomics_always_lock_free =
        std::atomic<std::uint64_t>::is_always_lock_free;

  private:
    const std::uint64_t capacity_;
    const std::uint64_t mask_;
    std::unique_ptr<Slot[]> slots_;
    alignas(64) std::uint64_t write_ = 0;
    alignas(64) std::uint64_t read_ = 0;
    alignas(64) std::atomic<bool> closed_{false};
};
} // namespace log_ingest
