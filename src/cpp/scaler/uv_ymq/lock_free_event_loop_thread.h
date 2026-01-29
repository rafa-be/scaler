#pragma once

#include <atomic>
#include <expected>
#include <memory>
#include <optional>
#include <thread>

#include "scaler/utility/move_only_function.h"
#include "scaler/wrapper/uv/async.h"
#include "scaler/wrapper/uv/loop.h"
#include "scaler/ymq/third_party/concurrentqueue.h"

namespace scaler {
namespace uv_ymq {

// A thread running its own libuv event loop with lock-free callback queuing.
//
// This implementation uses moodycamel::ConcurrentQueue for lock-free enqueue operations.
class LockFreeEventLoopThread {
public:
    using Callback = scaler::utility::MoveOnlyFunction<void()>;

    LockFreeEventLoopThread() noexcept;
    ~LockFreeEventLoopThread() noexcept;

    LockFreeEventLoopThread(LockFreeEventLoopThread&&) noexcept            = default;
    LockFreeEventLoopThread& operator=(LockFreeEventLoopThread&&) noexcept = default;

    LockFreeEventLoopThread(const LockFreeEventLoopThread&) noexcept            = delete;
    LockFreeEventLoopThread& operator=(const LockFreeEventLoopThread&) noexcept = delete;

    scaler::wrapper::uv::Loop& loop() noexcept;

    // Schedule the execution of a function within the event loop thread.
    //
    // Thread-safe and lock-free.
    void executeThreadSafe(Callback callback) noexcept;

private:
    scaler::wrapper::uv::Loop _loop;

    std::jthread _thread;

    // Lock-free queue for callbacks. Use an atomic counter to keep track of the current queue size.
    std::atomic<uint64_t> _executeQueueCount;
    moodycamel::ConcurrentQueue<Callback> _executeQueue;
    scaler::wrapper::uv::Async _executeAsync;

    void run() noexcept;

    void processExecuteCallbacks() noexcept;
};

}  // namespace uv_ymq
}  // namespace scaler
