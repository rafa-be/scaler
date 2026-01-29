#include "scaler/uv_ymq/lock_free_event_loop_thread.h"

#include <functional>
#include <iostream>
#include <queue>
#include <thread>
#include <utility>

#include "scaler/logging/logging.h"
#include "scaler/wrapper/uv/error.h"

namespace scaler {
namespace uv_ymq {

LockFreeEventLoopThread::LockFreeEventLoopThread() noexcept
    : _loop(UV_EXIT_ON_ERROR(scaler::wrapper::uv::Loop::init()))
    , _executeQueueCount(0)
    , _executeQueue()
    , _executeAsync(UV_EXIT_ON_ERROR(
          scaler::wrapper::uv::Async::init(
              _loop, std::bind_front(&LockFreeEventLoopThread::processExecuteCallbacks, this))))
{
    _thread = std::jthread([this](std::stop_token stop_token) { run(); });
}

LockFreeEventLoopThread::~LockFreeEventLoopThread() noexcept
{
    assert(_thread.joinable());

    executeThreadSafe([this]() { _loop.stop(); });  // _loop.stop() must be called from the loop's thread.
}

scaler::wrapper::uv::Loop& LockFreeEventLoopThread::loop() noexcept
{
    return _loop;
}

void LockFreeEventLoopThread::executeThreadSafe(Callback callback) noexcept
{
    _executeQueue.enqueue(std::move(callback));
    _executeQueueCount.fetch_add(1, std::memory_order_release);

    // Wake up the event loop
    UV_EXIT_ON_ERROR(_executeAsync.send());
}

void LockFreeEventLoopThread::run() noexcept
{
    _loop.run(UV_RUN_DEFAULT);
}

void LockFreeEventLoopThread::processExecuteCallbacks() noexcept
{
    // Read and reset the callback count atomically
    size_t nCallbacks = _executeQueueCount.exchange(0, std::memory_order_acquire);

    for (size_t i = 0; i < nCallbacks; ++i) {
        Callback callback;

        while (!_executeQueue.try_dequeue(callback))
            ;

        callback();
    }
}

}  // namespace uv_ymq
}  // namespace scaler
