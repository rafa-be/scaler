
#pragma once

// #include <algorithm>
// #include <chrono>
// #include <concepts>
// #include <map>
#include <cstdint>
#include <queue>
#include <set>

// // #include "scaler/io/ymq/event_loop.h"
#include "scaler/ymq/configuration.h"
// #include "scaler/io/ymq/event_manager.h"
// #include "scaler/io/ymq/event_selector.h"
#include "scaler/utility/error.h"
#include "scaler/utility/io_result.h"
#include "scaler/utility/pipe/pipe.h"
#include "scaler/utility/timestamp.h"
#include "scaler/ymq/event/event_loop.h"
#include "scaler/ymq/event/event_type.h"
#include "scaler/ymq/event/selector/selector.h"
#include "scaler/ymq/event_manager.h"
#include "scaler/ymq/third_party/concurrentqueue.h"

#ifdef __linux__
#include "scaler/ymq/event/selector/epoll_selector.h"
#endif  // __linux__

#ifdef __APPLE__
#include "scaler/ymq/event/selector/kqueue_selector.h"
#endif  // __APPLE__

using namespace scaler::ymq::event::selector;

namespace scaler {
namespace ymq {
namespace event {

// An event loop that relies on a an event selector implementation (e.g. select(), epoll_wait(), kqueue())
template <Selector TSelector>
class EventSelectorLoop {
    // std::multiset behaves like std::priority_queue, but allows O(log n) item removal by value.
    template <typename T>
    using IndexedPriorityQueue = std::multiset<T>;

    struct ScheduledFunction;

public:
    using Function = Configuration::ExecutionFunction;

    using TaskIdentifier = IndexedPriorityQueue<ScheduledFunction>::const_iterator;

    EventSelectorLoop() noexcept
    {
        _interruptPipe.reader.setNonBlocking();
        _selector.add(_interruptPipe.reader.fd(), EventType::Read);
    }

    ~EventSelectorLoop() noexcept { _selector.remove(_interruptPipe.reader.fd()); }

    void executeSoon(Function&& func) noexcept { _ready.emplace(std::move(func)); }

    void executeSoonThreadSafe(Function&& func) noexcept
    {
        _readyExternal.enqueue(std::move(func));
        _interrupt();  // forces routine() to process the function.
    }

    TaskIdentifier executeAt(utility::Timestamp timestamp, Function func) noexcept
    {
        return _scheduled.emplace(timestamp, std::move(func));
    }

    void cancelExecution(TaskIdentifier taskIdent) noexcept { _scheduled.erase(taskIdent); }

    void addFdToLoop(int fd, EventType events, EventManager&& manager) noexcept
    {
        _fdToEventManager.emplace(fd, std::move(manager));
        _selector.add(fd, events);
    }

    void removeFdFromLoop(int fd) noexcept
    {
        _fdToEventManager.erase(fd);
        _selector.remove(fd);
    }

    void routine() noexcept
    {
        // Deduce the selector's timeout.

        std::optional<std::chrono::milliseconds> timeout {};

        if (!_ready.empty()) {
            // we have tasks ready for processing, only do a non-blocking select() call.
            timeout = std::chrono::milliseconds::zero();
        } else if (!_scheduled.empty()) {
            // block until the next scheduled function
            auto next = _scheduled.cbegin();
            timeout   = std::max(next->timestamp - utility::Timestamp(), std::chrono::milliseconds::zero());
        } else {
            // no scheduled or ready task, blocking until the next I/O event.
            // timeout = nullopt;
        }

        // Collect and add I/O events to the ready queue.

        auto events = _selector.select(timeout);
        for (const auto& event: events) {
            _ready.emplace([this, event]() { _onFdEvent(event); });
        };

        // Add the now ready scheduled tasks to the ready queue.
        {
            utility::Timestamp now {};

            while (!_scheduled.empty() && _scheduled.cbegin()->timestamp <= now) {
                auto node = _scheduled.extract(_scheduled.cbegin());
                _ready.emplace(std::move(node.value().function));
            }
        }

        // Add the externally received tasks to the ready queue.
        _collectExternal();

        // Execute all ready tasks.
        // We do not directly iterate the queue (with `for(:)`) as additional tasks might be added to the ready
        // queue by the callbacks themselves.

        while (!_ready.empty()) {
            _ready.front()();
            _ready.pop();
        }
    }

private:
    struct ScheduledFunction {
        utility::Timestamp timestamp;
        Function function;

        constexpr bool operator<(const ScheduledFunction& other) const { return timestamp < other.timestamp; }
    };

    TSelector _selector;

    std::queue<Function> _ready;

    IndexedPriorityQueue<ScheduledFunction> _scheduled;

    utility::pipe::Pipe _interruptPipe;

    moodycamel::ConcurrentQueue<Function> _readyExternal;

    std::map<int, EventManager> _fdToEventManager;

    // When called from an external thread, forces the main thread to stop blocking on select() and to process the
    // _readyExternal queue.
    void _interrupt() noexcept
    {
        std::array<uint8_t, 1> byte {'\0'};
        _interruptPipe.writer.writeAll(byte);
    }

    void _collectExternal() noexcept
    {
        std::array<uint8_t, 1> byte;
        utility::IOResult readResult;

        while (!(readResult = _interruptPipe.reader.read(byte)).error) {
            Function externalFunction;

            // There is at least one function in the external ready queue. We might wait until it's consumable from
            // this thread.
            while (!_readyExternal.try_dequeue(externalFunction))
                ;

            _ready.emplace(std::move(externalFunction));
        }

        if (readResult.error && readResult.error != utility::IOResult::Error::WouldBlock) {
            unrecoverableError({utility::Error::ErrorCode::CoreBug, "failed to read from interrupt pipe"});
        }
    }

    void _onFdEvent(SelectorEvent<TSelector> event) noexcept
    {
        auto it = _fdToEventManager.find(event.handle);

        if (it == _fdToEventManager.end()) {
            // The file descriptor has been removed from the event loop.
            // This can occur if the file descriptor was explicitly removed by a previous callback from the `_ready`
            // queue.
            return;
        }

        const EventManager& eventManager = it->second;

        if (event.events & EventType::Read) {
            eventManager.onRead();
        }
        if (event.events & EventType::Write) {
            eventManager.onWrite();
        }
        if (event.events & EventType::Close) {
            eventManager.onClose();
        }
        if (event.events & EventType::Error) {
            eventManager.onError();
        }
    }
};

#ifdef __linux__
static_assert(EventLoop<EventSelectorLoop<EpollSelector>>);
#endif  // __linux__

#ifdef __APPLE__
static_assert(EventLoop<EventSelectorLoop<KQueueSelector>>);
#endif  // __APPLE__

}  // namespace event
}  // namespace ymq
}  // namespace scaler