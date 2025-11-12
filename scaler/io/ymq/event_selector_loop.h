#pragma once

#include <unistd.h>

#include <algorithm>
#include <chrono>
#include <concepts>
#include <map>
#include <queue>
#include <set>

// #include "scaler/io/ymq/event_loop.h"
#include "scaler/io/ymq/configuration.h"
#include "scaler/io/ymq/event_manager.h"
#include "scaler/io/ymq/event_selector.h"
#include "scaler/io/ymq/timestamp.h"
#include "third_party/concurrentqueue.h"

namespace scaler {
namespace ymq {

template <typename T>
concept _EventLoop = requires(T loop, typename T::Function function, typename T::TaskIdentifier taskIdent) {
    typename T::Function;
    typename T::TaskIdentifier;

    // Call this function during the next iteration of the event loop. Must be called from the event loop thread.
    { loop.executeSoon(std::move(function)) } -> std::same_as<void>;

    { loop.executeSoonThreadSafe(std::move(function)) } -> std::same_as<void>;

    // Plan the execution of a function in the future. Must be called from the event loop thread.
    { loop.executeAt(Timestamp {}, std::move(function)) } -> std::same_as<typename T::TaskIdentifier>;

    // Cancel the execution of a function in the future. Must be called from the event loop thread.
    { loop.cancelExecution(taskIdent) } -> std::same_as<void>;

    // Register a file descriptor for events. Must be called from the event loop thread.
    { loop.addFdToLoop(int {}, EventSelectorType {}, std::move(EventManager {})) } -> std::same_as<void>;
    { loop.removeFdFromLoop(int {}) } -> std::same_as<void>;

    // Execute one iteration of the event loop, calling ready functions and I/O events.
    { loop.routine() } -> std::same_as<void>;
};

// An event loop that relies on a an event selector implementation (e.g. select(), epoll_wait(), kqueue())
template <EventSelector Selector>
class EventSelectorLoop {
    // std::multiset behaves like std::priority_queue, but allows O(log n) item removal by value.
    template <typename T>
    using IndexedPriorityQueue = std::multiset<T>;

    struct ScheduledFunction;

public:
    using Function = Configuration::ExecutionFunction;

    using TaskIdentifier = IndexedPriorityQueue<ScheduledFunction>::const_iterator;

    EventSelectorLoop() noexcept { _initInterruptPipe(); }

    ~EventSelectorLoop() noexcept { _closeInterruptPipe(); }

    void executeSoon(Function&& func) noexcept { _ready.emplace(std::move(func)); }

    void executeSoonThreadSafe(Function&& func) noexcept
    {
        _readyExternal.enqueue(std::move(func));
        _interrupt();  // forces routine() to process the function.
    }

    TaskIdentifier executeAt(Timestamp timestamp, Function func) noexcept
    {
        return _scheduled.emplace(timestamp, std::move(func));
    }

    void cancelExecution(TaskIdentifier taskIdent) noexcept { _scheduled.erase(taskIdent); }

    void addFdToLoop(int fd, EventSelectorType events, EventManager&& manager) noexcept
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
            timeout   = std::max(next->timestamp - Timestamp(), std::chrono::milliseconds::zero());
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
            Timestamp now {};

            while (!_scheduled.empty() && _scheduled.cbegin()->timestamp <= now) {
                auto node = _scheduled.extract(_scheduled.cbegin());
                _ready.emplace(std::move(node.value().function));
            }
        }

        // Add the externally received tasks to the ready queue.
        _collectExternal();

        // Execute all ready tasks.
        // We do not directly iterate the queue (with `for(:)`) as additional tasks might be added to the ready queue by
        // the callbacks themselves.

        while (!_ready.empty()) {
            _ready.front()();
            _ready.pop();
        }
    }

private:
    struct ScheduledFunction {
        Timestamp timestamp;
        Function function;

        constexpr bool operator<(const ScheduledFunction& other) const { return timestamp < other.timestamp; }
    };

    Selector _selector;

    std::queue<Function> _ready;

    IndexedPriorityQueue<ScheduledFunction> _scheduled;

    int _interruptPipe[2];

    moodycamel::ConcurrentQueue<Function> _readyExternal;

    std::map<int, EventManager> _fdToEventManager;

    void _initInterruptPipe() noexcept
    {
        if (pipe(_interruptPipe) != 0) {
            unrecoverableError({
                Error::ErrorCode::CoreBug,
                "Originated from",
                "pipe(2)",
                "Errno is",
                strerror(errno),
            });
        }

        fcntl(_interruptPipe[0], F_SETFL, O_NONBLOCK);
        fcntl(_interruptPipe[1], F_SETFL, O_NONBLOCK);

        _selector.add(_interruptPipe[0], EventSelectorType::Read);
    }

    void _closeInterruptPipe() noexcept
    {
        close(_interruptPipe[0]);
        close(_interruptPipe[1]);
    }

    // When called from an external thread, forces the main thread to stop blocking on select() and to process the
    // _readyExternal queue.
    void _interrupt() noexcept
    {
        const char byte = '\0';
        ssize_t n       = write(_interruptPipe[1], &byte, 1);

        if (n != 1) {
            unrecoverableError({
                Error::ErrorCode::CoreBug,
                "Originated from",
                "write(2)",
                "Errno is",
                strerror(errno),
            });
        }
    }

    void _collectExternal() noexcept
    {
        char byte;
        while (read(_interruptPipe[0], &byte, 1) == 1) {
            Function externalFunction;

            // There is at least one function in the external ready queue. We might wait until it's consumable from this
            // thread.
            while (!_readyExternal.try_dequeue(externalFunction))
                ;

            _ready.emplace(std::move(externalFunction));
        }
    }

    void _onFdEvent(EventSelectorEvent<typename Selector::Identifier> event) noexcept
    {
        auto it = _fdToEventManager.find(event.identifier);

        if (it == _fdToEventManager.end()) {
            // fd got removed from the event loop.
            return;
        }

        const EventManager& eventManager = it->second;

        if (event.types & EventSelectorType::Read) {
            eventManager.onRead();
        }
        if (event.types & EventSelectorType::Write) {
            eventManager.onWrite();
        }
        if (event.types & EventSelectorType::Close) {
            eventManager.onClose();
        }
        if (event.types & EventSelectorType::Error) {
            eventManager.onError();
        }
    }
};

#ifdef __linux__
static_assert(_EventLoop<EventSelectorLoop<EpollSelector>>);
#endif  // __linux__

#ifdef __APPLE__
static_assert(_EventLoop<EventSelectorLoop<KQueueSelector>>);
#endif  // __APPLE__

}  // namespace ymq
}  // namespace scaler
