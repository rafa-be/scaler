#pragma once

#include <concepts>
#include <cstdint>  // uint64_t
#include <utility>  // std::move

#include "scaler/io/ymq/configuration.h"

#if defined(__linux__)
#include "scaler/io/ymq/epoll_context.h"
#elif defined(_WIN32)
#include "scaler/io/ymq/iocp_context.h"
#elif defined(__APPLE__)
#include "scaler/io/ymq/kqueue_context.h"
#endif

namespace scaler {
namespace ymq {

struct Timestamp;
class EventManager;

template <class Backend>
concept EventLoopBackend = requires(Backend backend, Backend::Function f) {
    { backend.executeNow(std::move(f)) } -> std::same_as<void>;
    { backend.executeLater(std::move(f)) } -> std::same_as<void>;
    { backend.executeAt(Timestamp {}, std::move(f)) } -> std::integral;
    { backend.cancelExecution(0) } -> std::same_as<void>;

    backend.addFdToLoop(int {}, uint64_t {}, (EventManager*)nullptr);
    { backend.removeFdFromLoop(int {}) } -> std::same_as<void>;
};

template <EventLoopBackend Backend>
class EventLoop {
    Backend backend;

public:
    using Function   = Backend::Function;
    using Identifier = Backend::Identifier;

    void loop() { backend.loop(); }

    void executeNow(Function func) { backend.executeNow(std::move(func)); }
    void executeLater(Function func) { backend.executeLater(std::move(func)); }

    Identifier executeAt(Timestamp timestamp, Function func) { return backend.executeAt(timestamp, std::move(func)); }
    void cancelExecution(Identifier identifier) { backend.cancelExecution(identifier); }

    auto addFdToLoop(int fd, uint64_t events, EventManager* manager)
    {
        return backend.addFdToLoop(fd, events, manager);
    }

    void removeFdFromLoop(int fd) { backend.removeFdFromLoop(fd); }
};

///
///
///

#include <queue>
#include <vector>

/* abstract */ class Selector {
    using Identifier;

    enum EventType { READ = 0x01, WRITE = 0x02, CLOSE = 0x04 };

    struct Event {
        Identifier ident;
        EventType events;
    }

    // Start monitoring an identifier for the selected I/O events.
    //
    // Will fail if the identifier is already registered.
    void add(Identifier ident, EventType events);

    // Stop monitoring an identifier for any I/O events.
    void remove(Identifier ident);

    // Block until it collects events on the registered identifiers, or until the optional timeout triggers.
    std::vector<SelectorEvent> select(std::optional<int> timeout);

    // Immediately stop the current or next blocking select() call. Threadsafe.
    void interrupt();
};

/* abstract */ class EventLoop {
public:
    using Function  = ...;
    using TaskIdent = ...;

    // Call this function during the next iteration of the event loop. Must be called from the event loop thread.
    void executeSoon(Function func);

    void executeSoonThreadSafe(Function func);

    void executeAt(Timestamp timestamp, Function func);

    // Execute one iteration of the event loop.
    void routine();
};

template <typename Selector>
class SelectorEventLoop: EventLoop {
public:
    void executeSoon(Function func) { _ready.emplace(std::move(func)); }

    void executeSoonThreadSafe(Function func);
    {
        _readyExternal.emplace(std::move(func));
        _selector.interrupt();  // forces the event loop thread to process the functions.
    }

    void executeAt(Timestamp timestamp, Function func) {}

    void routine()
    {
        // Deduce the selector's timeout from the next scheduled function.

        std::optional<int> timeout {};

        if (!_scheduled.empty()) {
            ScheduledFunction next = _scheduled.top();
            timeout                = next.timestamp - Timestamp::now();
        }

        // Add the

        auto events = _selector.select(timeout);
    }

private:
    using FunctionQueue = std::queue<Function>;

    struct ScheduledFunction {
        Timestamp timestamp;

        Function function;
        Identifier ident;

        constexpr bool operator<(const ScheduledFunction& other) const { return timestamp < other.timestamp; }
    };

    Selector _selector;

    std::queue<Function> _ready;
    std::priorityqueue<ScheduledFunction> _scheduled;

    moodycamel::ConcurrentQueue<T> _readyExternal;
}

}  // namespace ymq
}  // namespace scaler
