#pragma once
#ifdef __APPLE__

#include <sys/event.h>
#include <sys/types.h>

#include <functional>
#include <queue>

#include "scaler/io/ymq/configuration.h"
#include "scaler/io/ymq/interruptive_concurrent_queue.h"
#include "scaler/io/ymq/timed_queue.h"
#include "scaler/io/ymq/timestamp.h"

namespace scaler {
namespace ymq {

class EventManager;

// macOS kqueue-based event loop context
class KqueueContext {
public:
    using Function             = Configuration::ExecutionFunction;
    using DelayedFunctionQueue = std::queue<Function>;
    using Identifier           = Configuration::ExecutionCancellationIdentifier;

    KqueueContext();
    ~KqueueContext();

    void loop();

    void addFdToLoop(int fd, uint64_t events, EventManager* manager);
    void removeFdFromLoop(int fd);

    // NOTE: Thread-safe method to communicate with the event loop thread
    void executeNow(Function func) { _interruptiveFunctions.enqueue(std::move(func)); }
    // WARN: NOT thread-safe. Thread safety is guaranteed by executeNow.
    void executeLater(Function func) { _delayedFunctions.emplace(std::move(func)); }
    // WARN: NOT thread-safe. Thread safety is guaranteed by executeNow.
    Identifier executeAt(Timestamp timestamp, Function callback)
    {
        return _timingFunctions.push(timestamp, std::move(callback));
    }
    void cancelExecution(Identifier identifier) { _timingFunctions.cancelExecution(identifier); }

    void armNextTimer(uintptr_t timerIdent);

private:
    constexpr static size_t MAX_EVENT_BATCH_SIZE = 1024;

    int _kq;
    TimedQueue _timingFunctions;
    DelayedFunctionQueue _delayedFunctions;
    InterruptiveConcurrentQueue<Function> _interruptiveFunctions;
    constexpr static const uintptr_t _interruptiveIdent = 0;
    constexpr static const uintptr_t _timerIdent        = 1;
    constexpr static const size_t _reventSize           = 1024;

    using TimedFunc           = std::tuple<Timestamp, Function, Identifier>;
    constexpr static auto cmp = [](const auto& x, const auto& y) { return std::get<0>(x) < std::get<0>(y); };
    using PriorityQueue       = std::priority_queue<TimedFunc, std::vector<TimedFunc>, decltype(cmp)>;

    PriorityQueue pq;

    void registerInterruptiveIdent();

    void registerTimerIdent();

    void execPendingFunctions();

    void _setKEvent(
        uintptr_t ident,
        short filter,
        uint16_t flags,
        uint32_t filterFlags = 0,
        int64_t filterData   = 0,
        void* userData       = nullptr);

    static int _createKQueue();
};

}  // namespace ymq
}  // namespace scaler

#endif  // __APPLE__
