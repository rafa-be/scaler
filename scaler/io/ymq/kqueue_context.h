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

private:
    constexpr static size_t MAX_EVENT_BATCH_SIZE = 1024;

    int _kq;
    TimedQueue _timingFunctions;
    DelayedFunctionQueue _delayedFunctions;
    InterruptiveConcurrentQueue<Function> _interruptiveFunctions;
    constexpr static const uintptr_t _interruptiveIdent = 0;
    constexpr static const uintptr_t _timerIdent        = 1;
    constexpr static const size_t _reventSize           = 1024;

    void registerInterruptiveIdent();

    void registerTimerIdent();

    void execPendingFunctions();

    void KqueueContext::_setKEvent(
        uintptr_t ident,
        short filter,
        uint16_t flags,
        uint32_t filterFlags = 0,
        int64_t filterData   = 0,
        uint64_t userData    = 0);

    static int _createKQueue();
}  // namespace ymq
}  // namespace scaler

#endif  // __APPLE__
