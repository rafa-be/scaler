#ifdef __APPLE__

#include "scaler/io/ymq/kqueue_context.h"

#include <sys/event.h>
#include <sys/types.h>
#include <unistd.h>

#include <array>
#include <cerrno>
#include <cstring>
#include <functional>
#include <ranges>

#include "scaler/io/ymq/error.h"
#include "scaler/io/ymq/event_manager.h"

namespace scaler {
namespace ymq {

KqueueContext::KqueueContext()
    : _kq(_createKQueue())
    , _timingFunctions(_kq, _timerIdent)
    , _delayedFunctions()
    , _interruptiveFunctions(_kq, _interruptiveIdent)
{
    registerInterruptiveIdent();
    registerTimerIdent();
}

KqueueContext::~KqueueContext()
{
    if (_kq >= 0)
        close(_kq);
}

void KqueueContext::execPendingFunctions()
{
    while (!_delayedFunctions.empty()) {
        auto top = std::move(_delayedFunctions.front());
        top();
        _delayedFunctions.pop();
    }
}

void KqueueContext::loop()
{
    std::array<struct kevent, MAX_EVENT_BATCH_SIZE> events {};

    int n = kevent(_kq, nullptr, 0, events.data(), events.size(), nullptr);
    if (n == -1) {
        switch (errno) {
            case EINTR:
                // Signal interrupted the wait, just return and try again
                return;
            default:
                unrecoverableError({
                    Error::ErrorCode::CoreBug,
                    "Originated from",
                    __PRETTY_FUNCTION__,
                    "Errno is",
                    strerror(errno),
                });
                break;
        }
    }

    for (auto it = events.begin(); it != events.begin() + n; ++it) {
        const struct kevent& event = *it;

        if (event.filter == EVFILT_USER && event.ident == _interruptiveIdent) {
            // Handle interruptive functions
            auto vec = _interruptiveFunctions.dequeue();
            std::ranges::for_each(vec, [](auto&& x) { x(); });
        } else if (event.filter == EVFILT_TIMER) {
            // Handle timing functions
            auto vec = _timingFunctions.dequeue();
            std::ranges::for_each(vec, [](auto& x) { x(); });
        } else {
            // Handle socket events
            EventManager* eventManager = static_cast<EventManager*>(event.udata);
            if (eventManager) {
                if (event.filter == EVFILT_READ) {
                    eventManager->onRead();
                }
                if (event.filter == EVFILT_WRITE) {
                    eventManager->onWrite();
                }
                if (event.flags & EV_EOF) {
                    eventManager->onClose();
                }
                if (event.flags & EV_ERROR) {
                    eventManager->onError();
                }
            }
        }
    }

    execPendingFunctions();
}

void KqueueContext::addFdToLoop(int fd, uint64_t events, EventManager* manager)
{
    if (events & EVFILT_READ) {
        _setKEvent(fd, EVFILT_READ, EV_ADD | EV_ENABLE);
    }

    if (events & EVFILT_WRITE) {
        _setKEvent(fd, EVFILT_WRITE, EV_ADD | EV_ENABLE);
    }
}

void KqueueContext::removeFdFromLoop(int fd)
{
    std::array<struct kevent, 2> events;

    EV_SET(&events[0], fd, EVFILT_READ, EV_DELETE, 0, 0, nullptr);
    EV_SET(&events[1], fd, EVFILT_WRITE, EV_DELETE, 0, 0, nullptr);

    // It's OK if one of them fails (might not be registered), so we don't check error
    kevent(_kq, events.data(), events.size(), nullptr, 0, nullptr);
}

int KqueueContext::_createKQueue()
{
    const int kq = kqueue();
    if (kq == -1) {
        switch (errno) {
            case EACCES:
            case EFAULT:
            case EBADF:
            case EINVAL:
            case ENOENT:
            case ESRCH:
                unrecoverableError({
                    Error::ErrorCode::ConfigurationError,
                    "Originated from",
                    __PRETTY_FUNCTION__,
                    "Errno is",
                    strerror(errno),
                });
                break;

            case ENOMEM:
            case EMFILE:
            case ENFILE:
            case EINTR:
            default:
                unrecoverableError({
                    Error::ErrorCode::CoreBug,
                    "Originated from",
                    __PRETTY_FUNCTION__,
                    "Errno is",
                    strerror(errno),
                });
                break;
        }
    }
    return kq;
}

void KqueueContext::registerInterruptiveIdent()
{
    _setKEvent(_interruptiveIdent, EVFILT_USER, EV_ADD | EV_ENABLE | EV_CLEAR);
}

void KqueueContext::registerTimerIdent()
{
    // TODO
}

void KqueueContext::_setKEvent(
    uintptr_t ident, short filter, uint16_t flags, uint32_t filterFlags, int64_t filterData, void* userData)
{
    struct kevent kev;
    EV_SET(&kev, ident, filter, flags, filterFlags, filterData, userData);

    if (kevent(_kq, &kev, 1, nullptr, 0, nullptr) == -1) {
        unrecoverableError({
            Error::ErrorCode::CoreBug,
            "Originated from",
            "kevent(2)",
            "Errno is",
            strerror(errno),
        });
    }
}

int64_t clampMicroseconds(int64_t micros)
{
    return micros < 0 ? 0 : micros;
}

TimedQueue::TimedQueue(int kq, uintptr_t timerIdent): _kq(kq), _timerIdent(timerIdent), _currentId(0)
{
}

TimedQueue::~TimedQueue() = default;

void KqueueContext::armNextTimer(uintptr_t timerIdent)
{
    // Remove any existing timer
    _setKEvent(timerIdent, EVFILT_TIMER, EV_DELETE);

    if (pq.empty()) {
        // No scheduled task.
        return;
    }

    Timestamp nextEvent = std::get<0>(pq.top());
    int64_t microsecs   = clampMicroseconds(convertToKqueueTimer(nextEvent));

    _setKEvent(timerIdent, EVFILT_TIMER, EV_ADD | EV_ENABLE | EV_ONESHOT, NOTE_USECONDS, microsecs);
}

TimedQueue::Identifier TimedQueue::push(Timestamp timestamp, Callback cb)
{
    const auto id = _currentId++;
    // pq.push({timestamp, std::move(cb), id});
    //  armNextTimer();
    return id;
}

std::vector<TimedQueue::Callback> TimedQueue::dequeue()
{
    std::vector<Callback> callbacks;
    Timestamp now;

    // while (!pq.empty()) {
    //     if (std::get<0>(pq.top()) < now) {
    //         auto [ts, cb, id] = std::move(const_cast<PriorityQueue::reference>(pq.top()));
    //         pq.pop();
    //         auto cancelled = _cancelledFunctions.find(id);
    //         if (cancelled != _cancelledFunctions.end()) {
    //             _cancelledFunctions.erase(cancelled);
    //         } else {
    //             callbacks.emplace_back(std::move(cb));
    //         }
    //     } else {
    //         break;
    //     }
    // }

    // armNextTimer();

    return callbacks;
}

// InterruptiveConcurrentQueue implementation for macOS
template <typename T>
void InterruptiveConcurrentQueue<T>::enqueue(T item)
{
    _queue.enqueue(std::move(item));

    // Trigger a user event on the kqueue to wake up the event loop
    struct kevent kev;
    EV_SET(&kev, _ident, EVFILT_USER, 0, NOTE_TRIGGER, 0, nullptr);

    if (kevent(_kq, &kev, 1, nullptr, 0, nullptr) == -1) {
        unrecoverableError({
            Error::ErrorCode::CoreBug,
            "Originated from",
            "kevent(2) - trigger user event",
            "Errno is",
            strerror(errno),
        });
    }
}

template <typename T>
std::vector<T> InterruptiveConcurrentQueue<T>::dequeue()
{
    std::vector<T> vecT;
    while (true) {
        T next;
        if (!_queue.try_dequeue(next)) {
            break;
        }
        vecT.emplace_back(std::move(next));
    }

    return vecT;
}

// Explicit template instantiation for Function type
template class InterruptiveConcurrentQueue<Configuration::ExecutionFunction>;

}  // namespace ymq
}  // namespace scaler

#endif  // __APPLE__
