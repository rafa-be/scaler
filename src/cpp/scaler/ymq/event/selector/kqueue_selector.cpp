#include "scaler/ymq/event/selector/kqueue_selector.h"

#include <sys/event.h>
#include <sys/types.h>

#include <chrono>
#include <optional>
#include <vector>

#include "scaler/utility/error.h"
#include "scaler/ymq/event/event_type.h"
#include "scaler/ymq/event/selector/selector.h"

namespace scaler {
namespace ymq {
namespace event {
namespace selector {

KQueueSelector::KQueueSelector()
{
    _kqueueCreate();
}

KQueueSelector::~KQueueSelector()
{
    ::close(_kq);
}

void KQueueSelector::add(Handle handle, EventType events)
{
    struct ::kevent event {};

    if ((events & EventType::Read) || (events & EventType::Close)) {
        EV_SET(&event, handle, EVFILT_READ, EV_ADD | EV_ENABLE | EV_CLEAR, 0, 0, nullptr);
        _setKEvent(EV_ADD, &event);
    }

    if (events & EventType::Write) {
        EV_SET(&event, handle, EVFILT_WRITE, EV_ADD | EV_ENABLE | EV_CLEAR, 0, 0, nullptr);
        _setKEvent(EV_ADD, &event);
    }
}

void KQueueSelector::remove(Handle handle)
{
    struct kevent event {};

    EV_SET(&event, handle, EVFILT_READ, EV_DELETE, 0, 0, nullptr);
    _setKEvent(EV_DELETE, &event);

    EV_SET(&event, handle, EVFILT_WRITE, EV_DELETE, 0, 0, nullptr);
    _setKEvent(EV_DELETE, &event);
}

std::vector<SelectorEvent<KQueueSelector>> KQueueSelector::select(std::optional<std::chrono::milliseconds> timeout)
{
    struct timespec ts {};
    struct timespec* tsPtr = nullptr;

    if (timeout) {
        ts.tv_sec  = timeout->count() / 1000;
        ts.tv_nsec = (timeout->count() % 1000) * 1000000;
        tsPtr      = &ts;
    }

    std::array<struct kevent, _MAX_EVENTS> events {};
    int n = ::kevent(_kq, nullptr, 0, events.data(), events.size(), tsPtr);

    if (n == -1) {
        switch (errno) {
            case EINTR:
                // Signal interrupted the wait, just return and try again
                return {};
            default:
                unrecoverableError({
                    utility::Error::ErrorCode::CoreBug,
                    "Originated from",
                    "kevent(2)",
                    "Errno is",
                    strerror(errno),
                });
                break;
        }
    }

    std::vector<SelectorEvent<KQueueSelector>> result;

    for (const auto& event: std::span(events.data(), n)) {
        EventType eventTypes = EventType::None;

        if (event.flags & EV_EOF) {
            eventTypes = static_cast<EventType>(eventTypes | EventType::Close);
        }
        if (event.flags & EV_ERROR) {
            eventTypes = static_cast<EventType>(eventTypes | EventType::Error);
        }
        if (event.filter == EVFILT_READ) {
            eventTypes = static_cast<EventType>(eventTypes | EventType::Read);
        }
        if (event.filter == EVFILT_WRITE) {
            eventTypes = static_cast<EventType>(eventTypes | EventType::Write);
        }

        result.emplace_back(event.ident, eventTypes);
    }

    return result;
}

void KQueueSelector::_kqueueCreate()
{
    _kq = ::kqueue();
    if (_kq == -1) {
        switch (errno) {
            case EACCES:
            case EFAULT:
            case EBADF:
            case EINVAL:
            case ENOENT:
            case ESRCH:
                unrecoverableError({
                    utility::Error::ErrorCode::ConfigurationError,
                    "Originated from",
                    "kqueue(2)",
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
                    utility::Error::ErrorCode::CoreBug,
                    "Originated from",
                    "kqueue(2)",
                    "Errno is",
                    strerror(errno),
                });
                break;
        }
    }
}

void KQueueSelector::_setKEvent(int op, const struct kevent* event)
{
    if (::kevent(_kq, event, 1, nullptr, 0, nullptr) == -1) {
        // Ignore identifier not found when deleting a filter.
        if (op == EV_DELETE && errno == ENOENT) {
            return;
        }

        unrecoverableError({
            utility::Error::ErrorCode::CoreBug,
            "Originated from",
            "kevent(2)",
            "Errno is",
            strerror(errno),
        });
    }
}

static_assert(Selector<KQueueSelector>);

}  // namespace selector
}  // namespace event
}  // namespace ymq
}  // namespace scaler
