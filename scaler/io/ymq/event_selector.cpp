#include "scaler/io/ymq/event_selector.h"

#include <unistd.h>

#include <array>
#include <cerrno>
#include <span>

#ifdef __linux__
#include <sys/epoll.h>
#endif  // __linux__

#ifdef __APPLE__
#include <sys/event.h>
#include <sys/types.h>
#endif  // __APPLE__

namespace scaler {
namespace ymq {

#ifdef __linux__

EpollSelector::EpollSelector()
{
    _epollCreate();
}

EpollSelector::~EpollSelector()
{
    close(_epFd);
}

void EpollSelector::add(Identifier ident, EventSelectorType events)
{
    epoll_event event {};

    event.data.fd = ident;

    event.events = EPOLLET;
    if (events & EventSelectorType::Read) {
        event.events |= EPOLLIN;
    }
    if (events & EventSelectorType::Write) {
        event.events |= EPOLLOUT;
    }
    if (events & EventSelectorType::Close) {
        event.events |= EPOLLRDHUP;
    }

    _epollCtl(EPOLL_CTL_ADD, ident, &event);
}

void EpollSelector::remove(Identifier ident)
{
    _epollCtl(EPOLL_CTL_DEL, ident, nullptr);
}

std::vector<EventSelectorEvent<EpollSelector::Identifier>> EpollSelector::select(
    std::optional<std::chrono::milliseconds> timeout)
{
    std::array<epoll_event, _MAX_EVENTS> events {};
    int timeoutMs = timeout ? static_cast<int>(timeout->count()) : -1;
    int n         = epoll_wait(_epFd, events.data(), _MAX_EVENTS, timeoutMs);

    if (n == -1) {
        switch (errno) {
            case EINTR:
                // the epoll thread is not expected to receive signals(?)
                // but occasionally does (e.g. sigwinch) and we shouldn't stop the thread in that case
                return {};
            case EBADF:
            case EFAULT:
            case EINVAL:
            default:
                unrecoverableError({
                    Error::ErrorCode::CoreBug,
                    "Originated from",
                    "epoll_wait(2)",
                    "Errno is",
                    strerror(errno),
                    "_epFd",
                    _epFd,
                });
                break;
        }
    }

    std::vector<EventSelectorEvent<Identifier>> result;

    for (const auto& event: std::span(events.data(), n)) {
        Identifier ident = event.data.fd;

        EventSelectorType eventTypes = EventSelectorType::None;

        if (event.events & (EPOLLHUP | EPOLLRDHUP)) {
            eventTypes = static_cast<EventSelectorType>(eventTypes | EventSelectorType::Close);
        }
        if (event.events & EPOLLERR) {
            eventTypes = static_cast<EventSelectorType>(eventTypes | EventSelectorType::Error);
        }
        if (event.events & EPOLLIN) {
            eventTypes = static_cast<EventSelectorType>(eventTypes | EventSelectorType::Read);
        }
        if (event.events & EPOLLOUT) {
            eventTypes = static_cast<EventSelectorType>(eventTypes | EventSelectorType::Write);
        }

        result.emplace_back(ident, eventTypes);
    }

    return result;
}

void EpollSelector::_epollCreate()
{
    _epFd = epoll_create1(0);
    if (_epFd == -1) {
        switch (errno) {
            case ENFILE:
            case ENODEV:
            case ENOMEM:
            case EMFILE:
                unrecoverableError({
                    Error::ErrorCode::ConfigurationError,
                    "Originated from",
                    "epoll_create1(2)",
                    "Errno is",
                    strerror(errno),
                });
                break;

            case EINVAL:
            default:
                unrecoverableError({
                    Error::ErrorCode::CoreBug,
                    "Originated from",
                    "epoll_create1(2)",
                    "Errno is",
                    strerror(errno),
                });
                break;
        }
    }
}

void EpollSelector::_epollCtl(int op, int fd, epoll_event* event)
{
    int res = epoll_ctl(_epFd, op, fd, event);
    if (res == 0) {
        return;
    }

    switch (errno) {
        case ENOMEM:
        case ENOSPC:
            unrecoverableError({
                Error::ErrorCode::ConfigurationError,
                "Originated from",
                "epoll_ctl(2)",
                "Errno is",
                strerror(errno),
                "epFd",
                _epFd,
                "fd",
                fd,
            });
            break;

        case EBADF:
        case EPERM:
        case EINVAL:
        case ELOOP:
        case ENOENT:
        default:
            unrecoverableError({
                Error::ErrorCode::CoreBug,
                "Originated from",
                "epoll_ctl(2)",
                "Errno is",
                strerror(errno),
                "epFd",
                _epFd,
                "fd",
                fd,
            });
            break;
    }
}

static_assert(EventSelector<EpollSelector>);

#endif  // __linux__

#ifdef __APPLE__

KQueueSelector::KQueueSelector()
{
    _kqueueCreate();
}

KQueueSelector::~KQueueSelector()
{
    close(_kq);
}

void KQueueSelector::add(Identifier ident, EventSelectorType events)
{
    struct kevent event {};

    if ((events & EventSelectorType::Read) || (events & EventSelectorType::Close)) {
        EV_SET(&event, ident, EVFILT_READ, EV_ADD | EV_ENABLE | EV_CLEAR, 0, 0, nullptr);
        _setKEvent(EV_ADD, &event);
    }

    if (events & EventSelectorType::Write) {
        EV_SET(&event, ident, EVFILT_WRITE, EV_ADD | EV_ENABLE | EV_CLEAR, 0, 0, nullptr);
        _setKEvent(EV_ADD, &event);
    }
}

void KQueueSelector::remove(Identifier ident)
{
    struct kevent event {};

    EV_SET(&event, ident, EVFILT_READ, EV_DELETE, 0, 0, nullptr);
    _setKEvent(EV_DELETE, &event);

    EV_SET(&event, ident, EVFILT_WRITE, EV_DELETE, 0, 0, nullptr);
    _setKEvent(EV_DELETE, &event);
}

std::vector<EventSelectorEvent<KQueueSelector::Identifier>> KQueueSelector::select(
    std::optional<std::chrono::milliseconds> timeout)
{
    struct timespec ts {};
    struct timespec* tsPtr = nullptr;

    if (timeout) {
        ts.tv_sec  = timeout->count() / 1000;
        ts.tv_nsec = (timeout->count() % 1000) * 1000000;
        tsPtr      = &ts;
    }

    std::array<struct kevent, _MAX_EVENTS> events {};
    int n = kevent(_kq, nullptr, 0, events.data(), events.size(), tsPtr);

    if (n == -1) {
        switch (errno) {
            case EINTR:
                // Signal interrupted the wait, just return and try again
                return {};
            default:
                unrecoverableError({
                    Error::ErrorCode::CoreBug,
                    "Originated from",
                    "kevent(2)",
                    "Errno is",
                    strerror(errno),
                });
                break;
        }
    }

    std::vector<EventSelectorEvent<Identifier>> result;

    for (const auto& event: std::span(events.data(), n)) {
        EventSelectorType eventTypes = EventSelectorType::None;

        if (event.flags & EV_EOF) {
            eventTypes = static_cast<EventSelectorType>(eventTypes | EventSelectorType::Close);
        }
        if (event.flags & EV_ERROR) {
            eventTypes = static_cast<EventSelectorType>(eventTypes | EventSelectorType::Error);
        }
        if (event.filter == EVFILT_READ) {
            eventTypes = static_cast<EventSelectorType>(eventTypes | EventSelectorType::Read);
        }
        if (event.filter == EVFILT_WRITE) {
            eventTypes = static_cast<EventSelectorType>(eventTypes | EventSelectorType::Write);
        }

        result.emplace_back(event.ident, eventTypes);
    }

    return result;
}

void KQueueSelector::_kqueueCreate()
{
    _kq = kqueue();
    if (_kq == -1) {
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
                    Error::ErrorCode::CoreBug,
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
    if (kevent(_kq, event, 1, nullptr, 0, nullptr) == -1) {
        // Ignore identifier not found when deleting a filter.
        if (op == EV_DELETE && errno == ENOENT) {
            return;
        }

        unrecoverableError({
            Error::ErrorCode::CoreBug,
            "Originated from",
            "kevent(2)",
            "Errno is",
            strerror(errno),
        });
    }
}

static_assert(EventSelector<KQueueSelector>);

#endif  // __APPLE__

}  // namespace ymq
}  // namespace scaler
